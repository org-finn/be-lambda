import os
import boto3
import random
import json
import logging

elbv2_client = boto3.client('elbv2')
sqs_client = boto3.client('sqs')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ALB_LISTENER_ARN = os.environ.get('ALB_LISTENER_ARN')
LIVE_TARGET_GROUP_ARN = os.environ.get('LIVE_TARGET_GROUP_ARN')
CANARY_TARGET_GROUP_ARN = os.environ.get('CANARY_TARGET_GROUP_ARN')
LIVE_SQS_QUEUE_URL = os.environ.get('LIVE_SQS_QUEUE_URL')
CANARY_SQS_QUEUE_URL = os.environ.get('CANARY_SQS_QUEUE_URL')


def get_deployment_status_and_weights():
    """ALB 리스너 설정을 동적으로 확인하여 배포 상태와 트래픽 가중치를 반환합니다."""
    try:
        response = elbv2_client.describe_listeners(ListenerArns=[ALB_LISTENER_ARN])
        forward_config = response['Listeners'][0]['DefaultActions'][0].get('ForwardConfig')
        if not forward_config:
            return False, 100, 0
            
        target_groups = forward_config.get('TargetGroups', [])
        is_canary_active = False
        if len(target_groups) > 1 and any(tg['TargetGroupArn'] == CANARY_TARGET_GROUP_ARN for tg in target_groups):
            is_canary_active = True
        
        if not is_canary_active:
            return False, 100, 0
        
        live_weight, canary_weight = 0, 0
        for tg in target_groups:
            if tg['TargetGroupArn'] == LIVE_TARGET_GROUP_ARN:
                live_weight = tg.get('Weight', 0)
            elif tg['TargetGroupArn'] == CANARY_TARGET_GROUP_ARN:
                canary_weight = tg.get('Weight', 0)
        
        logger.info(f"Dynamic Check -> Canary Active: True, Weights: Live={live_weight}, Canary={canary_weight}")
        return True, live_weight, canary_weight
    except Exception as e:
        logger.error(f"Failed to get ALB listener rules, defaulting to Live: {e}", exc_info=True)
        return False, 100, 0

def _send_batch_to_sqs(queue_url: str, messages: list):
    """주어진 메시지 리스트를 SQS 큐에 10개씩 묶어 일괄 전송합니다."""
    if not messages:
        return

    # SQS SendMessageBatch API는 최대 10개의 메시지를 한 번에 보낼 수 있습니다.
    for i in range(0, len(messages), 10):
        chunk = messages[i:i + 10]
        entries = []
        for msg in chunk:
            # 각 메시지는 'id'와 'body' 키를 가져야 합니다.
            entries.append({
                'Id': msg['id'],
                'MessageBody': json.dumps(msg['body'])
            })
        
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            # 성공/실패 여부 로깅
            if 'Successful' in response:
                logger.info(f"Successfully sent {len(response['Successful'])} messages to {queue_url}")
            if 'Failed' in response and response['Failed']:
                logger.error(f"Failed to send {len(response['Failed'])} messages to {queue_url}: {response['Failed']}")
        except Exception as e:
            logger.error(f"Exception during send_message_batch to {queue_url}: {e}", exc_info=True)
            raise e

def send_prediction_messages(messages: list):
    """
    메시지 리스트를 받아 카나리 배포 상태와 트래픽 가중치에 따라 각 SQS 큐로 분배하여 전송합니다.

    :param messages: [{'id': str, 'body': dict}, ...] 형태의 메시지 리스트.
    """
    if not messages:
        logger.info("No messages to send.")
        return

    is_canary_active, live_weight, canary_weight = get_deployment_status_and_weights()

    if not is_canary_active:
        logger.info(f"Canary not active. Sending all {len(messages)} messages to the Live queue.")
        _send_batch_to_sqs(LIVE_SQS_QUEUE_URL, messages)
        return

    # ✅ 핵심 로직: 트래픽 비율에 따라 메시지 리스트를 분배
    total_messages = len(messages)
    total_weight = live_weight + canary_weight
    
    if total_weight == 0: # 비정상 상태일 경우 안전하게 라이브로 모두 전송
        logger.warning("Total traffic weight is 0. Sending all messages to the Live queue.")
        _send_batch_to_sqs(LIVE_SQS_QUEUE_URL, messages)
        return
        
    # 카나리로 보낼 메시지 개수 계산 (반올림하여 정수화)
    num_canary = round(total_messages * (canary_weight / total_weight))
    
    # 메시지를 무작위로 섞어 특정 메시지 그룹이 항상 카나리로 가는 것을 방지
    random.shuffle(messages)
    
    canary_messages = messages[:num_canary]
    live_messages = messages[num_canary:]

    logger.info(
        f"Distributing {total_messages} messages -> "
        f"Live: {len(live_messages)}, Canary: {len(canary_messages)}"
    )

    # 각 큐로 일괄 전송
    _send_batch_to_sqs(LIVE_SQS_QUEUE_URL, live_messages)
    _send_batch_to_sqs(CANARY_SQS_QUEUE_URL, canary_messages)