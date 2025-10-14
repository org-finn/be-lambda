import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client('sqs')

MAIN_QUEUE_URL = os.environ['MAIN_QUEUE_URL']
DLQ_URL = os.environ['DLQ_URL']
# 이 Lambda가 관리하는 전체 재시도 횟수 제한
MAX_TOTAL_RETRIES = int(os.environ.get('MAX_TOTAL_RETRIES', 5)) 

def lambda_handler(event, context):
    """
    DLQ의 메시지를 주기적으로 메인 큐로 옮겨 재처리를 시도합니다.
    전체 재시도 횟수를 직접 카운트하여 무한 루프를 방지합니다.
    """
    response = sqs.receive_message(
        QueueUrl=DLQ_URL,
        MaxNumberOfMessages=10,
        MessageAttributeNames=['All']
    )
    
    messages = response.get('Messages', [])
    if not messages:
        logger.info("No messages in DLQ to redrive.")
        return

    for msg in messages:
        try:
            # 'totalRetryCount' 속성을 통해 전체 재시도 횟수를 관리합니다.
            total_retry_count = 0
            msg_attributes = msg.get('MessageAttributes', {})
            if 'totalRetryCount' in msg_attributes:
                total_retry_count = int(msg_attributes['totalRetryCount']['StringValue'])

            if total_retry_count >= MAX_TOTAL_RETRIES:
                # 🚨 최대 전체 재시도 횟수 초과: 메시지를 영구 폐기
                logger.error(
                    f"[FATAL] Message reached max total retries ({total_retry_count}). Discarding message. MessageId: {msg['MessageId']}"
                )
                sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=msg['ReceiptHandle'])
            else:
                # ✅ 재시도 가능: 전체 카운트를 1 증가시켜 메인 큐로 이동
                new_total_retry_count = total_retry_count + 1
                logger.info(
                    f"Redriving message. Total Retry Count: {new_total_retry_count}/{MAX_TOTAL_RETRIES}. MessageId: {msg['MessageId']}"
                )
                
                sqs.send_message(
                    QueueUrl=MAIN_QUEUE_URL,
                    MessageBody=msg['Body'],
                    MessageAttributes={
                        'totalRetryCount': {
                            'StringValue': str(new_total_retry_count),
                            'DataType': 'Number'
                        }
                        # 기존 다른 속성이 있다면 함께 전달해야 합니다.
                    }
                )
                
                # DLQ에서 원본 메시지 삭제
                sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=msg['ReceiptHandle'])

        except Exception as e:
            logger.error(f"Error processing message {msg['MessageId']}: {e}", exc_info=True)
            # 에러 발생 시 DLQ에서 메시지를 삭제하지 않으므로, 다음 Lambda 실행 시 재시도됨

    return {'status': 'completed'}