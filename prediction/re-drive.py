import boto3
import os

# Boto3 클라이언트 초기화
sqs = boto3.client('sqs')
elbv2 = boto3.client('elbv2')

# 환경 변수에서 리소스 정보 가져오기
LIVE_MAIN_QUEUE_URL = os.environ['LIVE_MAIN_QUEUE_URL']
LIVE_DLQ_URL = os.environ['LIVE_DLQ_URL']
CANARY_MAIN_QUEUE_URL = os.environ['CANARY_MAIN_QUEUE_URL']
CANARY_DLQ_URL = os.environ['CANARY_DLQ_URL']
CANARY_TG_ARN = os.environ['CANARY_TG_ARN']

def is_canary_active():
    """카나리 대상 그룹에 정상 인스턴스가 있는지 확인하여 카나리 배포 진행 여부를 반환합니다."""
    try:
        response = elbv2.describe_target_health(TargetGroupArn=CANARY_TG_ARN)
        for target in response.get('TargetHealthDescriptions', []):
            if target.get('TargetHealth', {}).get('State') == 'healthy':
                print("Canary deployment is active (found healthy target in Canary-TG).")
                return True
        print("Canary deployment is NOT active (no healthy targets in Canary-TG).")
        return False
    except Exception as e:
        print(f"Error checking canary status: {e}")
        # 오류 발생 시 안전하게 비활성 상태로 간주
        return False

def move_messages(source_queue_url, dest_queue_url):
    """소스 SQS 대기열의 모든 메시지를 목적지 대기열로 이동시킵니다."""
    message_count = 0
    while True:
        # 소스 대기열에서 메시지 수신
        response = sqs.receive_message(
            QueueUrl=source_queue_url,
            MaxNumberOfMessages=10, # 한 번에 최대 10개 처리
            WaitTimeSeconds=1
        )
        
        messages = response.get('Messages', [])
        if not messages:
            break # 더 이상 메시지가 없으면 루프 종료

        # 목적지 대기열로 메시지 전송
        entries = [{'Id': msg['MessageId'], 'MessageBody': msg['Body']} for msg in messages]
        sqs.send_message_batch(QueueUrl=dest_queue_url, Entries=entries)

        # 소스 대기열에서 메시지 삭제
        delete_entries = [{'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']} for msg in messages]
        sqs.delete_message_batch(QueueUrl=source_queue_url, Entries=delete_entries)
        
        message_count += len(messages)

    print(f"Moved {message_count} messages from {source_queue_url} to {dest_queue_url}")

def lambda_handler(event, context):
    if not is_canary_active():
        # --- 카나리 배포 중이 아닐 때의 로직 ---
        print("Executing standard mode logic...")
        
        # 1. 라이브 DLQ -> 라이브 메인 큐로 리드라이브
        move_messages(LIVE_DLQ_URL, LIVE_MAIN_QUEUE_URL)
        
        # 2. (정리) 카나리 DLQ에 남은 메시지가 있다면 라이브 메인 큐로 이동
        move_messages(CANARY_DLQ_URL, LIVE_MAIN_QUEUE_URL)
        
        # 3. (정리) 카나리 메인 큐에 남은 메시지가 있다면 라이브 메인 큐로 이동
        move_messages(CANARY_MAIN_QUEUE_URL, LIVE_MAIN_QUEUE_URL)
        
    else:
        # --- 카나리 배포 중일 때의 로직 ---
        print("Executing canary mode logic...")
        
        # 1. 라이브 DLQ -> 라이브 메인 큐로 리드라이브
        move_messages(LIVE_DLQ_URL, LIVE_MAIN_QUEUE_URL)
        
        # 2. 카나리 DLQ -> 카나리 메인 큐로 리드라이브
        move_messages(CANARY_DLQ_URL, CANARY_MAIN_QUEUE_URL)
        
    return {
        'statusCode': 200,
        'body': 'DLQ re-drive process completed.'
    }