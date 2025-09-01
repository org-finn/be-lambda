import os
import json
import logging
import requests

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수에서 Spring Boot 서버 API 엔드포인트 URL 가져오기
API_ENDPOINT_URL = os.environ.get('API_ENDPOINT_URL')

def lambda_handler(event, context):
    """SQS 메시지를 EC2 Spring Boot 서버의 API로 전송합니다."""
    
    if not API_ENDPOINT_URL:
        logger.error("API_ENDPOINT_URL environment variable is not set.")
        # 이 경우 재시도해도 소용없으므로 예외를 발생시키지 않음
        return {'statusCode': 500, 'body': 'Configuration error.'}

    messages = event.get('Records', [])
    if not messages:
        logger.info("No messages in SQS event. Exiting.")
        return {'statusCode': 200, 'body': 'No messages to process.'}
        
    logger.info("Received %d messages from SQS. Forwarding to %s", len(messages), API_ENDPOINT_URL)

    # Spring Boot API로 보낼 헤더
    headers = {
        'Content-Type': 'application/json'
    }

    success_count = 0
    # SQS는 배치로 메시지를 전달하므로, 각 메시지를 순회하며 API 호출
    for record in messages:
        message_body = record.get('body')
        try:
            # Spring Boot 서버로 POST 요청
            response = requests.post(
                API_ENDPOINT_URL,
                data=message_body,
                headers=headers,
                timeout=10 # 10초 타임아웃 설정
            )
            # 4xx, 5xx 에러 발생 시 예외를 발생시켜 재시도 유도
            response.raise_for_status()
            
            logger.info("Successfully sent message to API. ReceiptHandle: %s", record.get('receiptHandle'))
            success_count += 1
            
        except requests.exceptions.RequestException as e:
            logger.exception("Failed to send message to Spring Boot API. Error: %s", e)
            # ⭐️ API 서버 접속 실패는 재시도가 필요하므로 예외를 다시 발생시킴
            raise e

    return {
        'statusCode': 200,
        'body': f'Successfully forwarded {success_count}/{len(messages)} messages.'
    }