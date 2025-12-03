import json
import os
import urllib.request
import boto3
import logging
from datetime import datetime, timedelta

# 1. 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client('sqs')

# 환경 변수 로드 (Cold Start 최적화)
AUTH_KEY = os.environ.get('AUTH_KEY')
EXCHANGE_RATE_SQS_QUEUE_URL = os.environ.get('EXCHANGE_RATE_SQS_QUEUE_URL')
WON_DOLLOR_INDEX_CODE = 'C01'
WON_DOLLOR_INDEX_INFO = '원/달러'

def lambda_handler(event, context):
    try:
        # 0. 필수 환경 변수 검증
        if not AUTH_KEY or not EXCHANGE_RATE_SQS_QUEUE_URL:
            raise ValueError("Required environment variables (AUTH_KEY or EXCHANGE_RATE_SQS_QUEUE_URL) are missing.")

        # 1. 오늘 날짜 구하기 (KST 기준)
        # Lambda는 기본 UTC 시간이므로 9시간을 더합니다.
        utc_now = datetime.utcnow()
        kst_now = utc_now + timedelta(hours=9)
        search_date = kst_now.strftime('%Y%m%d') # YYYYMMDD 포맷
        
        logger.info(f"Start processing. Current KST Date: {search_date}")

        # 2. 외부 API 호출 설정
        url = f"https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={AUTH_KEY}&searchdate={search_date}&data=AP01"
        
        # 3. API 요청 (urllib 사용)
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            if response.getcode() != 200:
                raise Exception(f"API Request failed with status code: {response.getcode()}")
            
            response_body = response.read().decode('utf-8')
            data = json.loads(response_body)

        # 4. 데이터 검증 (휴일 등 데이터 없는 경우 처리)
        if not data or not isinstance(data, list):
            logger.info("No data received from API (Possibly holiday or weekend).")
            return {
                'statusCode': 200,
                'body': json.dumps('No data available today.')
            }

        # 5. USD 정보 필터링
        # cur_unit이 'USD'인 첫 번째 항목 찾기
        usd_data = next((item for item in data if item['cur_unit'] == 'USD'), None)

        if not usd_data:
            logger.info("USD data not found in the response.")
            return {
                'statusCode': 404,
                'body': json.dumps('USD data not found.')
            }

        # [추가 요청 사항] indexCode 필드 추가
        usd_data['indexCode'] = WON_DOLLOR_INDEX_CODE
        usd_data['indexInfo'] = WON_DOLLOR_INDEX_INFO

        # 6. SQS로 데이터 전송
        sqs.send_message(
            QueueUrl=EXCHANGE_RATE_SQS_QUEUE_URL,
            # ensure_ascii=False를 해야 한글이 깨지지 않고 전송됨
            MessageBody=json.dumps(usd_data, ensure_ascii=False)
        )
        
        logger.info(f"Successfully sent USD data to SQS. DealBaseRate: {usd_data.get('deal_bas_r')}, IndexCode: {usd_data.get('indexCode')}")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success', 'data': usd_data}, ensure_ascii=False)
        }

    except Exception as e:
        # exc_info=True는 에러 발생 시 스택 트레이스를 로그에 남깁니다.
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
        }