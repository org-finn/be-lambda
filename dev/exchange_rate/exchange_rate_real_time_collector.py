import json
import os
import urllib.request
import boto3
import logging
from datetime import datetime, timedelta, timezone

# 1. 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client('sqs')

EXCHANGE_RATE_SQS_QUEUE_URL = os.environ.get('EXCHANGE_RATE_SQS_QUEUE_URL')
WON_DOLLOR_INDEX_CODE = 'C01'
WON_DOLLOR_INDEX_INFO = '원/달러'
INPUT_CURRENCY = 'USD'   # u3 (입력 통화)
TARGET_CURRENCY = 'KRW'  # u4 (대상 통화)

def lambda_handler(event, context):
    try:
        # 0. 필수 환경 변수 검증
        if not EXCHANGE_RATE_SQS_QUEUE_URL:
            raise ValueError("Required environment variable (EXCHANGE_RATE_SQS_QUEUE_URL) is missing.")

        # 1. 오늘 날짜 구하기 (KST 기준) - 로그용
        utc_now = datetime.now(timezone.utc)
        kst_now = utc_now + timedelta(hours=9)
        search_date = kst_now.strftime('%Y%m%d')
        
        logger.info(f"Start processing. Current KST Date: {search_date}")

        # 2. 네이버 환율 API 호출
        # 파라미터 설명: u3=INPUT_CURRENCY(입력통화), u4=TARGET_CURRENCY(대상통화), u1=keb(하나은행 기준)
        url = f"https://m.search.naver.com/p/csearch/content/qapirender.nhn?key=calculator&pkid=141&q=%ED%99%98%EC%9C%A8&where=m&u1=keb&u6=standardUnit&u7=0&u3={INPUT_CURRENCY}&u4={TARGET_CURRENCY}&u8=down&u2=1"
        
        # User-Agent 헤더 추가 (봇 차단 방지용)
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        
        with urllib.request.urlopen(req) as response:
            if response.getcode() != 200:
                raise Exception(f"API Request failed with status code: {response.getcode()}")
            
            response_body = response.read().decode('utf-8')
            data = json.loads(response_body)

        # 3. 데이터 파싱
        # 네이버 API 응답 구조: 
        # { "country": [ { "value": "1", "subValue": "", "currencyUnit": "USD", ... }, 
        #                { "value": "1,438.50", "subValue": "...", "currencyUnit": "KRW", ... } ] }
        # country[0]: 입력 통화(INPUT_CURRENCY), country[1]: 대상 통화(TARGET_CURRENCY) -> 우리가 필요한 환율 정보
        
        if 'country' not in data or len(data['country']) < 2:
            logger.error(f"Invalid API response format: {json.dumps(data)}")
            return {
                'statusCode': 500,
                'body': json.dumps('Invalid API response format.')
            }

        # 두 번째 항목(TARGET_CURRENCY)에서 환율 값 추출
        target_currency_data = data['country'][1]
        raw_rate = target_currency_data.get('value', '') # 예: "1,438.50"
        
        if not raw_rate:
            logger.info("Exchange rate (value) not found in the response.")
            return {
                'statusCode': 404,
                'body': json.dumps('Exchange rate not found.')
            }

        # 4. 데이터 포맷팅
        # Listener가 기대하는 포맷: deal_bas_r, cur_unit, indexCode, indexInfo
        usd_data = {
            'cur_unit': INPUT_CURRENCY,
            'cur_nm': '미국 달러',
            'deal_bas_r': raw_rate, # "1,438.50" (콤마 포함 문자열 그대로 전송, Listener가 처리함)
            'indexCode': WON_DOLLOR_INDEX_CODE,
            'indexInfo': WON_DOLLOR_INDEX_INFO
        }

        # 5. SQS로 데이터 전송
        sqs.send_message(
            QueueUrl=EXCHANGE_RATE_SQS_QUEUE_URL,
            MessageBody=json.dumps(usd_data, ensure_ascii=False)
        )
        
        logger.info(f"Successfully sent USD data to SQS. Rate: {raw_rate}, IndexCode: {usd_data.get('indexCode')}")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success', 'data': usd_data}, ensure_ascii=False)
        }

    except Exception as e:
        # 에러 발생 시 로그에 상세 정보 기록
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error', 'error': str(e)})
        }