import os
import json
import logging
import uuid
import boto3
import requests
from datetime import datetime, timezone

# --- 설정 및 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

FINNHUB_API_KEY = os.environ.get('FINNHUB_API_KEY')
PREDICTION_LLM_QUEUE_URL = os.environ.get('PREDICTION_LLM_QUEUE_URL')

sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')

SQS_BATCH_SIZE = 10

def get_tickers_from_parameter_store():
    """Parameter Store에서 티커 목록 조회"""
    try:
        logger.info("Fetching tickers from Parameter Store...")
        param = ssm_client.get_parameter(Name='/articker/tickers')
        cached_data = json.loads(param['Parameter']['Value'])
        all_tickers = cached_data.get('tickers', [])
        ticker_codes = [item[1] for item in all_tickers if item]
        return ticker_codes
    except Exception as e:
        logger.exception("Failed to get tickers.")
        return []

def fetch_company_news(symbol, limit=100):
    """Finnhub에서 뉴스 조회 후 최대 limit 개수만큼 잘라서 반환"""
    url = "https://finnhub.io/api/v1/company-news"
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    params = {
        'symbol': symbol,
        'from': today_str,
        'to': today_str,
        'token': FINNHUB_API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        news_data = response.json()
        
        # [변경점] API 결과가 리스트인지 확인 후 최대 100개(limit)까지만 슬라이싱하여 반환합니다.
        if isinstance(news_data, list):
            return news_data[:limit]
        return []
    except Exception as e:
        logger.error(f"Failed to fetch news for {symbol}: {e}")
        return []

def send_sqs_batch(entries):
    """SQS 배치 전송"""
    if not entries:
        return
    try:
        response = sqs_client.send_message_batch(
            QueueUrl=PREDICTION_LLM_QUEUE_URL,
            Entries=entries
        )
        failed = len(response.get('Failed', []))
        if failed > 0:
            logger.error(f"SQS Batch Partial Failure: {failed} messages failed.")
    except Exception as e:
        logger.exception("Critical error sending SQS batch.")

def lambda_handler(event, context):
    if not FINNHUB_API_KEY or not PREDICTION_LLM_QUEUE_URL:
        logger.error("Environment variables missing.")
        return {"statusCode": 500, "body": "Config Error"}

    tickers = get_tickers_from_parameter_store()
    if not tickers:
        return {"statusCode": 200, "body": "No tickers found."}

    total_queued = 0
    sqs_buffer = []

    for symbol in tickers:
        # fetch_company_news 함수 내부에서 이미 최대 100개로 잘려서 반환됩니다.
        news_list = fetch_company_news(symbol)
        
        if not news_list:
            continue

        for news_item in news_list:
            # 헤드라인만 추출
            headline = news_item.get('headline')
            
            # 헤드라인이 비어있으면 스킵
            if not headline:
                continue

            # 전송할 데이터 페이로드 최소화
            payload = {
                'headline': headline,
                'symbol': symbol,                 # 예측 대상 기업
                'datetime': news_item.get('datetime') # 뉴스 발생 시간 (Time Series 분석용)
            }

            entry = {
                'Id': str(uuid.uuid4()),
                # 전체 news_item 대신 경량화된 payload 전송
                'MessageBody': json.dumps(payload),
                'MessageAttributes': {
                    'Source': {'StringValue': 'Finnhub', 'DataType': 'String'},
                    'Ticker': {'StringValue': symbol, 'DataType': 'String'}
                }
            }
            sqs_buffer.append(entry)
            total_queued += 1

            if len(sqs_buffer) >= SQS_BATCH_SIZE:
                send_sqs_batch(sqs_buffer)
                sqs_buffer = []

    # 남은 잔여 버퍼 전송
    if sqs_buffer:
        send_sqs_batch(sqs_buffer)

    logger.info(f"Done. Queued {total_queued} headlines.")
    return {"statusCode": 200, "body": "Success"}