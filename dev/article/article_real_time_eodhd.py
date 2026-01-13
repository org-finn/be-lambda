import os
import json
import logging
import uuid
import boto3
import requests
from datetime import datetime, timezone, timedelta

# --- 설정 및 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

EODHD_API_KEY = os.environ.get('EODHD_API_KEY')
PREDICTION_LLM_QUEUE_URL = os.environ.get('PREDICTION_LLM_QUEUE_URL')

sqs_client = boto3.client('sqs')

API_FETCH_LIMIT = 300
SQS_BATCH_SIZE = 10

def fetch_eodhd_market_news():
    """
    EODHD에서 시장 대표 지수(SPY) 기준으로 대량 뉴스 조회 (1회 호출)
    """
    url = 'https://eodhd.com/api/news'
    
    recent_day = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    params = {
        's': 'SPY',                 # [변경] 개별 종목 대신 시장 대표 ETF(SPY) 사용
        'offset': 0,
        'limit': API_FETCH_LIMIT,   # [변경] 최대 200개 요청
        'from': recent_day.strftime('%Y-%m-%d'),
        'api_token': EODHD_API_KEY,
        'fmt': 'json'
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        if isinstance(data, dict) and 'status' in data and data['status'] != 200:
            logger.warning(f"EODHD Error: {data}")
            return []
            
        return data
        
    except Exception as e:
        logger.error(f"Failed to fetch market news: {e}")
        return []

def send_sqs_batch(entries):
    """SQS 배치 전송"""
    if not entries:
        return
    try:
        sqs_client.send_message_batch(
            QueueUrl=PREDICTION_LLM_QUEUE_URL,
            Entries=entries
        )
    except Exception as e:
        logger.exception("Critical error sending SQS batch.")

def lambda_handler(event, context):
    if not EODHD_API_KEY or not PREDICTION_LLM_QUEUE_URL:
        logger.error("Environment variables missing.")
        return {"statusCode": 500, "body": "Config Error"}

    # 1. 뉴스 수집 (단 1회 호출)
    logger.info("Fetching general market news from EODHD...")
    news_list = fetch_eodhd_market_news()
    
    if not news_list:
        return {"statusCode": 200, "body": "No news found."}

    total_queued = 0
    sqs_buffer = []

    for article in news_list:
        headline = article.get('title')
        if not headline:
            continue
        
        date_str = article.get('date')

        # 일반 시장 뉴스이므로 Symbol은 'Market'으로 표기
        payload = {
            'headline': headline,
            'symbol': 'Market',
            'datetime': date_str
        }

        entry = {
            'Id': str(uuid.uuid4()),
            'MessageBody': json.dumps(payload)
        }
        sqs_buffer.append(entry)
        total_queued += 1

        if len(sqs_buffer) >= SQS_BATCH_SIZE:
            send_sqs_batch(sqs_buffer)
            sqs_buffer = []

    if sqs_buffer:
        send_sqs_batch(sqs_buffer)

    logger.info(f"Done. Queued {total_queued} headlines from EODHD (Single Call).")
    return {"statusCode": 200, "body": "Success"}