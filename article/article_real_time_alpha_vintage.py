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

ALPHA_VINTAGE_API_KEY = os.environ.get('ALPHA_VINTAGE_API_KEY')
PREDICTION_LLM_QUEUE_URL = os.environ.get('PREDICTION_LLM_QUEUE_URL')

sqs_client = boto3.client('sqs')

# 한 번에 가져올 최대 뉴스 개수 (API 파라미터)
API_FETCH_LIMIT = 300
# SQS 배치 전송 크기 (AWS 제한)
SQS_BATCH_SIZE = 10

def format_av_datetime(av_time_str):
    """Alpha Vantage 시간 포맷 변환 (YYYYMMDDTHHMM -> ISO 8601)"""
    try:
        dt_obj = datetime.strptime(av_time_str, '%Y%m%dT%H%M')
        return dt_obj.isoformat()
    except Exception:
        return av_time_str

def fetch_market_news():
    """
    특정 종목이 아닌 'Financial Markets' 전체 뉴스 조회 (1회 호출)
    """
    url = 'https://www.alphavantage.co/query'
    
    # 어제 날짜 00:00 기준
    recent_day = (datetime.now(timezone.utc)).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=12)
    time_from_str = recent_day.strftime('%Y%m%dT%H%M')

    params = {
        'function': 'NEWS_SENTIMENT',
        'topics': 'financial_markets', # [변경] 특정 종목(tickers) 대신 시장 전체 토픽 사용
        'time_from': time_from_str,
        'limit': API_FETCH_LIMIT,      # [변경] 최대 200개 요청
        'sort': 'LATEST',
        'apikey': ALPHA_VINTAGE_API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        
        if 'Note' in data or 'Information' in data:
            logger.warning(f"Alpha Vantage API Info: {data}")
            return []
            
        return data.get('feed', [])
        
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
    if not ALPHA_VINTAGE_API_KEY or not PREDICTION_LLM_QUEUE_URL:
        logger.error("Environment variables missing.")
        return {"statusCode": 500, "body": "Config Error"}

    # 1. 뉴스 수집 (단 1회 호출)
    logger.info("Fetching general market news from Alpha Vantage...")
    news_list = fetch_market_news()
    
    if not news_list:
        return {"statusCode": 200, "body": "No news found or API limit reached."}

    total_queued = 0
    sqs_buffer = []

    # 2. 결과 처리
    for article in news_list:
        headline = article.get('title')
        if not headline:
            continue

        raw_time = article.get('time_published')
        formatted_time = format_av_datetime(raw_time) if raw_time else datetime.now(timezone.utc).isoformat()

        # [중요] 특정 종목 쿼리가 아니므로 Symbol은 'Market' 등 일반적인 값으로 설정
        # LLM이 헤드라인을 분석하여 실제 종목을 추출할 것이므로 문제는 없음
        payload = {
            'headline': headline,
            'symbol': 'Market', 
            'datetime': formatted_time
        }

        entry = {
            'Id': str(uuid.uuid4()),
            'MessageBody': json.dumps(payload),
        }
        sqs_buffer.append(entry)
        total_queued += 1

        if len(sqs_buffer) >= SQS_BATCH_SIZE:
            send_sqs_batch(sqs_buffer)
            sqs_buffer = []

    if sqs_buffer:
        send_sqs_batch(sqs_buffer)

    logger.info(f"Done. Queued {total_queued} headlines from Alpha Vantage (Single Call).")
    return {"statusCode": 200, "body": "Success"}