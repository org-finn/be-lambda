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

NEWS_API_ORG_API_KEY = os.environ.get('NEWS_API_ORG_API_KEY')
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
        # ticker 구조가 [id, code, name]이라고 가정 시 code(index 1) 추출
        # 만약 [code, name] 구조라면 item[0]으로 변경 필요. 
        # (제공해주신 Finnhub 코드의 item[1]을 따릅니다)
        ticker_codes = [item[1] for item in all_tickers if item]
        return ticker_codes
    except Exception as e:
        logger.exception("Failed to get tickers.")
        return []

def fetch_news_api_org(query_keyword):
    """
    NewsAPI.org에서 뉴스 조회
    :param query_keyword: 검색할 키워드 (여기서는 Ticker Code 사용)
    """
    url = 'https://newsapi.org/v2/everything'
    
    # 어제 날짜 00:00:00 기준 (User provided logic)
    recent_day = (datetime.now(timezone.utc)).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=12)
    # NewsAPI는 ISO format string (YYYY-MM-DD)을 선호합니다.
    from_date_str = recent_day.strftime('%Y-%m-%d')

    params = {
        'q': query_keyword,        # 검색어 (예: Apple, AAPL)
        'from': from_date_str,     # 검색 시작일
        'sortBy': 'publishedAt',   # 최신순 정렬
        'language': 'en',          # 영어 기사만 필터링 (선택사항)
        'apiKey': NEWS_API_ORG_API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        
        # NewsAPI는 {'status': 'ok', 'totalResults': n, 'articles': [...]} 형태
        data = response.json()
        return data.get('articles', [])
        
    except Exception as e:
        logger.error(f"Failed to fetch news for {query_keyword}: {e}")
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
    if not NEWS_API_ORG_API_KEY or not PREDICTION_LLM_QUEUE_URL:
        logger.error("Environment variables missing.")
        return {"statusCode": 500, "body": "Config Error"}

    tickers = get_tickers_from_parameter_store()
    if not tickers:
        return {"statusCode": 200, "body": "No tickers found."}

    total_queued = 0
    sqs_buffer = []

    for symbol in tickers:
        # NewsAPI는 Ticker(예: AAPL)로 검색할 수도 있고, 회사명(Apple)으로 검색할 수도 있습니다.
        # 현재 로직은 Ticker Code를 그대로 쿼리로 사용합니다.
        news_list = fetch_news_api_org(symbol)
        
        if not news_list:
            continue

        for article in news_list:
            # [NewsAPI 필드 매핑]
            # title -> headline
            headline = article.get('title')
            
            # 헤드라인이 비어있거나 '[Removed]' 인 경우 스킵 (NewsAPI 특성)
            if not headline or headline == '[Removed]':
                continue

            # 전송할 데이터 페이로드 구성
            payload = {
                'headline': headline,
                'symbol': symbol,  # 검색에 사용한 심볼 (문맥 정보)
                # publishedAt -> datetime (ISO String 그대로 사용)
                'datetime': article.get('publishedAt') 
            }

            entry = {
                'Id': str(uuid.uuid4()),
                'MessageBody': json.dumps(payload),
            }
            sqs_buffer.append(entry)
            total_queued += 1

            # 배치 사이즈가 차면 전송
            if len(sqs_buffer) >= SQS_BATCH_SIZE:
                send_sqs_batch(sqs_buffer)
                sqs_buffer = []

    # 남은 버퍼 처리
    if sqs_buffer:
        send_sqs_batch(sqs_buffer)

    logger.info(f"Done. Queued {total_queued} headlines from NewsAPI.")
    return {"statusCode": 200, "body": "Success"}