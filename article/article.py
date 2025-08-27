import os
import json
import logging
import boto3
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from polygon import RESTClient

# --- 로거, 환경 변수, 클라이언트 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# 클라이언트는 핸들러 함수 밖에 선언하여 재사용 (성능 최적화)
sqs_client = boto3.client('sqs')
polygon_client = RESTClient(POLYGON_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_tickers_from_supabase():
    """Supabase에서 처리할 모든 티커 목록을 조회합니다."""
    logger.info("Fetching tickers from Supabase.")
    try:
        response = supabase.table('ticker').select('id, code, short_company_name').execute()
        if response.data:
            tickers = [(item['id'], item['code'], item['short_company_name']) for item in response.data]
            logger.info("Found %d tickers to process.", len(tickers))
            return tickers
    except Exception as e:
        logger.exception("Failed to fetch tickers from Supabase.")
    return []

def get_market_status():
    """Polygon API를 호출하여 현재 시장 상태가 'OPEN'인지 여부를 반환합니다."""
    try:
        logger.info("Checking market status...")
        # polygon-api-client 라이브러리 사용
        status = polygon_client.get_market_status()

        nasdaq_status = getattr(status.exchanges, 'nasdaq', 'unknown').lower()
        is_open = nasdaq_status == 'open'
        
        logger.info(f"Market status check complete. Market is {'OPEN' if is_open else 'CLOSED'}.")
        return is_open
    except Exception as e:
        logger.exception("An error occurred during market status check. Assuming market is CLOSED.")
        # 오류 발생 시 안전하게 시장을 닫힌 것으로 간주
        return False
    
def fetch_articles(published_utc_gte, ticker_code=None, limit=10):
    """Polygon API를 호출하여 뉴스를 가져옵니다."""
    try:
        # ticker_code가 있으면 해당 티커 뉴스를, 없으면 전체 뉴스를 가져옵니다.
        response = polygon_client.list_ticker_news(
            ticker=ticker_code,
            limit=limit,
            order='desc',
            published_utc_gte=published_utc_gte
        )
        if response:
            return response
    except Exception as e:
        logger.error("Polygon news API failed for ticker %s: %s", ticker_code or "ALL", e)
    return []

def lambda_handler(event, context):
    """티커별 뉴스 및 전체 최신 뉴스를 수집하여 중복 제거 후 SQS 큐로 전송합니다."""
    logger.info("Article collection Lambda handler started.")
    
    try:
        # 0. 핸들러 시작 시 시장 상태를 한 번만 확인
        is_market_open = get_market_status()
        
        # 1. n분 전 시간 계산 (UTC 기준)
        time_n_minutes_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        all_articles = {} # 중복 제거를 위해 딕셔너리 사용 {article_id: payload}

        # 2. 전체 최신 뉴스 수집 (limit 20)
        logger.info("Fetching general articles from the last 15 minutes...")
        general_articles = fetch_articles(published_utc_gte=time_n_minutes_ago, limit=20)
        for article in general_articles:
            payload = {
                'published_date': article.published_utc, 'title': article.title,
                'description': article.description, 'article_url': article.article_url,
                'thumbnail_url': getattr(article, 'image_url', None), 'author': article.author,
                'distinct_id': article.id, 'created_at': datetime.now().isoformat(),
                'is_market_open': is_market_open
            }
            all_articles[article.id] = payload

        # 3. 티커별 최신 뉴스 수집 (limit 10)
        tickers = get_tickers_from_supabase()
        if tickers:
            for ticker_id, ticker_code, short_company_name in tickers:
                logger.info("Fetching articles for %s from the last 15 minutes...", ticker_code)
                ticker_articles = fetch_articles(published_utc_gte=time_n_minutes_ago, ticker_code=ticker_code, limit=10)
                
                for article in ticker_articles:
                    # 딕셔너리에 추가하면서 자연스럽게 중복 제거
                    if article.id not in all_articles:
                        payload = {
                            'published_date': article.published_utc, 'title': article.title,
                            'description': article.description, 'article_url': article.article_url,
                            'thumbnail_url': getattr(article, 'image_url', None), 'author': article.author,
                            'distinct_id': article.id, 'ticker_id': ticker_id, 'ticker_code': ticker_code,
                            'short_company_name': short_company_name, 'created_at': datetime.now().isoformat(),
                            'is_market_open': is_market_open
                        }
                        all_articles[article.id] = payload

        # 4. 수집된 뉴스를 SQS 큐로 전송
        final_article_list = list(all_articles.values())
        if not final_article_list:
            logger.info("No new articles found.")
            return {'statusCode': 200, 'body': 'No new articles found.'}

        logger.info("Sending %d unique articles to SQS queue...", len(final_article_list))
        
        for i in range(0, len(final_article_list), 10):
            batch = final_article_list[i:i+10]
            entries = [{'Id': msg['distinct_id'], 'MessageBody': json.dumps(msg)} for msg in batch]
            sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
        
        logger.info("✅ Successfully sent all articles to SQS.")
        
    except Exception as e:
        logger.exception("A critical error occurred in the lambda handler.")
        raise e

    return {
        'statusCode': 200,
        'body': f'Successfully processed and sent {len(final_article_list)} articles to SQS.'
    }
