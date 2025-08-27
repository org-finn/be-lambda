import os
import json
import logging
import boto3
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from polygon import RESTClient
import pytz

# --- 로거, 환경 변수, 클라이언트 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

MARKET_TIMEZONE = 'US/Eastern' # Nasdaq/NYSE 시장 기준 시간대

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
    """티커별로 뉴스를 그룹화하여 SQS 큐로 전송합니다."""
    logger.info("Article collection Lambda handler started.")
    
    try:
        # 0. 핸들러 시작 시 시장 상태를 한 번만 확인
        is_market_open = get_market_status()
        
        # 1. n분 전 시간 계산 (EST 기준)
        time_n_minutes_ago = (datetime.now(pytz.timezone(MARKET_TIMEZONE)) - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # ⭐ 티커별로 기사를 저장할 딕셔너리 및 중복 방지용 set
        articles_by_ticker = {}
        seen_article_ids = set()

        # 2. 전체 최신 뉴스 수집 (limit 20)
        logger.info("Fetching general articles...")
        general_articles = fetch_articles(published_utc_gte=time_n_minutes_ago, limit=20)
        for article in general_articles:
            if article.id not in seen_article_ids:
                sentiment = None
                reasoning = None
                if hasattr(article, 'insights') and article.insights:
                    first_insight = article.insights[0]
                    sentiment = first_insight.get('sentiment')
                    reasoning = first_insight.get('sentiment_reasoning')
                            
                payload = {
                    'published_date': article.published_utc, 'title': article.title,
                    'description': article.description, 'article_url': article.article_url,
                    'thumbnail_url': getattr(article, 'image_url', None), 'author': article.author,
                    'distinct_id': article.id, 'sentiment': sentiment,
                    'reasoning': reasoning
                }
                # 티커가 없는 뉴스는 'GENERAL' 키로 그룹화
                if 'GENERAL' not in articles_by_ticker:
                    articles_by_ticker['GENERAL'] = []
                articles_by_ticker['GENERAL'].append(payload)
                seen_article_ids.add(article.id)

        # 3. 티커별 최신 뉴스 수집 (limit 10)
        tickers = get_tickers_from_supabase()
        if tickers:
            for ticker_id, ticker_code, short_company_name in tickers:
                logger.info("Fetching articles for %s...", ticker_code)
                ticker_articles = fetch_articles(published_utc_gte=time_n_minutes_ago, ticker_code=ticker_code, limit=10)
                
                for article in ticker_articles:
                    if article.id not in seen_article_ids:
                        sentiment = None
                        reasoning = None
                        if hasattr(article, 'insights') and article.insights:
                            first_insight = article.insights[0]
                            sentiment = first_insight.get('sentiment')
                            reasoning = first_insight.get('sentiment_reasoning')
                            
                        payload = {
                            'published_date': article.published_utc, 'title': article.title,
                            'description': article.description, 'article_url': article.article_url,
                            'thumbnail_url': getattr(article, 'image_url', None), 'author': article.author,
                            'distinct_id': article.id, 'sentiment': sentiment,
                            'reasoning': reasoning    
                        }
                        # 해당 티커 코드로 그룹화
                        if ticker_code not in articles_by_ticker:
                            articles_by_ticker[ticker_code] = []
                        articles_by_ticker[ticker_code].append(payload)
                        seen_article_ids.add(article.id)

        # 4. 수집된 뉴스를 티커 단위로 SQS 큐로 전송
        if not articles_by_ticker:
            logger.info("No new articles found.")
            return {'statusCode': 200, 'body': 'No new articles found.'}

        logger.info("Sending news for %d tickers/groups to SQS queue...", len(articles_by_ticker))
        
        entries_to_send = []
        for ticker, articles in articles_by_ticker.items():
            message_body = {
                'ticker_code': ticker,
                'articles': articles,
                'is_market_open': is_market_open,
                'created_at': datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
            }
            entries_to_send.append({
                'Id': ticker,
                'MessageBody': json.dumps(message_body)
            })

        # 배치 단위 삽입 (최대 10개 그룹씩)
        for i in range(0, len(entries_to_send), 10):
            batch = entries_to_send[i:i+10]
            sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=batch)
        
        logger.info("✅ Successfully sent all article groups to SQS.")
        
    except Exception as e:
        logger.exception("A critical error occurred in the lambda handler.")
        raise e

    return {
        'statusCode': 200,
        'body': f'Successfully processed and sent news for {len(articles_by_ticker)} groups to SQS.'
    }