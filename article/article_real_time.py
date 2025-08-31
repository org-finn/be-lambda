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
        status = polygon_client.get_market_status()
        nasdaq_status = getattr(status.exchanges, 'nasdaq', 'unknown').lower()
        is_open = nasdaq_status == 'open'
        logger.info(f"Market status check complete. Market is {'OPEN' if is_open else 'CLOSED'}.")
        return is_open
    except Exception as e:
        logger.exception("An error occurred during market status check. Assuming market is CLOSED.")
        return False
    
def fetch_articles(published_utc_gte, ticker_code, limit):
    """Polygon API를 호출하여 뉴스를 가져옵니다."""
    try:
        response = polygon_client.list_ticker_news(
            ticker=ticker_code,
            limit=limit,
            order='asc',
            published_utc_gte=published_utc_gte
        )
        if response:
            return response
    except Exception as e:
        logger.error("Polygon news API failed for ticker %s: %s", ticker_code or "ALL", e)
    return []

def lambda_handler(event, context):
    """여러 종목 정보가 포함된 뉴스를 각 티커별로 그룹화하여 SQS로 전송합니다."""
    logger.info("Article collection Lambda handler started.")
    
    try:
        is_market_open = get_market_status()
        current_utc_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        if is_market_open:
            published_utc_gte = (current_utc_time - timedelta(hours=2)) \
            .strftime('%Y-%m-%dT%H:%M:%SZ') # created_at이 아닌 published_date gt 조건이므로, 좀 넉넉하게 범위를 잡아야함
        else:
            published_utc_gte = (current_utc_time - timedelta(hours=4)) \
            .strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info("Will fetch article %s ~ %s of UTC", published_utc_gte, 
                    current_utc_time.strftime('%Y-%m-%dT%H:%M:%SZ'))
        
        prediction_date = datetime.now(pytz.timezone('US/Eastern')) \
            .replace(second=0, microsecond=0) # 데이터 간 날짜 통일을 위해 사용
        
        # Supabase에서 티커 정보를 미리 가져와 빠른 조회를 위한 맵과 set 생성
        all_tickers_from_db = get_tickers_from_supabase()
        supported_tickers_set = set(ticker[1] for ticker in all_tickers_from_db)
        
        # 1. 전체 최신 뉴스 수집
        logger.info("Fetching general articles...")
        articles_to_process = fetch_articles(published_utc_gte, None, 100)
        
        articles_to_send = []
        # 2. 아티클별 페이로드 생성
        for article in articles_to_process:
            # ⭐️ 공통 페이로드 생성 (티커, 감정 정보 제외)
            payload = {
                'published_date': article.published_utc, 'title': article.title,
                'description': article.description, 'article_url': article.article_url,
                'thumbnail_url': getattr(article, 'image_url', None), 'author': article.author,
                'disinct_id' : article.id
            }
            
            # tickers(code) 필터링 및 추가
            tickers = []
            for ticker_code in article.tickers:
                 if ticker_code in supported_tickers_set:
                     tickers.append(ticker_code)
            payload['tickers'] = tickers
            
            # insights 필터링 및 추가
            insights = []
            for insight in article.insights:
                ticker_code = insight.ticker
                sentiemnt = insight.sentiment
                reasoning = insight.sentiment_reasoning
                if ticker_code in supported_tickers_set:
                    insights.append({
                        "ticker_code" : ticker_code,
                        "sentiment" : sentiemnt,
                        "reasoning" : reasoning
                    })
            payload['insights'] = insights
            
            articles_to_send.append(payload)

        # 3. 수집된 뉴스를 SQS 큐로 전송
        if not articles_to_send:
            logger.info("No new articles to process.")
            return {'statusCode': 200, 'body': 'No new articles found.'}

        # SQS로 보낼 총 기사 수 계산
        total_article_count = len(articles_to_send)
        logger.info("Sending %d unique articles to SQS queue",
            total_article_count)
        
        # 4. SQS로 메시지 전송
        entries_to_send = []
        for article in articles_to_send:
            message_body = {
                'article': article,
                'is_market_open': is_market_open,
                'prediction_date' : prediction_date.isoformat(),
                'created_at': datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
            }
            disinct_id = article['disinct_id']
            entries_to_send.append({
                'Id': disinct_id.replace('.', '-'), # SQS ID에 '.' 허용 안됨
                'MessageBody': json.dumps(message_body)
            })
        
        # 배치 단위 삽입 (최대 10개 그룹씩)
        for i in range(0, len(entries_to_send), 10):
            batch = entries_to_send[i:i+10]
            sqs_client.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=batch)
        
        logger.info("✅ Successfully sent all articles to SQS.")
        
    except Exception as e:
        logger.exception("A critical error occurred in the lambda handler.")
        raise e

    return {
        'statusCode': 200,
        'body': f'Successfully processed and sent {len(entries_to_send)} news to SQS.'
    }