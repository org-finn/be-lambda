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
            order='desc',
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
        current_utc_time = datetime.now(timezone.utc)
        time_n_minutes_ago = (current_utc_time - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ') # created_at이 아닌 published_date gt 조건이므로, 좀 넉넉하게 범위를 잡아야함
        logger.info("Will fetch article %s ~ %s of UTC", time_n_minutes_ago, current_utc_time.strftime('%Y-%m-%dT%H:%M:%SZ'))
        
        prediction_date = datetime.now(pytz.timezone('US/Eastern')) \
            .replace(second=0, microsecond=0) # 데이터 간 날짜 통일을 위해 사용
        
        articles_by_ticker = {}
        seen_article_ids = set()
        
        # Supabase에서 티커 정보를 미리 가져와 빠른 조회를 위한 맵과 set 생성
        all_tickers_from_db = get_tickers_from_supabase()
        ticker_info_map = {
            ticker[1]: {'id': ticker[0], 'name': ticker[2]} for ticker in all_tickers_from_db
        }
        supported_tickers_set = set(ticker_info_map.keys())

        # 처리할 모든 뉴스를 담을 리스트 (전체 뉴스 + 티커별 뉴스)
        articles_to_process = []
        
        # 1. 전체 최신 뉴스 수집
        logger.info("Fetching general articles...")
        articles_to_process.extend(fetch_articles(time_n_minutes_ago, None, 50))
        
        # 2. 티커별 최신 뉴스 수집
        if all_tickers_from_db:
            for _, ticker_code, _ in all_tickers_from_db:
                logger.info("Fetching articles for %s...", ticker_code)
                articles_to_process.extend(fetch_articles(time_n_minutes_ago, ticker_code, 20))

        # 3. 수집된 모든 뉴스를 순회하며 티커별로 그룹화
        for article in articles_to_process:
            if article.id in seen_article_ids:
                continue

            sentiment, reasoning = None, None
            if hasattr(article, 'insights') and article.insights:
                first_insight = article.insights[0]
                sentiment = first_insight.sentiment
                reasoning = first_insight.sentiment_reasoning

            # ⭐️ 공통 페이로드 생성 (티커 정보 제외)
            base_payload = {
                'published_date': article.published_utc, 'title': article.title,
                'description': article.description, 'article_url': article.article_url,
                'thumbnail_url': getattr(article, 'image_url', None), 'author': article.author,
                'sentiment': sentiment, 'reasoning': reasoning
            }
            
            # 이 기사와 연관된 모든 티커를 찾음
            related_tickers = set(article.tickers)
            if hasattr(article, 'insights') and article.insights:
                for insight in article.insights:
                    related_tickers.add(insight.ticker)

            is_related_to_supported_ticker = False
            if related_tickers:
                # 각 연관 티커 그룹에 뉴스 추가
                for ticker_code in related_tickers:
                    # 우리 서비스에서 지원하는 티커인지 확인
                    if ticker_code in supported_tickers_set:
                        payload = base_payload.copy()
                        # ⭐️ 원본 ID와 티커 코드를 조합하여 새로운 고유 ID 생성
                        payload['distinct_id'] = f"{article.id}-{ticker_code}"
                        articles_by_ticker.setdefault(ticker_code, []).append(payload)
                        is_related_to_supported_ticker = True
            
            # 연관 티커가 없거나, 있더라도 모두 지원하지 않는 티커일 경우 GENERAL 그룹에 추가
            if not is_related_to_supported_ticker:
                payload = base_payload.copy()
                # 원본 ID 사용(GENERAL에 속하는 아티클들은 distinct_id를 그대로 쓰므로 무조건 유니크함)
                payload['distinct_id'] = article.id
                articles_by_ticker.setdefault('GENERAL', []).append(payload)

            seen_article_ids.add(article.id)

        # 4. 수집된 뉴스를 SQS 큐로 전송
        if not articles_by_ticker:
            logger.info("No new articles to process.")
            return {'statusCode': 200, 'body': 'No new articles found.'}

        ticker_groups_to_send = list(articles_by_ticker.keys())
        logger.info(
            "Sending news for %d tickers/groups to SQS queue: %s",
            len(ticker_groups_to_send),
            ticker_groups_to_send
        )
        
        entries_to_send = []
        for ticker_code, articles in articles_by_ticker.items():
            ticker_info = ticker_info_map.get(ticker_code, {})
            message_body = {
                'ticker_code': ticker_code,
                'ticker_id': ticker_info.get('id'),
                'short_company_name': ticker_info.get('name'),
                'articles': articles,
                'is_market_open': is_market_open,
                'prediction_date' : prediction_date.isoformat(),
                'created_at': datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
            }
            entries_to_send.append({
                'Id': ticker_code.replace('.', '-'), # SQS ID에 '.' 허용 안됨
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