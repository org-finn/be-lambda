import os
import json
import logging
import boto3
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from polygon import RESTClient
import pytz
from collections import defaultdict

# --- 로거, 환경 변수, 클라이언트 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
ARTICLE_SQS_QUEUE_URL = os.environ.get('ARTICLE_SQS_QUEUE_URL')
PREDICTION_SQS_QUEUE_URL = os.environ.get('PREDICTION_SQS_QUEUE_URL')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# 클라이언트는 핸들러 함수 밖에 선언하여 재사용 (성능 최적화)
sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')
polygon_client = RESTClient(POLYGON_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_tickers_from_parameter_store():
    """Parameter Store에서 저장된 티커 목록을 가져옵니다."""
    logger.info("Fetching tickers from Parameter Store.")
    
    try:
        param = ssm_client.get_parameter(Name='/articker/tickers')
        
        # 저장된 JSON 문자열을 파싱
        cached_data = json.loads(param['Parameter']['Value'])
        all_tickers = cached_data.get('tickers', [])

        filtered_tickers = [
            (item[0], item[1], item[2]) for item in all_tickers
        ]
        
        return filtered_tickers
    
    except ssm_client.exceptions.ParameterNotFound:
        logger.info("No ticker cache found in Parameter Store.")
        return None
    except Exception as e:
        logger.exception("Failed to get tickers from Parameter Store.")
        return None
    
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
        if ticker_code:
            response = polygon_client.list_ticker_news(
                ticker=ticker_code,
                limit=limit,
                order='asc',
                published_utc_gte=published_utc_gte
            )
        else:
            response = polygon_client.list_ticker_news(
                limit=limit,
                order='asc',
                published_utc_gte=published_utc_gte
            )
        if response:
            return response
    except Exception as e:
        logger.error("Polygon news API failed for ticker %s: %s", ticker_code or "ALL", e)
    return []

def add_news_count_for_sentiment(insight, ticker, sentiment_stats):

    sentiment = insight.sentiment
    if sentiment == 'positive':
        sentiment_stats[ticker]["positive"] += 1
    elif sentiment == 'negative':
        sentiment_stats[ticker]["negative"] += 1
    else: # 'neutral' 또는 None일 경우
        sentiment_stats[ticker]["neutral"] += 1


def lambda_handler(event, context):
    """여러 종목 정보가 포함된 뉴스를 각 티커별로 그룹화하여 SQS로 전송합니다."""
    logger.info("Article collection Lambda handler started.")
    
    try:
        is_market_open = get_market_status()
        current_utc_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        if is_market_open:
            published_utc_gte = (current_utc_time - timedelta(hours=1)) \
            .strftime('%Y-%m-%dT%H:%M:%SZ') # created_at이 아닌 published_date gt 조건이므로, 좀 넉넉하게 범위를 잡아야함
        else:
            published_utc_gte = (current_utc_time - timedelta(hours=12)) \
            .strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info("Will fetch article %s ~ %s of UTC", published_utc_gte, 
                    current_utc_time.strftime('%Y-%m-%dT%H:%M:%SZ'))
        
        prediction_date = datetime.now(timezone.utc) \
            .replace(hour=0, minute=0, second=0, microsecond=0) # 데이터 간 날짜 통일을 위해 사용
        
        tickers = get_tickers_from_parameter_store() # 선 파라미터 조회
        if tickers is None:
            tickers = get_tickers_from_supabase() # 안전장치로 db에서 조회
        ticker_info_map = { ticker[1]: {'id': ticker[0], 'name': ticker[2]} for ticker in tickers }
        supported_tickers_set = set(ticker[1] for ticker in tickers)
        
        # 1. 전체 최신 뉴스 수집
        logger.info("Fetching general articles...")
        articles_to_process = fetch_articles(published_utc_gte, None, 100)
        
        articles_to_send = []
        # 티커별 sentiment_news_count
        sentiment_stats = defaultdict(lambda: {"positive": 0, "negative": 0, "neutral": 0})
        
        # 2. 아티클별 페이로드 생성
        for article in articles_to_process:
            # ⭐️ 공통 페이로드 생성 (티커, 감정 정보 제외)
            payload = {
                'published_date': article.published_utc, 'title': article.title,
                'description': article.description, 'article_url': article.article_url,
                'thumbnail_url': getattr(article, 'image_url', None), 'author': article.author,
                'distinct_id' : article.id
            }
            
            # tickers(code) 필터링 및 추가
            tickers = []
            for ticker_code in article.tickers:
                 if ticker_code in supported_tickers_set: # 우리 서비스에서 지원하는 티커여야함
                     tickers.append(ticker_info_map.get(ticker_code, {}).get('name'))
            if tickers:
                payload['tickers'] = tickers
            else:
                payload['tickers'] = None
                
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
                    add_news_count_for_sentiment(insight, ticker_code, sentiment_stats) # 뉴스 감정 count 업데이트
            payload['insights'] = insights
            
            articles_to_send.append(payload)

        # 3. 수집된 뉴스를 SQS 큐로 전송
        if not articles_to_send:
            logger.info("No new articles to process.")
        else:
            # 3-1. 아티클 메시지 전송
            # SQS로 보낼 총 기사 수 계산
            logger.info("Sending %d unique articles to SQS queue...",
                len(articles_to_send))
            
            entries_to_send = []
            for article in articles_to_send:
                message_body = {
                    'article': article,
                    'is_market_open': is_market_open,
                    'created_at': datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
                }
                distinct_id = article['distinct_id']
                entries_to_send.append({
                    'Id': distinct_id.replace('.', '-'), # SQS ID에 '.' 허용 안됨
                    'MessageBody': json.dumps(message_body)
                })
            
            # 배치 단위 삽입 (최대 10개 그룹씩)
            for i in range(0, len(entries_to_send), 10):
                batch = entries_to_send[i:i+10]
                sqs_client.send_message_batch(QueueUrl=ARTICLE_SQS_QUEUE_URL, Entries=batch)
            
            logger.info("✅ Successfully sent all articles to SQS.")
            
        # 3-2. 예측 메시지 전송(정규장 외에도 다음 정규장을 위해 예측 계속 수행)
        if not sentiment_stats:
            logger.info("No new sentiment stats to process.")
        else:
            logger.info("Sending %d sentiment stats to SQS queue...", 
                len(sentiment_stats))
            
            stats_entries_to_send = []
            for ticker_code, counts in sentiment_stats.items():
                ticker_info = ticker_info_map.get(ticker_code, {})
                message_body = {
                    'tickerId': ticker_info.get('id'),
                    'type' : 'article',
                    'payload' : {
                        'positiveArticleCount': counts['positive'],
                        'negativeArticleCount': counts['negative'],
                        'neutralArticleCount': counts['neutral'],
                        'predictionDate': prediction_date.isoformat(),
                        'createdAt': datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
                    }
                }
                stats_entries_to_send.append({
                    'Id': ticker_code.replace('.', '-'),
                    'MessageBody': json.dumps(message_body)
                })

            # 통계 메시지 일괄 전송
            for i in range(0, len(stats_entries_to_send), 10):
                batch = stats_entries_to_send[i:i+10]
                sqs_client.send_message_batch(QueueUrl=PREDICTION_SQS_QUEUE_URL, Entries=batch)
            logger.info("✅ Successfully sent all sentiment stats to SQS.")

    except Exception as e:
        logger.exception("A critical error occurred in the lambda handler.")
        raise e

    return {
        'statusCode': 200,
        'body': f'Successfully processed and sent news/predictions to SQS.'
    }