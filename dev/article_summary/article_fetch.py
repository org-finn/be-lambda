import os
import json
import logging
import boto3
from itertools import groupby
from operator import itemgetter
from supabase import create_client, Client
import uuid
from polygon import RESTClient
from datetime import datetime, timedelta, timezone

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
LLM_REQUEST_SQS_QUEUE_URL = os.environ.get('LLM_REQUEST_SQS_QUEUE_URL')
sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')
polygon_client = RESTClient(POLYGON_API_KEY)


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

def fetch_articles(published_utc_gte, limit):
    """Polygon API를 호출하여 뉴스를 가져옵니다."""
    try:
        
        response = polygon_client.list_ticker_news(
            limit=limit,
            order='asc',
            published_utc_gte=published_utc_gte
        )
        if response:
            return list(response)
    except Exception as e:
        logger.error("Polygon news API failed", e)
    return []


def lambda_handler(event, context):
    """여러 종목 정보가 포함된 뉴스를 각 티커별로 그룹화하여 SQS로 전송합니다."""
    logger.info("Article collection Lambda handler started.")
    
    try:
        # 1. 최근 24시간 기준 시간 계산 (UTC)
        now = datetime.now(timezone.utc)
        time_24h_ago = now - timedelta(hours=24)
        published_utc_gte = time_24h_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"Will fetch article >= {published_utc_gte} (UTC)")
        
        # 2. 티커 정보 가져오기 (매핑용)
        tickers = get_tickers_from_parameter_store() # 선 파라미터 조회
        if tickers is None:
            tickers = get_tickers_from_supabase() # 안전장치로 db에서 조회
        
        # ticker_code -> {id, name} 매핑 맵 생성
        # API 응답의 'tickers' 필드는 ticker code 리스트입니다.
        ticker_info_map = { ticker[1]: {'id': ticker[0], 'name': ticker[2]} for ticker in tickers }
        
        # 3. 전체 최신 뉴스 수집
        logger.info("Fetching general articles...")
        articles_raw = fetch_articles(published_utc_gte, 100)

        # 4. 데이터 가공 및 그룹화
        # API 결과의 각 기사는 여러 티커를 포함할 수 있습니다 ("tickers": ["AAPL", "MSFT"])
        # 따라서 이를 "티커별 기사" 형태로 플랫하게 펼쳐야 합니다.
        
        flattened_data = []
        
        for article in articles_raw:
            article_tickers = article.tickers
            if not article_tickers:
                continue
                
            title = article.title
            description = article.description
            
            if not title or not description:
                continue

            # 기사에 포함된 각 티커에 대해 데이터 생성
            for ticker_code in article_tickers:
                # 우리가 관리하는 티커인지 확인
                ticker_info = ticker_info_map.get(ticker_code)
                if ticker_info:
                    flattened_data.append({
                        'ticker_id': ticker_info['id'],
                        'short_company_name': ticker_info['name'],
                        'title': title,
                        'description': description
                    })

        # ticker_id 기준으로 정렬 (groupby를 위해 필수)
        flattened_data.sort(key=itemgetter('ticker_id'))
        
        grouped_data = []
        # ticker_id로 그룹화
        for ticker_id, items in groupby(flattened_data, key=itemgetter('ticker_id')):
            items_list = list(items)
            first_item = items_list[0]
            
            # 해당 티커의 기사 리스트 생성
            articles = [
                {'title': item['title'], 
                 'description': item['description']}
                for item in items_list
            ]
            
            # 너무 많으면 자르기 (SQS 용량 고려)
            if len(articles) > 20:
                articles = articles[:20]

            grouped_data.append({
                'tickerId': ticker_id,
                'shortCompanyName': first_item['short_company_name'],
                'articles': articles
            })

        logger.info(f"Generated {len(grouped_data)} unique ticker messages.")

        # 5. SQS로 메시지 전송 (배치 처리)
        entries = []
        for item in grouped_data:
            # 메시지 본문 생성 (LLM이 처리하기 좋은 포맷)
            message_body = {
                'tickerId': item['tickerId'],
                'shortCompanyName': item['shortCompanyName'],
                'articles': item['articles'],
                'requestId': str(uuid.uuid4())
            }

            entries.append({
                'Id': str(uuid.uuid4()), # SQS 배치 전송용 고유 ID
                'MessageBody': json.dumps(message_body, ensure_ascii=False)
            })

        # 10개 단위로 나누어 전송 (SQS 배치 제한)
        for i in range(0, len(entries), 10):
            batch = entries[i:i+10]
            try:
                sqs_client.send_message_batch(
                    QueueUrl=LLM_REQUEST_SQS_QUEUE_URL,
                    Entries=batch
                )
                logger.info(f"Sent batch of {len(batch)} messages to SQS.")
            except Exception as e:
                logger.error(f"Failed to send batch to SQS: {e}")

        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully processed {len(grouped_data)} tickers and sent messages.")
        }

    except Exception as e:
        logger.exception("Critical error in lambda_handler")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }