import os
import json
import logging
from datetime import datetime
from pytz import timezone
from supabase import create_client, Client
import uuid

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def lambda_handler(event, context):
    """
        Article 데이터를 받아 Supabase DB에 저장합니다.
    """

    records = event.get('Records', [])
    if not records:
        logger.warning("No records found in the event. Exiting.")
        return {'statusCode': 200, 'body': json.dumps('No records to process.')}

    logger.info(f"Received {len(records)} messages to process.")

    # 각 메시지를 순회하며 처리합니다.
    for record in records:
        message_id = record.get('messageId')
        try:
            message_body_str = record.get('body')
            if not message_body_str:
                logger.warning(f"Message with id {message_id} has an empty body. Skipping.")
                continue
            
            message_body = json.loads(message_body_str)
    
            article_data = message_body.get('article')
            if not article_data:
                logger.warning(f"Message with id {message_id} does not contain 'article' data. Skipping.")
                continue

            article_id = save_article(article_data)
            
            # unique 제약 조건을 통과한 경우에만, article_ticker 데이터 삽입을 허용함
            if article_id:
                logger.info(f"Article processed for messageId {message_id}. Article ID: {article_id}")
                insights = article_data.get('insights', [])
                if insights:
                    ticker_codes = [i['ticker_code'] for i in insights]
                    ticker_id_map = get_ticker_id_map(ticker_codes)
                    save_article_tickers(article_id, article_data, insights, ticker_id_map)
                else:
                    logger.info(f"No insights found for article in messageId {message_id}.")
            
        except Exception as e:
            # 특정 메시지 처리 중 발생하는 모든 예외 처리
            logger.error(f"An error occurred while processing messageId {message_id}: {e}", exc_info=True)
            raise e # 예외를 다시 발생시켜 SQS 재처리를 유도

    return {
        'statusCode': 200,
        'body': json.dumps({'message': f'Successfully processed {len(records)} messages.'})
    }
    
def save_article(article: dict) -> str:
    """
    Article을 DB에 저장합니다. 이미 존재하면 건너뛰고 ID를 반환합니다.
    supabase.upsert with ignore_duplicates=True를 사용하여 'INSERT IGNORE'를 구현합니다.
    """
    seoul_time = datetime.now(timezone("Asia/Seoul"))
    article_params = {
        'published_date': article.get('published_date'),
        'title': article.get('title'),
        'title_kr' : article.get('title_kr'),
        'description': article.get('description'),
        'description_kr': article.get('description_kr'),
        'article_url': article.get('article_url'),
        'thumbnail_url': article.get('thumbnail_url'),
        'author': article.get('author'),
        'distinct_id': article.get('distinct_id'),
        'tickers': article.get('tickers'),
        'created_at': seoul_time.isoformat()
    }

    # RPC 함수 호출
    response = supabase.rpc('insert_article_if_not_exists', {'article_data': article_params}).execute()

    # response.data는 성공 시 UUID 문자열, 중복 시 null(None)을 반환합니다.
    article_id = response.data
    
    if article_id:
        logger.info(f"New article inserted successfully. ID: {article_id}")
    else:
        logger.info(f"Article with distinct_id '{article.get('distinct_id')}' already exists. Skipping.")

    return article_id


def get_ticker_id_map(ticker_codes: list) -> dict:
    """
    Ticker 코드 리스트를 받아 Ticker ID(UUID) 맵을 반환합니다.
    """
    if not ticker_codes:
        return {}

    response = supabase.table('ticker').select('id, code, short_company_name').in_('code', ticker_codes).execute()
    
    if response.data:
        return {item['code']: {item['id'], item['short_company_name']} for item in response.data}
    return {}


def save_article_tickers(article_id: str, article_data: dict, insights: list, ticker_id_map: dict):
    """
    Article에 연관된 Ticker 정보들을 Batch Insert 합니다.
    """
    to_insert = []
    seoul_time = datetime.now(timezone("Asia/Seoul"))

    for insight in insights:
        ticker_code = insight['ticker_code']
        ticker_id, short_company_name = ticker_id_map.get(insight['ticker_code'])

        if not ticker_id:
            logger.warning(f"Ticker ID for code '{ticker_code}' not found. Skipping.")
            continue
            
        to_insert.append({
            'id' : str(uuid.uuid4()),
            'article_id': article_id,
            'ticker_id': ticker_id,
            'ticker_code': ticker_code,
            'title': article_data['title'],
            'title_kr': article_data['title_kr'],
            'sentiment': insight['sentiment'],
            'reasoning': insight['reasoning'],
            'reasoning_kr': insight['reasoning_kr'],
            'short_company_name' : short_company_name,
            'published_date': article_data['published_date'],
            'created_at': seoul_time.isoformat()
        })

    if not to_insert:
        logger.info("No valid article_ticker to insert.")
        return

    # Batch Upsert 실행
    response = supabase.table('article_ticker').upsert(
        to_insert,
        ignore_duplicates=True
    ).execute()
    
    # Supabase upsert는 count를 반환하지 않으므로, 로그 메시지를 단순화합니다.
    logger.info(f"Attempted to save {len(to_insert)} article_ticker data.")
