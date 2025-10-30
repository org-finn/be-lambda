import os
import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from pytz import timezone
import uuid

# --- 로거 및 환경 변수, 전역 변수 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ⚠️ Secrets Manager 대신 환경 변수에서 직접 DB 정보를 가져옵니다.
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# Lambda 실행 컨텍스트 간에 DB 연결을 재사용하기 위한 전역 변수
db_conn = None

def get_db_connection():
    """RDS PostgreSQL 데이터베이스 연결을 생성하거나 기존 연결을 재사용합니다."""
    global db_conn
    # 연결이 없거나 끊어졌을 경우에만 새로 연결합니다.
    if db_conn is None or db_conn.closed != 0:
        try:
            logger.info(f"Connecting to database host: {DB_HOST}")
            db_conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logger.info("✅ Database connection successful.")
        except psycopg2.Error as e:
            logger.error(f"❌ Database connection failed: {e}", exc_info=True)
            raise e
    return db_conn

def lambda_handler(event, context):
    """SQS 메시지를 받아 RDS DB에 데이터를 저장합니다."""
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.fatal(f"Could not establish database connection. Aborting. Error: {e}")
        raise e

    records = event.get('Records', [])
    if not records:
        logger.warning("No records found in the event. Exiting.")
        return {'statusCode': 200, 'body': json.dumps('No records to process.')}

    logger.info(f"Received {len(records)} messages to process.")

    for record in records:
        message_id = record.get('messageId')
        try:
            # 각 메시지 처리를 위한 트랜잭션 시작
            with conn.cursor() as cursor:
                message_body = json.loads(record.get('body', '{}'))
                article_data = message_body.get('article')
                if not article_data:
                    logger.warning(f"Message {message_id} is missing 'article' data. Skipping.")
                    continue

                # 1. Article 저장 시도
                article_id = save_article(cursor, article_data)
                
                # 2. 신규 Article인 경우에만 Ticker 정보 저장
                if article_id:
                    logger.info(f"Article processed for messageId {message_id}. Article ID: {article_id}")
                    insights = article_data.get('insights', [])
                    if insights:
                        ticker_codes = [i['ticker_code'] for i in insights]
                        ticker_id_map = get_ticker_id_map(cursor, ticker_codes)
                        save_article_tickers(cursor, article_id, article_data, insights, ticker_id_map)

            # 메시지 처리가 성공하면 트랜잭션 커밋
            conn.commit()

        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}", exc_info=True)
            if conn: conn.rollback()
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps({'message': f'Successfully processed {len(records)} messages.'})
    }

# --- 비즈니스 로직 함수들은 변경할 필요가 없습니다 ---

def save_article(cursor, article: dict):
    """DB 함수(RPC)를 호출하여 Article을 저장하고, 신규 생성 시에만 ID를 반환합니다."""
    seoul_time = datetime.now(timezone("Asia/Seoul"))
    article_params = {
        'published_date': article.get('published_date'),
        'title': article.get('title'), 'description': article.get('description'),
        'article_url': article.get('article_url'), 'thumbnail_url': article.get('thumbnail_url'),
        'author': article.get('author'), 'distinct_id': article.get('distinct_id'),
        'tickers': article.get('tickers'), 'created_at': seoul_time.isoformat()
    }
    
    cursor.execute("SELECT insert_article_if_not_exists(%s);", (json.dumps(article_params),))
    article_id = cursor.fetchone()[0]

    if article_id:
        logger.info(f"New article inserted successfully. ID: {article_id}")
    else:
        logger.info(f"Article with distinct_id '{article.get('distinct_id')}' already exists. Skipping.")
    return article_id

def get_ticker_id_map(cursor, ticker_codes: list) -> dict:
    """Ticker 코드 리스트로 Ticker ID 맵을 조회합니다."""
    if not ticker_codes: return {}
    cursor.execute("SELECT id, code, short_company_name FROM ticker WHERE code = ANY(%s);", (ticker_codes,))
    return {code: (ticker_id, short_company_name) for ticker_id, code, short_company_name in cursor.fetchall()}

def save_article_tickers(cursor, article_id: str, article_data: dict, insights: list, ticker_id_map: dict):
    """Article Ticker 정보들을 Batch Insert 합니다."""
    seoul_time = datetime.now(timezone("Asia/Seoul"))
    to_insert = []
    for insight in insights:
        ticker_code = insight['ticker_code']
        ticker_id, short_company_name = ticker_id_map.get(insight['ticker_code'])
        
        if not ticker_id:
            logger.warning(f"Ticker ID for code '{ticker_code}' not found. Skipping.")
            continue
        to_insert.append((
            str(uuid.uuid4()), article_id, ticker_id, ticker_code, article_data['title'],
            insight['sentiment'], insight['reasoning'], article_data['published_date'], 
            short_company_name, seoul_time.isoformat()
        ))

    if not to_insert:
        logger.info("No valid article_tickers to insert.")
        return

    query = """
        INSERT INTO article_ticker (
            id, article_id, ticker_id, ticker_code, title, sentiment, 
            reasoning, published_date, short_company_name, created_at
        ) VALUES %s;
    """
    psycopg2.extras.execute_values(cursor, query, to_insert)
    logger.info(f"Attempted to save {len(to_insert)} article_ticker data.")