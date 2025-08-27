import os
import json
import logging
import boto3
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# --- 로거, 환경 변수, 클라이언트 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT')
DB_USER = os.environ.get('DB_USER')

db_connection = None
db_credentials = None


def get_db_connection():
    """데이터베이스 연결을 생성하거나 기존 연결을 반환합니다."""
    global db_connection
    if db_connection is None or db_connection.closed != 0:
        try:
            db_connection = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            logger.info("✅ Successfully connected to RDS PostgreSQL database.")
        except psycopg2.Error as e:
            logger.exception("❌ Database connection failed.")
            raise e
    return db_connection

def calculate_change_rate(new_close_price, previous_close):
    """등락률을 계산합니다."""
    if previous_close is None or new_close_price is None or float(previous_close) == 0:
        return 0.0000
    return ((new_close_price - float(previous_close)) / float(previous_close))

def get_latest_close_prices(cur):
    """모든 티커의 가장 최근 종가를 한 번의 쿼리로 조회하여 딕셔너리로 반환합니다."""
    logger.info("Fetching latest close prices for all tickers.")
    query = """
        WITH LatestPrices AS (
            SELECT
                ticker_id,
                close,
                ROW_NUMBER() OVER(PARTITION BY ticker_id ORDER BY price_date DESC) as rn
            FROM
                ticker_prices
        )
        SELECT
            ticker_id,
            close
        FROM
            LatestPrices
        WHERE
            rn = 1;
    """
    cur.execute(query)
    # 빠른 조회를 위해 딕셔너리로 변환: {ticker_id: latest_close_price}
    previous_closes = {row[0]: row[1] for row in cur.fetchall()}
    logger.info("Fetched latest close prices for %d tickers from DB.", len(previous_closes))
    return previous_closes

def lambda_handler(event, context):
    """SQS 메시지를 처리하여 RDS에 데이터를 저장합니다."""
    logger.info("SQS to RDS Lambda handler started.")
    
    # SQS 이벤트에서 메시지 추출
    messages = event.get('Records', [])
    if not messages:
        logger.info("No messages in SQS event. Exiting.")
        return {'statusCode': 200, 'body': 'No messages to process.'}

    logger.info("Received %d messages from SQS.", len(messages))

    conn = None
    processed_count = 0
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1. 메시지에서 티커 정보 추출
        ticker_data_from_sqs = [json.loads(msg['body']) for msg in messages]
        
        # 2. 필요한 모든 직전 종가를 한 번에 조회
        previous_closes = get_latest_close_prices(cur)

        data_to_insert = []
        insert_time = datetime.now()

        # 3. 각 메시지에 대해 등락률 계산 및 데이터 가공
        for data in ticker_data_from_sqs:
            previous_close = previous_closes.get(data['ticker_id'])
            change_rate = calculate_change_rate(data['close'], previous_close)
            
            data_to_insert.append((
                data['price_date'], data.get('open'), data.get('high'), data.get('low'),
                data.get('close'), data.get('volume'), change_rate, data['ticker_code'], 
                data['ticker_id'], insert_time
            ))

        # 4. 가공된 데이터를 DB에 일괄 저장
        if data_to_insert:
            insert_query = """
                INSERT INTO ticker_prices (
                    price_date, open, high, low, close, volume, change_rate, ticker_code, ticker_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker_id, price_date) DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                    close = EXCLUDED.close, volume = EXCLUDED.volume, change_rate = EXCLUDED.change_rate;
            """
            execute_batch(cur, insert_query, data_to_insert)
            conn.commit()
            processed_count = cur.rowcount
            logger.info("✅ Successfully inserted/updated %d records into RDS.", processed_count)

    except Exception as e:
        logger.exception("An unhandled exception occurred.")
        if conn: conn.rollback()
        # 실패 시 SQS 메시지가 자동으로 재시도되므로 여기서 예외를 다시 발생시키는 것이 좋음
        raise e
    finally:
        if conn and conn.closed == 0:
            cur.close()

    return {'statusCode': 200, 'body': f'Successfully processed {processed_count} messages.'}