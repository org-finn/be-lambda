import os
import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT', '5432')

def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
    except Exception as e:
        logger.error(f"DB Connection Error: {e}")
        raise e

def lambda_handler(event, context):
    logger.info("Lambda 3: Saving Keyword & Ticker results to RDS...")
    
    records_to_insert = []
    current_time = datetime.now(timezone.utc).isoformat()
    # 요청 조건: 현재 UTC 기준 분/초 drop 처리한 시간 설정
    utc_hourly_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    
    # SQS 배치로 인입된 레코드들 일괄 파싱 및 매핑
    for record in event.get('Records', []):
        try:
            item = json.loads(record['body'])
            
            article_ids_list = item.get('articles')
            article_ids_str = ",".join(article_ids_list) if article_ids_list else None
            
            # ⭐️ Lambda 2에서 넘어온 tickerId 추출 및 튜플에 추가
            row = (
                str(uuid.uuid4()),
                item.get('keyword'),
                article_ids_str,       # 값이 없으면 None(Null) 반영
                item.get('sentiment', 0),
                utc_hourly_date,
                current_time,
                item.get('tickerId')   # 새로 추가된 매핑 데이터 (매핑 없으면 None)
            )
            records_to_insert.append(row)
        except Exception as e:
            logger.error(f"Error parsing save record: {e}")
            continue

    if records_to_insert:
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            # ⭐️ 인서트 쿼리에 ticker_id 컬럼 추가
            insert_query = """
                INSERT INTO articles_with_keyword (
                    id, keyword, articles, sentiment, date, created_at, ticker_id
                ) VALUES %s
            """
            
            execute_values(cur, insert_query, records_to_insert)
            conn.commit()
            cur.close()
            logger.info(f"✅ Successfully batch inserted {len(records_to_insert)} records (with ticker_id) to RDS.")
            
        except Exception as e:
            logger.error(f"Lambda 3 Database Insert Error: {e}")
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
                
    return {'statusCode': 200, 'body': f'Processed {len(records_to_insert)} items.'}