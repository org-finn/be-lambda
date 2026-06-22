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
    utc_hourly_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    
    for record in event.get('Records', []):
        try:
            item = json.loads(record['body'])
            
            # Lambda 2에서 조립한 리스트 형태의 articles 객체를 가져옴
            article_list_of_dicts = item.get('articles', [])
            
            # ⭐️ PostgreSQL의 JSON(또는 JSONB) 타입 컬럼에 넣기 위해 문자열로 직렬화
            # 비어있을 경우 "[]" (빈 JSON 배열) 문자열로 변환하여 저장
            articles_json_str = json.dumps(article_list_of_dicts, ensure_ascii=False) if article_list_of_dicts else '[]'
            
            row = (
                str(uuid.uuid4()),
                item.get('keyword'),
                articles_json_str,     # JSON 문자열 포맷 반영
                item.get('sentiment', 0),
                utc_hourly_date,
                current_time,
                item.get('tickerId')
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
            
            # DB의 articles 컬럼 타입이 JSON 또는 JSONB 로 변경되어야 정상 작동합니다.
            insert_query = """
                INSERT INTO articles_with_keyword (
                    id, keyword, articles, sentiment, date, created_at, ticker_id
                ) VALUES %s
            """
            
            execute_values(cur, insert_query, records_to_insert)
            conn.commit()
            cur.close()
            logger.info(f"✅ Successfully batch inserted {len(records_to_insert)} records (with JSON articles) to RDS.")
            
        except Exception as e:
            logger.error(f"Lambda 3 Database Insert Error: {e}")
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
                
    return {'statusCode': 200, 'body': f'Processed {len(records_to_insert)} items.'}