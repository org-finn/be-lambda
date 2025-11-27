import os
import json
import logging
import boto3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import uuid

# --- 설정 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수 (RDS 접속 정보)
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT', '5432')

# SQS 클라이언트 (필요 시 명시적 삭제 등을 위해 사용)
sqs_client = boto3.client('sqs')

def get_db_connection():
    """RDS 데이터베이스 연결을 생성합니다."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"ERROR: Could not connect to Postgres instance: {e}")
        raise e

def lambda_handler(event, context):
    logger.info("Starting Save LLM Result handler (Prod - RDS)...")
    
    records_to_insert = []
    
    # 1. SQS 메시지 파싱 및 데이터 매핑
    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            
            ticker_id = body.get('tickerId')
            short_company_name = body.get('shortCompanyName')
            summary_date = body.get('summaryDate')
            
            # 날짜 처리
            if not summary_date:
                summary_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

            # 튜플 형태로 데이터 구성 (execute_values 사용을 위해)
            # 순서: id, ticker_id, short_company_name, summary_date, pos_reason, neg_reason, neu_reason, pos_kw, neg_kw, neu_kw
            row = (
                str(uuid.uuid4()),
                ticker_id,
                short_company_name,
                summary_date,
                body.get('positiveReasoning'),
                body.get('negativeReasoning'),
                (body.get('positiveKeywords') or "")[:100],
                (body.get('negativeKeywords') or "")[:100],
                datetime.now(timezone.utc).isoformat()
            )
            
            records_to_insert.append(row)

        except Exception as e:
            logger.error(f"Error parsing record {record.get('messageId')}: {e}")
            continue

    # 2. RDS에 일괄 삽입 (Bulk Insert)
    if records_to_insert:
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            logger.info(f"Inserting {len(records_to_insert)} records into article_summary...")
            
            # execute_values를 사용한 고속 Bulk Insert
            # 주의: 'short_company_name' 컬럼이 DB 테이블에 존재해야 합니다.
            insert_query = """
                INSERT INTO article_summary (
                    id, 
                    ticker_id, 
                    short_company_name, 
                    summary_date, 
                    positive_reasoning, 
                    negative_reasoning, 
                    positive_keywords, 
                    negative_keywords, 
                    created_at
                ) VALUES %s
                ON CONFLICT (ticker_id, summary_date) DO NOTHING
            """
            
            execute_values(cur, insert_query, records_to_insert)
            conn.commit()
            
            logger.info("Successfully inserted data into RDS.")
            cur.close()

        except Exception as e:
            logger.error(f"Failed to insert data into RDS: {e}")
            if conn:
                conn.rollback()
            # DB 저장 실패 시 에러를 던져서 SQS 메시지가 DLQ로 가거나 재시도되게 함
            raise e
        finally:
            if conn:
                conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed and saved {len(records_to_insert)} summaries.")
    }