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
DB_PORT = os.environ.get('DB_PORT')

# SQS 클라이언트
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
    logger.info("Starting Save LLM Result handler (Global Summary / Prod)...")
    
    records_to_insert = []
    
    # 1. SQS 메시지 파싱 및 데이터 매핑
    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            
            # SQS 메시지 필드 추출
            summary_date = body.get('summaryDate')
            
            # 날짜 처리 (요청하신 대로 분/초 단위 절삭 -> 시간 단위 저장)
            if not summary_date:
                summary_date = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()

            # 튜플 형태로 데이터 구성 (execute_values 사용을 위해)
            # 순서: id, summary_date, pos_reason, neg_reason, pos_kw, neg_kw, created_at
            row = (
                str(uuid.uuid4()),
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
            
            logger.info(f"Inserting {len(records_to_insert)} records into article_summary_all...")
            
            # [수정] 대상 테이블 article_summary_all
            # ON CONFLICT DO NOTHING (제약조건 위반 시 Skip)
            # summary_date 등에 unique constraint가 걸려있어야 합니다.
            insert_query = """
                INSERT INTO article_summary_all (
                    id, 
                    summary_date, 
                    positive_reasoning, 
                    negative_reasoning, 
                    positive_keywords, 
                    negative_keywords, 
                    created_at
                ) VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            execute_values(cur, insert_query, records_to_insert)
            conn.commit()
            
            logger.info("Successfully inserted data into RDS (duplicates skipped).")
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
        'body': json.dumps(f"Processed {len(records_to_insert)} items (duplicates skipped).")
    }