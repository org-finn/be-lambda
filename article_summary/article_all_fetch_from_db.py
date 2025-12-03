import os
import json
import logging
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
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

LLM_REQUEST_SQS_QUEUE_URL = os.environ.get('LLM_REQUEST_SQS_QUEUE_URL')

# SQS 클라이언트 초기화
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
    logger.info("Starting LLM message generation process (Global Summary / Prod)...")
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. 어제 날짜의 모든 최신 기사 조회 (Global Summary용)
        # 티커 구분 없이 시간순 정렬하여 가져옵니다.
        # DB 부하 및 SQS 용량을 고려하여 최대 20~30개 정도로 제한(LIMIT)하는 것이 안전합니다.
        query = """
            SELECT 
                a.title, 
                a.description
            FROM article as a
            WHERE a.published_date >= (CURRENT_DATE - INTERVAL '1 day') 
              AND a.published_date < CURRENT_DATE
            ORDER BY a.published_date DESC
            LIMIT 20
        """
        
        cur.execute(query)
        data = cur.fetchall()
        cur.close()

        if not data:
            logger.info("No articles found to process.")
            return {'statusCode': 200, 'body': 'No articles found.'}

        # 로그: DB에서 가져온 총 기사 수
        logger.info(f"Fetched {len(data)} articles from RDS (Limit applied).")

        # 2. 전체 기사 리스트 구성
        # RealDictCursor 덕분에 딕셔너리처럼 접근 가능
        all_articles = [
            {'title': item['title'], 
             'description': item['description']}
            for item in data
        ]

        # 3. 메시지 본문 생성 (단일 메시지)
        message_body = {
            'type': 'global_market_summary', # 메시지 타입 구분
            'articles': all_articles,        # 전체 기사 리스트
            'articleCount': len(all_articles),
            'requestId': str(uuid.uuid4()),
            'timestamp': context.aws_request_id
        }

        # 4. SQS로 단일 메시지 전송 (send_message 사용)
        try:
            sqs_client.send_message(
                QueueUrl=LLM_REQUEST_SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body, ensure_ascii=False)
            )
            logger.info(f"Successfully sent global summary message with {len(all_articles)} articles.")
        except Exception as e:
            logger.error(f"Failed to send message to SQS: {e}")
            raise e

        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully processed and sent global summary.")
        }

    except Exception as e:
        logger.exception("Critical error in lambda_handler")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
    finally:
        if conn:
            conn.close()
            logger.info("RDS connection closed.")