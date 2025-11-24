import os
import json
import logging
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from itertools import groupby
from operator import itemgetter
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
    logger.info("Starting LLM message generation process (Prod - RDS)...")
    
    conn = None
    try:
        # 1. RDS 연결 및 쿼리 실행
        conn = get_db_connection()
        # RealDictCursor를 사용하여 결과를 딕셔너리 형태로 받음 (Supabase 호환)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 최적화된 쿼리: 인덱스를 활용하여 어제(Yesterday) 데이터 조회
        query = """
            SELECT 
                atr.ticker_id, 
                atr.short_company_name, 
                a.title, 
                a.description
            FROM article as a
            INNER JOIN article_ticker as atr ON a.id = atr.article_id
            WHERE a.published_date >= (CURRENT_DATE - INTERVAL '1 day') 
              AND a.published_date < CURRENT_DATE
            ORDER BY atr.ticker_id
        """
        
        cur.execute(query)
        data = cur.fetchall() # List[Dict] 형태 반환
        cur.close()

        if not data:
            logger.info("No articles found to process.")
            return {'statusCode': 200, 'body': 'No articles found.'}

        logger.info(f"Fetched {len(data)} article-ticker rows from RDS.")

        # 2. 데이터를 Ticker ID 별로 그룹화 (Grouping)
        # 쿼리에서 ORDER BY atr.ticker_id를 했으므로 groupby 사용 가능
        grouped_data = []
        # data가 RealDictRow 객체이므로 딕셔너리처럼 키 접근 가능
        for ticker_id, items in groupby(data, key=lambda x: str(x['ticker_id'])):
            items_list = list(items)
            first_item = items_list[0]
            
            articles = [
                {'title': item['title'], 
                 'description': item['description']}
                for item in items_list
            ]

            grouped_data.append({
                'tickerId': ticker_id,
                'shortCompanyName': first_item['short_company_name'],
                'articles': articles
            })

        logger.info(f"Generated {len(grouped_data)} unique ticker messages.")

        # 3. SQS로 메시지 전송 (배치 처리)
        entries = []
        for item in grouped_data:
            message_body = {
                'tickerId': item['tickerId'],
                'shortCompanyName': item['shortCompanyName'],
                'articles': item['articles'],
                'requestId': str(uuid.uuid4())
            }

            entries.append({
                'Id': str(uuid.uuid4()),
                'MessageBody': json.dumps(message_body, ensure_ascii=False)
            })

        # 10개 단위 배치 전송
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
            'body': json.dumps(f"Successfully sent {len(entries)} messages to LLM queue.")
        }

    except Exception as e:
        logger.exception("Critical error in lambda_handler")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
    finally:
        # DB 연결 종료 (중요)
        if conn:
            conn.close()
            logger.info("RDS connection closed.")