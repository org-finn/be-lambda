import os
import json
import logging
import boto3
import psycopg2
from difflib import SequenceMatcher

# --- 설정 및 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT', '5432')
FETCH_QUEUE_URL = os.environ.get('FETCH_QUEUE_URL')

# SQS 클라이언트 초기화
sqs_client = boto3.client('sqs')


def get_db_connection():
    """RDS 데이터베이스 연결을 생성합니다."""
    try:
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
    except Exception as e:
        logger.error(f"DB Connection Error: {e}")
        raise e


def deduplicate_articles(articles, similarity_threshold=0.8):
    """제목(title)의 유사도를 기준으로 뉴스 기사 목록의 중복을 제거합니다."""
    deduplicated_articles = []
    
    for new_article in articles:
        new_title = new_article.get('title')
        if not new_title:
            continue
        
        is_duplicate = False
        for existing_article in deduplicated_articles:
            existing_title = existing_article.get('title')
            
            # SequenceMatcher로 두 제목 간의 유사도 비율 계산
            s = SequenceMatcher(None, new_title, existing_title)
            if s.ratio() >= similarity_threshold:
                is_duplicate = True
                break

        if not is_duplicate:
            deduplicated_articles.append(new_article)
            
    return deduplicated_articles


def lambda_handler(event, context):
    logger.info("Lambda 1: Fetching recent articles from RDS DB...")
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. DB에서 최근 24시간 이내의 기사 조회 (최대 50개)
        query = """
            SELECT id, title, description
            FROM article
            WHERE published_date >= NOW() - INTERVAL '1 day'
            ORDER BY published_date DESC
            LIMIT 50;
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        
        if not rows:
            logger.info("No articles found in the database within the last 24 hours.")
            return {'statusCode': 200, 'body': 'No articles to process.'}

        # 2. DB 조회 결과(Tuple)를 중복 제거 및 다음 람다 처리를 위해 Dict로 변환
        articles_to_process = []
        for row in rows:
            articles_to_process.append({
                "id": str(row[0]),          # UUID 타입인 id를 문자열로 변환하여 할당
                "title": row[1] or "",
                "description": row[2] or ""
            })
            
        # 3. 80% 유사도 기준 중복 제거 적용
        initial_count = len(articles_to_process)
        unique_articles = deduplicate_articles(articles_to_process, similarity_threshold=0.8)
        logger.info(f"Deduplication complete. Removed {initial_count - len(unique_articles)} articles.")
            
        # 4. SQS 큐로 하나의 묶음 메시지 전송
        logger.info(f"Sending {len(unique_articles)} unique articles to SQS...")
        sqs_client.send_message(
            QueueUrl=FETCH_QUEUE_URL,
            MessageBody=json.dumps({"articles": unique_articles})
        )
        
        return {
            'statusCode': 200,
            'body': f'Successfully fetched {len(unique_articles)} articles from DB and queued.'
        }
        
    except Exception as e:
        logger.error(f"Lambda 1 Critical Error: {e}")
        raise e
    finally:
        if conn:
            conn.close()