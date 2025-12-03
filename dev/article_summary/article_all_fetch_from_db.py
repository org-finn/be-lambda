import os
import json
import logging
import boto3
from itertools import groupby
from operator import itemgetter
from supabase import create_client, Client
import uuid

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
LLM_REQUEST_SQS_QUEUE_URL = os.environ.get('LLM_REQUEST_SQS_QUEUE_URL')
sqs_client = boto3.client('sqs')

def lambda_handler(event, context):
    logger.info("Starting LLM message generation process (Global Summary)...")

    try:
        # 1. Supabase RPC 호출 (전체 최신 기사 조회)
        # 중요: DB 레벨에서 이미 최대 20개로 제한하여 반환한다고 가정합니다.
        response = supabase.rpc('get_latest_articles', {}).execute()
        data = response.data

        if not data:
            logger.info("No articles found to process.")
            return {'statusCode': 200, 'body': 'No articles found.'}

        logger.info(f"Fetched {len(data)} articles in total (DB Limit applied).")

        # 2. 전체 기사 리스트 구성
        # 티커 구분 없이 모든 기사를 하나의 리스트로 만듭니다.
        # 필요 없는 필드는 제외하고 title, description만 추출하여 용량을 줄입니다.
        all_articles = [
            {'title': item['title'], 
             'description': item['description']}
            for item in data
        ]
        
        # [수정] DB에서 이미 20개 제한을 적용했으므로, Python 코드 내 Truncate 로직 제거
        # 20개 정도의 기사(Title + Description)는 SQS 용량(256KB)을 초과하지 않습니다.

        # 3. 메시지 본문 생성 (단일 메시지)
        message_body = {
            'type': 'global_market_summary', # 메시지 타입 구분
            'articles': all_articles,        # 전체 기사 리스트 (Max 20)
            'articleCount': len(all_articles),
            'requestId': str(uuid.uuid4())
        }

        # 4. SQS로 단일 메시지 전송
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
            'body': json.dumps(f"Successfully sent global summary message.")
        }

    except Exception as e:
        logger.exception("Critical error in lambda_handler")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }