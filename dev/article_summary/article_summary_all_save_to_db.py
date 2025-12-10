import os
import json
import logging
import boto3
from datetime import datetime, timezone
from supabase import create_client, Client
import uuid

# --- 설정 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
ARTICLE_LLM_RESULT_SQS_QUEUE_URL = os.environ.get('ARTICLE_LLM_RESULT_SQS_QUEUE_URL')

# 클라이언트 초기화
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
sqs_client = boto3.client('sqs')

def lambda_handler(event, context):
    logger.info("Starting Save LLM Result handler...")
    
    processed_receipt_handles = []
    records_to_insert = []

    # 1. SQS 메시지 파싱 및 데이터 매핑
    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            
            # SQS 메시지 필드 추출
            summary_date = body.get('summaryDate')
            
            # 날짜 처리: analyzedAt이 없으면 현재 시간 사용
            if not summary_date:
                summary_date = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()

            # DB 컬럼 매핑 (CamelCase JSON -> Snake_case DB Column)
            row = {
                "id" : str(uuid.uuid4()),
                "summary_date": summary_date,
                "positive_reasoning": body.get('positiveReasoning'),
                "negative_reasoning": body.get('negativeReasoning'),
                # varchar(100) 제한을 고려하여 문자열 자르기 (안전장치)
                "positive_keywords": (body.get('positiveKeywords') or "")[:100] if body.get('positiveKeywords') else None,
                "negative_keywords": (body.get('negativeKeywords') or "")[:100] if body.get('negativeKeywords') else None,
                "created_at" : datetime.now(timezone.utc).isoformat()
            }
            
            records_to_insert.append(row)
            processed_receipt_handles.append({
                'Id': record['messageId'],
                'ReceiptHandle': record['receiptHandle']
            })

        except Exception as e:
            logger.error(f"Error parsing record {record.get('messageId')}: {e}")
            continue

    # 2. Supabase에 일괄 삽입 (Bulk Insert)
    if records_to_insert:
        try:
            logger.info(f"Inserting {len(records_to_insert)} records into article_summary_all...")
            # Supabase insert 실행
            supabase.table('article_summary_all').upsert(
                records_to_insert, 
                ignore_duplicates=True
                ).execute()
            logger.info("Successfully inserted data into Supabase.")
        except Exception as e:
            logger.error(f"Failed to insert data into Supabase: {e}")
            # DB 저장 실패 시 에러를 던져서 SQS 메시지가 DLQ로 가거나 재시도되게 함
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed and saved {len(records_to_insert)} summaries.")
    }