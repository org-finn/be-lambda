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
    logger.info("Starting LLM message generation process...")

    try:
        # 1. Supabase RPC를 호출하여 최신 기사 데이터 가져오기
        # (ticker_id 기준으로 정렬된 상태로 반환됨)
        response = supabase.rpc('get_latest_ticker_articles', {}).execute()
        data = response.data

        if not data:
            logger.info("No articles found to process.")
            return {'statusCode': 200, 'body': 'No articles found.'}

        logger.info(f"Fetched {len(data)} article-ticker rows.")

        # 2. 데이터를 Ticker ID 별로 그룹화 (Grouping)
        # 데이터가 이미 ticker_id로 정렬되어 있다고 가정 (RPC 쿼리의 ORDER BY)
        grouped_data = []
        for ticker_id, items in groupby(data, key=itemgetter('ticker_id')):
            items_list = list(items)
            # 그룹의 첫 번째 항목에서 공통 정보(회사명 등) 추출
            first_item = items_list[0]
            
            # 해당 티커의 기사 리스트 생성
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
            # 메시지 본문 생성 (LLM이 처리하기 좋은 포맷)
            message_body = {
                'tickerId': item['tickerId'],
                'shortCompanyName': item['shortCompanyName'],
                'articles': item['articles'],
                'requestId': str(uuid.uuid4())
            }

            entries.append({
                'Id': str(uuid.uuid4()), # SQS 배치 전송용 고유 ID
                'MessageBody': json.dumps(message_body, ensure_ascii=False)
            })

        # 10개 단위로 나누어 전송 (SQS 배치 제한)
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
