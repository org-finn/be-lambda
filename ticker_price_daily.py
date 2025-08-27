import json
import os
import boto3
import logging
from supabase import create_client, Client
from datetime import date, timedelta, datetime
import requests
import json
import time
import psycopg2
from psycopg2.extras import execute_batch
from polygon import RESTClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')

SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

# 티커 조회 용도로 사용
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

sqs_client = boto3.client('sqs')
polygon_client = RESTClient(POLYGON_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_tickers_from_supabase():
    """Supabase에서 처리할 모든 티커 목록을 조회합니다."""
    logger.info("Fetching tickers from Supabase.")
    try:
        response = supabase.table('ticker').select('id, code').execute()
        if response.data:
            tickers = [(item['id'], item['code']) for item in response.data]
            logger.info("Found %d tickers to process.", len(tickers))
            return tickers
    except Exception as e:
        logger.exception("Failed to fetch tickers from Supabase: %s", e)
    return []

def fetch_previous_day_data(ticker_code):
    """Polygon API를 호출하여 특정 티커의 전날 데이터를 가져옵니다."""
    try:
        response = polygon_client.get_previous_close_agg(
            ticker_code,
            adjusted="true",
        )
        
        if response and len(response) == 1:
            # vars()를 사용하여 리스트의 첫 번째 항목을 딕셔너리로 변환
            return vars(response[0])
        
    except Exception as e:
        logger.error("Polygon client failed for ticker %s: %s", ticker_code, e)
    return None

def process_ticker(ticker_info):
    """단일 티커 데이터를 가져와 SQS 메시지 형식으로 가공합니다."""
    ticker_id, ticker_code = ticker_info
    result = fetch_previous_day_data(ticker_code)
    
    if result:
        price_date = datetime.fromtimestamp(result['timestamp'] / 1000).strftime('%Y-%m-%d')
        
        # SQS 메시지로 보낼 데이터 객체 생성
        # 등락률 계산은 이제 SQS 메시지를 받는 쪽에서 담당
        message_payload = {
            'ticker_id': ticker_id,
            'ticker_code': ticker_code,
            'price_date': price_date,
            'open': result.get('open'),
            'high': result.get('high'),
            'low': result.get('low'),
            'close': result.get('close'),
            'volume': int(result.get('volume'))
        }
        logger.info("Successfully fetched data for %s", ticker_code)
        return message_payload
    else:
        logger.warning("Could not retrieve price data for %s.", ticker_code)
        return None

def lambda_handler(event, context):
    logger.info("Lambda handler started: Fetching stock prices to send to SQS.")
    
    # 1. 처리할 티커 목록 조회
    tickers = get_tickers_from_supabase()
    if not tickers:
        return {'statusCode': 200, 'body': 'No tickers to process.'}
        
    messages_to_send = []

    # 2. 순차 처리를 통해 각 티커의 데이터를 API로 가져오기
    logger.info("Starting sequential processing for %d tickers...", len(tickers))
    for ticker in tickers:
        result = process_ticker(ticker)
        if result:
            messages_to_send.append(result)

    # 3. 수집된 데이터를 SQS 큐로 전송
    if messages_to_send:
        logger.info("Sending %d messages to SQS queue...", len(messages_to_send))
        
        # SQS는 최대 10개씩 메시지를 묶어 보낼 수 있음 (Batch 전송)
        for i in range(0, len(messages_to_send), 10):
            batch = messages_to_send[i:i+10]
            entries = [
                # Id는 배치 내에서 유니크해야 하므로 ticker_code 사용
                {'Id': msg['ticker_code'], 'MessageBody': json.dumps(msg)} 
                for msg in batch
            ]
            
            try:
                sqs_client.send_message_batch(
                    QueueUrl=SQS_QUEUE_URL,
                    Entries=entries
                )
            except Exception as e:
                logger.exception("Failed to send batch to SQS: %s", e)
                return {'statusCode': 500, 'body': "Failed to send batch to SQS"}
        
        logger.info("✅ Successfully sent all message batches to SQS.")
    
    processed_count = len(messages_to_send)
    logger.info("Lambda handler finished. Processed %d tickers.", processed_count)
    return {
        'statusCode': 200,
        'body': f'Successfully processed and sent {processed_count} tickers to SQS.'
    }