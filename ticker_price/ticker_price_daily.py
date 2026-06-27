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
import pytz
import exchange_calendars as xcals

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')

SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

# 티커 조회 용도로 사용
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')
polygon_client = RESTClient(POLYGON_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
US_CALENDAR = xcals.get_calendar("XNYS") # ✅ 미국 증시 캘린더
US_EASTERN_TZ = pytz.timezone("America/New_York") # ✅ 미국 동부 타임존


def get_tickers_from_parameter_store():
    """Parameter Store에서 저장된 티커 목록을 가져옵니다."""
    logger.info("Fetching tickers from Parameter Store.")
    try:
        response = ssm_client.get_parameters(
            Names=['/articker/tickers', '/articker/tickers/2']
        )
        
        all_tickers = []
        
        # 성공적으로 조회된 파라미터들을 순회하며 하나의 리스트로 병합합니다.
        for param in response.get('Parameters', []):
            try:
                cached_data = json.loads(param['Value'])
                all_tickers.extend(cached_data.get('tickers', []))
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON for parameter: {param.get('Name')}")
                
        # 만약 생성되지 않았거나 오타가 있는 파라미터가 있다면 경고 로그를 남깁니다.
        invalid_params = response.get('InvalidParameters', [])
        if invalid_params:
            logger.warning(f"Invalid or missing parameters: {invalid_params}")

        filtered_tickers = [
            (item[0], item[1]) for item in all_tickers
        ]
        
        return filtered_tickers
    
    except ssm_client.exceptions.ParameterNotFound:
        logger.info("No ticker cache found in Parameter Store.")
        return None
    except Exception as e:
        logger.exception("Failed to get tickers from Parameter Store.")
        return None
    
def get_tickers_from_supabase():
    """Supabase에서 처리할 모든 티커 목록을 조회합니다."""
    logger.info("Fetching tickers from Supabase.")
    
    try:
        response = supabase.table('ticker').select('id, code').execute()
        if response.data:
            tickers = [(item['id'], item['code']) for item in response.data]
            logger.info("Found %d tickers to process.", len(tickers))
            return tickers
        else:
            # 데이터가 없는 것은 재시도할 필요가 없는 정상 상황일 수 있음
            logger.warning("No tickers found from Supabase.")
            return []
    except Exception as e:
        # DB 연결 실패 등은 재시도가 필요한 오류이므로 예외 발생
        logger.exception("Failed to fetch tickers from Supabase.")
        raise e


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
    ticker_id = ticker_info[0]
    ticker_code = ticker_info[1]
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
    try:
        # 0. 휴장일 확인 로직
        # 람다 실행 시점의 미국 동부 시간 기준 '어제' 날짜 계산
        now_et = datetime.now(US_EASTERN_TZ)
        previous_day_str = now_et.strftime('%Y-%m-%d')

        # 어제(동부 시간 기준 오늘)가 거래일(session)이 아니었는지 확인
        if not US_CALENDAR.is_session(previous_day_str):
            logger.warning(f"Skipping execution because the previous day ({previous_day_str}) was a market holiday.")
            return {'statusCode': 200, 'body': f'Skipped: {previous_day_str} was a holiday.'}
        
        # 1. 처리할 티커 목록 조회
        tickers = get_tickers_from_parameter_store() # 선 파라미터 조회
        if tickers is None:
            tickers = get_tickers_from_supabase() # 안전장치로 db에서 조회
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
        if processed_count == 0 and len(tickers) > 0:
            raise Exception(f"Processed 0 tickers out of {len(tickers)}. All API calls might have failed.")
            
        logger.info("Lambda handler finished successfully.")
        return {
            'statusCode': 200,
            'body': f'Successfully processed and sent {processed_count} tickers to SQS.'
        }
    
    except Exception as e:
        logger.exception("A critical error occurred in the lambda handler.")
        # 🚨 예외를 다시 발생시켜 Lambda 실행을 '실패'로 AWS에 알립니다.
        raise e
