import os
import json
import logging
from supabase import create_client, Client
from datetime import datetime,timezone
import uuid

# --- 로거, 환경 변수, 클라이언트 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
# Supabase 클라이언트는 함수 핸들러 밖에 선언하여 재사용합니다.
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def calculate_change_rate(new_close_price, previous_close):
    """등락률을 계산합니다."""
    # Supabase는 'numeric' 타입을 문자열로 반환할 수 있으므로 float() 캐스팅을 유지합니다.
    if previous_close is None or new_close_price is None or float(previous_close) == 0:
        return 0.0000
    return ((new_close_price - float(previous_close)) / float(previous_close))

def get_latest_close_prices_supabase():
    """Supabase RPC를 호출하여 모든 티커의 가장 최근 종가를 조회합니다."""
    logger.info("Fetching latest close prices for all tickers from Supabase.")
    
    # 1단계에서 생성한 SQL 함수를 호출합니다.
    response = supabase.rpc('get_latest_close_prices').execute()
    
    if not response.data:
        logger.warn("No previous close prices found in Supabase.")
        return {}

    # 빠른 조회를 위해 딕셔너리로 변환: {ticker_id: latest_close_price}
    # (RPC 반환 컬럼명 'ticker_id_out', 'close_out'을 사용)
    previous_closes = {row['ticker_id_out']: row['close_out'] for row in response.data}
    
    logger.info("Fetched latest close prices for %d tickers from Supabase.", len(previous_closes))
    return previous_closes

def lambda_handler(event, context):
    """SQS 메시지를 처리하여 Supabase DB에 데이터를 저장합니다."""
    logger.info("SQS to Supabase Lambda handler started.")
    
    messages = event.get('Records', [])
    if not messages:
        logger.info("No messages in SQS event. Exiting.")
        return {'statusCode': 200, 'body': 'No messages to process.'}

    logger.info("Received %d messages from SQS.", len(messages))

    processed_count = 0
    
    try:
        # 1. 메시지에서 티커 정보 추출
        ticker_data_from_sqs = [json.loads(msg['body']) for msg in messages]
        
        # 2. Supabase에서 필요한 모든 직전 종가를 한 번에 조회
        previous_closes = get_latest_close_prices_supabase()

        data_to_upsert = []
        insert_time = datetime.now(timezone.utc).isoformat()

        # 3. 각 메시지에 대해 등락률 계산 및 데이터 가공 (Upsert용 dict 리스트 생성)
        for data in ticker_data_from_sqs:
            previous_close = previous_closes.get(data['ticker_id'])
            change_rate = calculate_change_rate(data['close'], previous_close)
            
            data_to_upsert.append({
                'id' : str(uuid.uuid4()),
                'price_date': data['price_date'],
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'volume': data.get('volume'),
                'change_rate': change_rate,
                'ticker_code': data['ticker_code'], 
                'ticker_id': data['ticker_id'], 
                'created_at': insert_time
                # 'id'는 DB의 gen_random_uuid()에 의해 자동 생성되므로 제외
            })

        # 4. 가공된 데이터를 Supabase에 일괄 Upsert
        if data_to_upsert:
            logger.info("Attempting to upsert %d records to Supabase...", len(data_to_upsert))
            
            # 'ON CONFLICT' 로직을 'upsert'와 'on_conflict' 파라미터로 대체
            response = supabase.table('ticker_prices').upsert(
                data_to_upsert,
                on_conflict='ticker_id,price_date' # 충돌 감지 기준 (고유 인덱스)
            ).execute()

            # Supabase Python 클라이언트는 에러가 발생하면 예외를 던집니다.
            # 성공적으로 실행되면 response.data에 결과가 담깁니다.
            processed_count = len(response.data)
            logger.info("✅ Successfully upserted %d records into Supabase.", processed_count)

    except Exception as e:
        logger.exception("An unhandled exception occurred.")
        # 실패 시 SQS 메시지가 자동으로 재시도되므로 예외를 다시 발생시킴
        raise e

    return {'statusCode': 200, 'body': f'Successfully processed {processed_count} messages.'}