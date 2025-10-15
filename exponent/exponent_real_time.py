import json
import os
import boto3
import logging
from supabase import create_client, Client
from datetime import date, timedelta, datetime, timezone
import requests
import json
import time
import pytz
import exchange_calendars as xcals
from common.sqs_message_distributor_with_canary import send_prediction_messages

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')

# 티커 조회 용도로 사용
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KIS_BASE_URL = os.environ.get('KIS_BASE_URL')
KIS_APP_KEY = os.environ.get('KIS_APP_KEY')
KIS_APP_SECRET = os.environ.get('KIS_APP_SECRET')

EXPONENT_SQS_QUEUE_URL = os.environ.get('EXPONENT_SQS_QUEUE_URL')
PREDICTION_SQS_QUEUE_URL = os.environ.get('PREDICTION_SQS_QUEUE_URL')
sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')
KIS_TOKEN_PARAMETER_NAME = os.environ.get('KIS_TOKEN_PARAMETER_NAME')

MARKET_STATUS_PARAMETER_NAME = os.environ.get('MARKET_STATUS_PARAMETER_NAME')

wildcard_ticker_id = "00000000-0000-0000-0000-000000000000"
US_CALENDAR = xcals.get_calendar("XNYS")
US_EASTERN_TZ = pytz.timezone("America/New_York")
UTC_TZ = pytz.utc

def get_market_info(target_date_str: str) -> dict | None:
    """미국 동부 시간(ET) 기준으로 시장 정보를 계산합니다."""
    if not US_CALENDAR.is_session(target_date_str):
        logger.warning(f"Market is closed on {target_date_str}. No data will be processed.")
        return None

    schedule = US_CALENDAR.schedule.loc[target_date_str]
    
    # ✅ 변경점: UTC가 아닌 ET 기준으로 개장/폐장 시간 계산
    market_open_et = schedule['open'].astimezone(US_EASTERN_TZ)
    market_close_et = schedule['close'].astimezone(US_EASTERN_TZ)
    
    # 초 단위를 0으로 설정
    market_open_et = market_open_et.replace(second=0, microsecond=0)
    
    duration_minutes = (market_close_et - market_open_et).total_seconds() / 60
    max_len = int(duration_minutes / 5) + 1 # (시작 시간 ~ 끝 시간 차) + 1

    # ✅ 변경점: ET 개장 시간 반환
    return {"open_time_et": market_open_et, "max_len": max_len}

def is_first_data(market_time, price_date_str):
    market_info = get_market_info(price_date_str) # 가장 첫 데이터에 대한 엣지 케이스 처리

    if not market_info:
        return False
    
    market_open_et = market_info['open_time_et']
    market_open_utc = market_open_et.astimezone(UTC_TZ).replace(second=0, microsecond=0)
    if market_open_utc == market_time:
        return True
    
    return False

def get_exponent_from_parameter_store():
    """Parameter Store에서 저장된 지수 목록을 가져옵니다."""
    logger.info("Fetching exponents from Parameter Store.")
    try:
        param = ssm_client.get_parameter(Name='/articker/exponents')
        
        # 저장된 JSON 문자열을 파싱
        cached_data = json.loads(param['Parameter']['Value'])
        all_exponents = cached_data.get('exponents', [])

        filtered_exponents = [
            (item[0], item[1]) for item in all_exponents
        ]
        
        return filtered_exponents
    
    except ssm_client.exceptions.ParameterNotFound:
        logger.info("No ticker cache found in Parameter Store.")
        return None
    except Exception as e:
        logger.exception("Failed to get tickers from Parameter Store.")
        return None
    

def get_exponents_from_supabase():
    """Supabase에서 처리할 모든 지수 목록을 조회합니다."""
    logger.info("Fetching exponents from Supabase.")
    
    try:
        response = supabase.table('exponent').select('id, code').execute()
        if response.data:
            exponents = [(item['id'], item['code']) for item in response.data]
            logger.info("Found %d exponents to process.", len(exponents))
            return exponents
        else:
            # 데이터가 없는 것은 재시도할 필요가 없는 정상 상황일 수 있음
            logger.warning("No exponents found from Supabase.")
            return []
    except Exception as e:
        # DB 연결 실패 등은 재시도가 필요한 오류이므로 예외 발생
        logger.exception("Failed to fetch exponents from Supabase.")
        raise e
    
def get_token_from_parameter_store():
    """Parameter Store에서 토큰 객체를 가져오는 함수"""
    try:
        response = ssm_client.get_parameter(Name=KIS_TOKEN_PARAMETER_NAME, WithDecryption=True)
        # 저장된 값은 JSON 문자열이므로 파싱
        return json.loads(response['Parameter']['Value'])
    except ssm_client.exceptions.ParameterNotFound:
        logger.info("Token not found in Parameter Store.")
        return None
    except Exception as e:
        logger.exception("Failed to get token from Parameter Store: %s", e)
        return None

def save_token_to_parameter_store(token_info):
    """새 토큰 객체를 Parameter Store에 저장하는 함수"""
    try:
        ssm_client.put_parameter(
            Name=KIS_TOKEN_PARAMETER_NAME,
            Value=json.dumps(token_info), # 딕셔너리를 JSON 문자열로 변환
            Type='SecureString', # 암호화하여 저장
            Overwrite=True
        )
        logger.info("Successfully saved new token to Parameter Store.")
    except Exception as e:
        logger.exception("Failed to save token to Parameter Store: %s", e)

def get_new_token_from_api(key, secret, base_url):
    """접근 토큰을 발급받는 함수"""
    path = "/oauth2/tokenP"
    url = f"{base_url}{path}"
    
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": key,
        "appsecret": secret
    }
    
    try:
        logger.info("Requesting new access token...")
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
        
        return response.json() 
    except requests.exceptions.RequestException as e:
        # logger.exception은 오류의 스택 트레이스(traceback)를 함께 기록해 줍니다.
        logger.exception("An exception occurred during token issuance: %s", e)
        return None

def get_access_token(key, secret, base_url):
    """
    Parameter Store를 먼저 확인한 후, 유효한 토큰을 반환하는 메인 함수
    """
    token_info = get_token_from_parameter_store()
    
    # 토큰이 없거나, 발급된 지 23시간이 지났으면 새로 발급 (24시간 꽉 채우기보다 여유를 두는 방식)
    if not token_info or time.time() > token_info.get('issued_at', 0) + 82800: # 23시간
        logger.info("Token is invalid or expired. Issuing a new one from API...")
        
        new_token_data = get_new_token_from_api(key, secret, base_url)
        if new_token_data:
            logger.info("✅ Successfully issued a new access token.")
            # 현재 시간을 함께 저장하여 추후에 만료 여부 판단
            new_token_data['issued_at'] = int(time.time())
            save_token_to_parameter_store(new_token_data)
            return new_token_data['access_token']
        else:
            return None # 새 토큰 발급 실패
    else:
        logger.info("Using cached token from Parameter Store.")
        return token_info['access_token']

def check_and_update_market_status():
    """Polygon API를 호출하여 현재 시장 상태를 확인하고 Parameter Store를 업데이트합니다."""
    try:
        logger.info("Checking market status...")
        
        # Polygon API 호출
        url = f"https://api.polygon.io/v1/marketstatus/now?apiKey={POLYGON_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        # 'nasdaq' 키가 없을 경우를 대비하여 안전하게 접근
        nasdaq_status = data.get('exchanges', {}).get('nasdaq', 'unknown').lower()
        
        # 상태를 'OPEN' 또는 'CLOSED'로 결정
        market_status_to_update = 'OPEN' if nasdaq_status == 'open' else 'CLOSED'
        
        # 결정된 상태를 Parameter Store에 업데이트 ('팻말' 바꾸기)
        ssm_client.put_parameter(
            Name=MARKET_STATUS_PARAMETER_NAME,
            Value=market_status_to_update,
            Type='String',
            Overwrite=True
        )
        logger.info(f"Market status successfully updated to: {market_status_to_update}")

    except Exception as e:
        logger.exception(f"An error occurred during market status check: {e}")
        # 오류 발생 시, 안전을 위해 시장 상태를 'CLOSED'로 강제 업데이트
        logger.warning("Setting market status to CLOSED due to an error.")
        ssm_client.put_parameter(
            Name=MARKET_STATUS_PARAMETER_NAME,
            Value='CLOSED',
            Type='String',
            Overwrite=True
        )

def is_market_open():
    try:
        status_param = ssm_client.get_parameter(Name=MARKET_STATUS_PARAMETER_NAME)
        return status_param['Parameter']['Value'] == 'OPEN'
    except Exception as e:
        logger.exception(f"Could not determine market status: {e}")
        return False

def get_overseas_exponent_price(token, key, secret, base_url, exponent_code, price_date):
    """지수 현재 수치를 조회하는 함수"""
    path = "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice"
    url = f"{base_url}{path}"

    # 요청 헤더 설정
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": key,
        "appsecret": secret,
        "tr_id": "FHKST03030100", # 거래 ID: API마다 정해진 고유값
        "custtype" : "P"
    }   

    # 요청 파라미터 설정
    params = {
        "FID_COND_MRKT_DIV_CODE": 'N',
        "FID_INPUT_ISCD": exponent_code,
        "FID_INPUT_DATE_1" : price_date,
        "FID_INPUT_DATE_2" : price_date,
        "FID_PERIOD_DIV_CODE" : "D"
    }

    try:
        logger.info("Requesting stock price for %s", exponent_code)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        # API 응답이 text일 경우를 대비한 안정적인 파싱
        return json.loads(response.content.decode('utf-8'))
    except requests.exceptions.RequestException as e:
        logger.exception("API request failed for exponent %s: %s", exponent_code, e)
        return None
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON for exponent %s. Response text: %s", exponent_code, response.text)
        return None

def send_exponent_price_message_to_sqs(exponent_id, price_date, price_data):
    """실시간 지수 데이터를 SQS FIFO 큐로 전송하는 함수"""
    try:
        message_body = json.dumps({
            "exponentId": exponent_id,
            "priceDate": price_date,
            "priceData": price_data
        })
        
        # 중복 방지를 위한 ID 생성 (exponentId와 현재 시간을 조합)
        deduplication_id = f"{exponent_id}-{price_date}"

        sqs_client.send_message(
            QueueUrl=EXPONENT_SQS_QUEUE_URL,
            MessageBody=message_body,
            MessageGroupId=exponent_id,
            MessageDeduplicationId=deduplication_id
        )
        logger.info(f"Successfully sent message to SQS for ticker: {exponent_id}")
    except Exception as e:
        logger.exception(f"Failed to send message to SQS for ticker: {exponent_id}")
        raise e # 에러 발생 시 재시도를 위해 예외를 다시 발생

def send_exponent_prediction_message_to_sqs(price_date, prediction_date, exponents):
    """
    지수 예측 데이터를 공통 메시지 분배기를 통해 전송합니다.
    이 함수는 카나리 배포를 자동으로 처리합니다.
    """
    try:
        message_body = {
            'tickerId': wildcard_ticker_id,
            'type' : 'exponent',
            'payload' : {
                'exponents' : exponents,
                'priceDate' : price_date,
                'predictionDate' : prediction_date.isoformat()
            }
        }
        
        # 예측 날짜를 기반으로 생성하여 동일 날짜에 대한 중복 전송을 방지
        message_id = f"exponent-prediction-{prediction_date.isoformat()}"

        message_to_send = [{
            'id': message_id,
            'body': message_body
        }]

        send_prediction_messages(message_to_send)
        
        logger.info(f"Successfully distributed exponent prediction message for date: {prediction_date.date()}")

    except Exception as e:
        logger.error(f"Failed to distribute exponent prediction message for date: {prediction_date.date()}")
        raise e

def lambda_handler(event, context):
    logger.info("Lambda handler started.")
    
    try:
        market_time = datetime.now(timezone.utc)
        if market_time.minute % 30 == 0:
            check_and_update_market_status()
        
        if not is_market_open():
            logger.info("Market is CLOSED. Skipping stock price collection.")
            return {'statusCode': 200, 'body': 'Market is closed.'}
        
        logger.info("Market is OPEN. Fetching stock prices...")
        
        now_et = datetime.now(timezone.utc)
        price_date_str = now_et.strftime('%Y-%m-%d')
        hours_str = now_et.strftime('%H:%M:00')
        price_date = now_et.strftime('%Y-%m-%dT%H:%M:00Z')

        access_token = get_access_token(KIS_APP_KEY, KIS_APP_SECRET, KIS_BASE_URL)
        if not access_token:
            raise Exception("Failed to get a valid access token.")
        
        exponents = get_exponent_from_parameter_store() # 선 파라미터 조회
        if exponents is None:    
            exponents = get_exponents_from_supabase()
        logger.info(f"Successfully fetched {len(exponents)} exponents to process.")
        
        processed_count = 0
        exponents_to_prediction = []
        for item in exponents:
            exponent_id = item[0]
            exponent_code = item[1]

            price_data = get_overseas_exponent_price(access_token, KIS_APP_KEY, KIS_APP_SECRET, KIS_BASE_URL, 
                                                     exponent_code, price_date_str)
            
            if price_data and price_data.get('output1'):
                current_price = price_data['output1'].get('ovrs_nmix_prpr')
                
                # SQS로 보낼 데이터 구성
                payload = {
                    "price": round(float(current_price), 4), # 소수점 4자리까지만 허용
                    "hours": hours_str
                }
                
                # SQS 메시지 전송
                send_exponent_price_message_to_sqs(exponent_id, price_date_str, payload)
                exponents_to_prediction.append({
                    'exponentId' : exponent_id,
                    'code' : exponent_code,
                    'value' : round(float(current_price), 4)    
                })
                processed_count += 1
            else:
                logger.warning(f"Could not retrieve price data for {exponent_code}.")

        if processed_count == 0 and len(exponents) > 0:
            raise Exception(f"Processed 0 exponents out of {len(exponents)}.")
        
        logger.info(f"Successfully processed and sent {processed_count} price messages to SQS.")
        
        # 현재 분(minute)이 30으로 나누어 떨어질때 혹은 오차를 고려하여 나머지가 1일때, 예측 메시지를 전송(즉, 0/1분 또는 30/31분인지) + 첫번째 데이터가 아닐때에만
        if market_time.minute % 30 <= 1 and not is_first_data(market_time.strftime("%Y-%m-%d %H:%M:00"), price_date_str):
            logger.info("Start to send prediction message.")
            
            prediction_date = datetime.now(timezone.utc) \
                .replace(hour=0, minute=0, second=0, microsecond=0) # 데이터 간 날짜 통일을 위해 사용
            send_exponent_prediction_message_to_sqs(price_date, prediction_date, exponents_to_prediction)
            
            logger.info(f"Successfully processed and sent {processed_count} prediction messages to SQS.")
        else:
            logger.info(f"Skipping send prediction message. Current minute is {market_time.minute}, not a scheduled time (00 or 30).")
            
        return {
            'statusCode': 200,
            'body': f'Successfully processed {processed_count} exponents.'
        }
    except Exception as e:
        logger.exception("A critical error occurred in the lambda handler.")
        raise e