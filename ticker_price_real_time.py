import json
import os
import boto3
import logging
from supabase import create_client, Client
from datetime import date, timedelta, datetime
import requests
import json
import time
import pytz

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')

# 티커 조회 용도로 사용
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

KIS_BASE_URL = os.environ.get('KIS_BASE_URL')
KIS_APP_KEY = os.environ.get('KIS_APP_KEY')
KIS_APP_SECRET = os.environ.get('KIS_APP_SECRET')

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

ssm_client = boto3.client('ssm')
KIS_TOKEN_PARAMETER_NAME = os.environ.get('KIS_TOKEN_PARAMETER_NAME')

MARKET_STATUS_PARAMETER_NAME = os.environ.get('MARKET_STATUS_PARAMETER_NAME')
MARKET_TIMEZONE = 'US/Eastern' # Nasdaq/NYSE 시장 기준 시간대


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


def get_overseas_stock_price(token, key, secret, base_url, symbol, exchange_code):
    """해외주식 현재가를 조회하는 함수"""
    path = "/uapi/overseas-price/v1/quotations/inquire-asking-price"
    url = f"{base_url}{path}"

    # 요청 헤더 설정
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": key,
        "appsecret": secret,
        "tr_id": "HHDFS76200100", # 거래 ID: API마다 정해진 고유값
        "custtype" : "P"
    }   

    # 요청 파라미터 설정
    params = {
        "AUTH": ' ', # 사용자 인증 토큰, 현재는 사용되지 않아 빈 값
        "EXCD": exchange_code, # 거래소 코드 (NASD: 나스닥, NYSE: 뉴욕 등)
        "SYMB": symbol # 종목 심볼
    }

    try:
        logger.info("Requesting stock price for %s (%s)", symbol, exchange_code)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        # API 응답이 text일 경우를 대비한 안정적인 파싱
        return json.loads(response.content.decode('utf-8'))
    except requests.exceptions.RequestException as e:
        logger.exception("API request failed for symbol %s: %s", symbol, e)
        return None
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON for symbol %s. Response text: %s", symbol, response.text)
        return None

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

def fetch_stock_data_if_market_open():
    """Parameter Store의 시장 상태를 확인하고, 열려있을 경우에만 주가 수집 로직을 실행합니다."""
    try:
        # 현재 '팻말' 상태를 Parameter Store에서 읽어오기
        status_param = ssm_client.get_parameter(Name=MARKET_STATUS_PARAMETER_NAME)
        current_market_status = status_param['Parameter']['Value']
        
        if current_market_status == 'OPEN':
            return True
        else:
            return False
        
    except Exception as e:
        logger.exception(f"An error occurred during stock price collection logic: {e}")


def save_data_in_s3(current_date, s3_payload):
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=f"{current_date}.json",
        Body=str(s3_payload)
    )

def lambda_handler(event, context):
    logger.info("Lambda handler started.")
    
    # 미국 동부 시간 기준 현재 시간이 30분 간격일 때만 시장 상태를 체크
    market_time = datetime.now(pytz.timezone(MARKET_TIMEZONE))
    if market_time.minute % 30 == 0:
        check_and_update_market_status()
    
    if not fetch_stock_data_if_market_open(): # 정규장이 열려있지 않으면 함수 즉시 종료
        logger.info("Market is CLOSED. Skipping stock price collection and close function.")
        return
    
    logger.info("Market is OPEN. Fetching stock prices...")
    
    current_date = datetime.now().isoformat()
    access_token = get_access_token(KIS_APP_KEY, KIS_APP_SECRET, KIS_BASE_URL)
    if not access_token:
        logger.error("Access token is not available. Aborting.")
        return
    logger.debug("Using access token starting with: %s", access_token[:15]) # 토큰 전체를 로그에 남기지 않도록 일부만 기록
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    response = supabase.table('ticker').select('code, id, exchange_code').execute()
    if response.data:
        tickers = [(item['code'], item['id'], item['exchange_code']) for item in response.data]
        logger.info("Successfully fetched %d tickers to process.", len(tickers))
    else:
        logger.warning("No ticker data to fetch.")
        return
    
    s3_payload = []
    processed_count = 0
    
    for ticker_code, ticker_id, exchange_code in tickers:

        # NYSE -> NYS, NASD -> NAS
        price_data = get_overseas_stock_price(access_token, KIS_APP_KEY, KIS_APP_SECRET, KIS_BASE_URL, ticker_code, exchange_code[:-1])
        
        if price_data and price_data.get('output1'):
            output = price_data['output1']
            current_price = output.get('last')
            logger.info("Processed %s: Current price is $%s", ticker_code, current_price)
            
            s3_payload.append({
                "tickerId" : ticker_id,
                "tickerCode" : ticker_code,
                "price" : current_price,
                "priceDate" : current_date
            })
            processed_count += 1
        else:
            logger.warning("Could not retrieve price data for %s.", ticker_code)

    logger.info("Successfully processed %d out of %d tickers.", processed_count, len(tickers))
    
    save_data_in_s3(current_date, s3_payload)

    logger.info("Lambda handler finished.")
    return {
        'statusCode': 200,
        'body': f'Successfully processed {processed_count} tickers.'
    }
