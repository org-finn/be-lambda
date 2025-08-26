import json
import os
import boto3
import logging
from supabase import create_client, Client
from datetime import date, timedelta, datetime
import requests
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

KO_BASE_URL = os.environ.get('KO_BASE_URL')
KO_APP_KEY = os.environ.get('KO_APP_KEY')
KO_APP_SECRET = os.environ.get('KO_APP_SECRET')

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

def get_access_token(key, secret, base_url):
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
        
        token_data = response.json()
        access_token = token_data.get("access_token")
        
        if not access_token:
            logger.error("Failed to retrieve access_token from response: %s", token_data)
            return None
        
        logger.info("✅ Successfully issued a new access token.")
        return access_token
    except requests.exceptions.RequestException as e:
        # logger.exception은 오류의 스택 트레이스(traceback)를 함께 기록해 줍니다.
        logger.exception("An exception occurred during token issuance: %s", e)
        return None

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
    
def lambda_handler(event, context):
    logger.info("Lambda handler started.")
    
    access_token = get_access_token(KO_APP_KEY, KO_APP_SECRET, KO_BASE_URL)
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
    current_date = datetime.now().isoformat()
    
    for ticker_code, ticker_id, exchange_code in tickers:

        # NYSE -> NYS, NASD -> NAS
        price_data = get_overseas_stock_price(access_token, KO_APP_KEY, KO_APP_SECRET, KO_BASE_URL, ticker_code, exchange_code[:-1])
        
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
    
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=f"{current_date}.json",
        Body=str(s3_payload)
    )

    logger.info("Lambda handler finished.")
    return {
        'statusCode': 200,
        'body': f'Successfully processed {processed_count} tickers.'
    }
