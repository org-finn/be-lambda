import os
import json
import logging
import uuid
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# --- 로거 설정 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- 환경 변수 및 클라이언트 전역 초기화 ---
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
MASSIVE_API_KEY = os.environ.get('MASSIVE_API_KEY')

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TARGET_TICKER_CODES = ['SPCX', 'MU', 'TSM', 'PLTR', 'ARM', 'CRWD']

def get_tickers_from_supabase():
    logger.info("Fetching target tickers from Supabase.")
    try:
        response = supabase.table('ticker') \
            .select('id, code') \
            .in_('code', TARGET_TICKER_CODES) \
            .execute()
        if response.data:
            return {item['code']: item['id'] for item in response.data}
    except Exception as e:
        logger.exception("Failed to fetch tickers from Supabase.")
    return {}

def get_latest_close_before(ticker_id, target_date):
    try:
        response = supabase.table('ticker_prices') \
            .select('close') \
            .eq('ticker_id', ticker_id) \
            .lt('price_date', target_date) \
            .order('price_date', desc=True) \
            .limit(1) \
            .execute()
        
        if response.data and len(response.data) > 0:
            return response.data[0]['close']
    except Exception as e:
        logger.exception(f"Failed to fetch previous close for ticker {ticker_id}")
    return None

def lambda_handler(event, context):
    logger.info("Massive API Price Fetcher Lambda started.")
    
    start_date = '2024-09-03'
    end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    tickers_map = get_tickers_from_supabase()
    if not tickers_map:
        logger.error("No valid target tickers found in Supabase.")
        return {'statusCode': 500, 'body': 'No tickers found.'}
        
    for ticker_code, ticker_id in tickers_map.items():
        logger.info(f"Processing {ticker_code}...")
        
        api_url = f"https://api.massive.com/v2/aggs/ticker/{ticker_code}/range/1/day/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'apikey': MASSIVE_API_KEY
        }
        
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = sorted(data.get('results', []), key=lambda x: x['t'])
            if not results:
                logger.info(f"No price data returned for {ticker_code}.")
                continue
                
            prev_close = get_latest_close_before(ticker_id, start_date)
            
            records_to_upsert = []
            for item in results:
                parsed_date = datetime.fromtimestamp(item['t'] / 1000.0).strftime('%Y-%m-%d')
                current_close = item['c']
                
                change_rate = 0.0
                if prev_close is not None and prev_close != 0:
                    change_rate = ((current_close - prev_close) / prev_close) * 100
                    
                records_to_upsert.append({
                    'id': str(uuid.uuid4()),
                    'price_date': parsed_date,
                    'open': item['o'],
                    'high': item['h'],
                    'low': item['l'],
                    'close': current_close,
                    'volume': int(item['v']), 
                    'ticker_code': ticker_code,
                    'ticker_id': ticker_id,
                    'change_rate': round(change_rate, 4),
                    'created_at': datetime.now(timezone.utc).isoformat()
                })
                
                prev_close = current_close
            
            if records_to_upsert:
                upsert_res = supabase.table('ticker_prices') \
                    .upsert(records_to_upsert, on_conflict='ticker_id, price_date') \
                    .execute()
                
                logger.info(f"✅ {ticker_code}: Upserted {len(records_to_upsert)} records.")
                
        except Exception as e:
            logger.exception(f"Error processing {ticker_code}: {e}")
            continue
            
    return {
        'statusCode': 200,
        'body': json.dumps('Data fetching and Supabase upsert completed successfully.')
    }