import os
import json
import logging
import boto3
import pytz
from datetime import date, datetime, timezone, timedelta
from supabase import create_client, Client
from polygon import RESTClient

# --- 초기 설정 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
PREDICTION_SQS_QUEUE_URL = os.environ.get('PREDICTION_SQS_QUEUE_URL')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# 클라이언트는 핸들러 함수 밖에 선언하여 재사용 (성능 최적화)
sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')
polygon_client = RESTClient(POLYGON_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_tickers_from_parameter_store():
    """Parameter Store에서 저장된 티커 목록을 가져옵니다."""
    logger.info("Fetching tickers from Parameter Store.")
    
    try:
        param = ssm_client.get_parameter(Name='/articker/tickers')
        
        # 저장된 JSON 문자열을 파싱
        cached_data = json.loads(param['Parameter']['Value'])
        all_tickers = cached_data.get('tickers', [])

        filtered_tickers = [
            (item[0], item[1], item[2]) for item in all_tickers
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
        response = supabase.table('ticker').select('id, code, short_company_name').execute()
        if response.data:
            tickers = [(item['id'], item['code'], item['short_company_name']) for item in response.data]
            logger.info("Found %d tickers to process.", len(tickers))
            return tickers
    except Exception as e:
        logger.exception("Failed to fetch tickers from Supabase.")
    return []

def get_macd(ticker_code):
    """특정 종목의 최근 2일치 MACD 데이터를 불러옵니다."""
    
    try:
        response = polygon_client.get_macd(
            ticker=ticker_code,
            timespan="day",
            limit=2
            
        )
        # 2. 데이터가 2개 미만일 경우 예외 처리 (신규 상장 주식 등)
        if not hasattr(response, 'values') or len(response.values) < 2:
            logger.warn(f"경고: {ticker_code}의 MACD 데이터가 2개 미만입니다.")
            return {}

        # 3. 최신 데이터(오늘)와 이전 데이터(어제)를 분리
        today_data = response.values[0]
        yesterday_data = response.values[1]

        # 4. 최종 메시지 바디에 필요한 형태로 딕셔너리 생성
        # Polygon의 'value' 키를 'macd' 키로 변경
        structured_data = {
            "todayMacd": {
                "macd": today_data.value,
                "signal": today_data.signal
            },
            "yesterdayMacd": {
                "macd": yesterday_data.value,
                "signal": yesterday_data.signal
            }
        }
        
        return structured_data
    except Exception as e:
        logger.error("Polygon macd API failed for ticker %s: %s", ticker_code, e)

def get_ma(ticker_code):
    """
    특정 티커의 5일, 20일 이동평균선 데이터를 불러옵니다
    """
    
    try:
        # 1. 5일 이동평균선(SMA) 데이터 요청 (최신 2개)
        sma5_response = polygon_client.get_sma(ticker=ticker_code, 
                                              timespan="day", 
                                              window=5, 
                                              limit=2)

        # 2. 20일 이동평균선(SMA) 데이터 요청 (최신 2개)
        sma20_response = polygon_client.get_sma(ticker=ticker_code, 
                                               timespan="day", 
                                               window=20, 
                                               limit=2)

        # 3. 데이터가 2개 미만일 경우 예외 처리
        if (not hasattr(sma5_response, 'values') or len(sma5_response.values) < 2 or
            not hasattr(sma20_response, 'values') or len(sma20_response.values) < 2):
            logger.warn(f"경고: {ticker_code}의 이동평균선 데이터가 2개 미만입니다.")
            return {}

        # 4. 각 이동평균선의 오늘/어제 값 추출
        today_sma5 = sma5_response.values[0].value
        yesterday_sma5 = sma5_response.values[1].value
        
        today_sma20 = sma20_response.values[0].value
        yesterday_sma20 = sma20_response.values[1].value

        # 5. 최종 메시지 바디에 필요한 형태로 딕셔너리 생성
        structured_data = {
            "todayMa": {
                "ma5": today_sma5,
                "ma20": today_sma20
            },
            "yesterdayMa": {
                "ma5": yesterday_sma5,
                "ma20": yesterday_sma20
            }
        }

        return structured_data
    except Exception as e:
        logger.error(f"오류 발생: {ticker_code} 이동평균선 데이터를 가져오는 중 실패 - {e}")
        return {}

def get_rsi(ticker_code):
    """
    특정 티커의 가장 최신 RSI 데이터를 가져옵니다.
    """
    try:
        # 1. RSI 데이터 요청 (최신 1개)
        response = polygon_client.get_rsi(ticker=ticker_code, 
                                          timespan="day", 
                                          limit=1)

        # 2. 데이터가 없는 경우 예외 처리
        if not hasattr(response, 'values') or len(response.values) < 1:
            logger.warn(f"경고: {ticker_code}의 RSI 데이터가 없습니다.")
            return {}

        # 3. 가장 최신 RSI 값 추출
        today_rsi = response.values[0].value

        # 4. 최종 메시지 바디에 필요한 형태로 딕셔너리 생성
        structured_data = {
            "todayRsi": round(float(today_rsi), 2) # 소수점 둘째 자리까지 반올림
        }
        
        return structured_data

    except Exception as e:
        logger.error(f"오류 발생: {ticker_code} RSI 데이터를 가져오는 중 실패 - {e}")
        return {}

def get_price_for_atr(ticker_code):
    """
    특정 티커의 오늘 고가, 오늘 저가, 전일 종가를 가져옵니다.
    (최근 2거래일의 T-1 고/저, T-2 종가)
    """
    try:
        # 1. 최근 2거래일 데이터 요청
        # (주말/휴장일 고려 넉넉히 7일 전부터 조회)
        to_date = date.today() - timedelta(days=1)
        from_date = to_date - timedelta(days=7)

        # 1. 제너레이터 반환
        aggs_generator = polygon_client.list_aggs(
            ticker=ticker_code,
            multiplier=1,
            timespan="day",
            from_=from_date.isoformat(),
            to=to_date.isoformat(),
            sort="desc", # 최신순 정렬
            limit=2      # 최근 2일치 데이터만 필요
        )

        # 2. 리스트로 변환
        aggs_list = list(aggs_generator)

        # 3. 리스트 길이 확인
        if not aggs_list or len(aggs_list) < 2:
            logger.warn(f"경고: {ticker_code}의 ATR 가격 데이터(2일치)가 2개 미만입니다.")
            return {}

        # 4. 값 추출 (라이브러리가 c, h, l을 close, high, low로 변환해 줌)
        today_high = aggs_list[0].high
        today_low = aggs_list[0].low
        yesterday_close = aggs_list[1].close

        # 4. 최종 딕셔너리 생성 (Kotlin DTO와 형식을 맞추기 위해 camelCase 사용)
        structured_data = {
            "todayHigh": today_high,
            "todayLow": today_low,
            "yesterdayClose": yesterday_close
        }
        
        return structured_data

    except Exception as e:
        logger.error(f"오류 발생: {ticker_code} ATR 가격 데이터를 가져오는 중 실패 - {e}")
        return {}
    
    

def lambda_handler(event, context):
    """
    UTC 자정에 실행되어, 새로운 날이 거래일일 경우
    해당 날짜의 초기 예측 데이터를 생성합니다.
    """
    logger.info("Lambda handler started: Initializing daily prediction data.")

    try:
        prediction_date = datetime.now(timezone.utc) \
            .replace(hour=0,minute=0, second=0, microsecond=0) # 00:00:00 고정
        
        tickers = get_tickers_from_parameter_store() # 선 파라미터 조회
        if tickers is None:
            tickers = get_tickers_from_supabase() # 안전장치로 db에서 조회
        ticker_info_map = { ticker[0]: {'code': ticker[1], 'name': ticker[2]} for ticker in tickers }

        prediction_to_send = []
        for ticker_id in ticker_info_map.keys():
            code = ticker_info_map.get(ticker_id, {}).get('code')
            short_company_name = ticker_info_map.get(ticker_id, {}).get('name')
            macd_data = get_macd(code)
            sma_data = get_ma(code)
            rsi_data = get_rsi(code)
            price_data = get_price_for_atr(code)
            
            message_body = {
                'tickerId': ticker_id,
                'type' : 'init',
                'payload' : {
                    'tickerCode' : code,
                    'shortCompanyName' : short_company_name,
                    'predictionDate': prediction_date.isoformat(),
                    'todayMacd' : macd_data.get('todayMacd'),
                    'yesterdayMacd' : macd_data.get('yesterdayMacd'),
                    'todayMa' : sma_data.get('todayMa'),
                    'yesterdayMa' : sma_data.get('yesterdayMa'),
                    'todayRsi' : rsi_data.get('todayRsi'),
                    'todayHigh': price_data.get('todayHigh'),
                    'todayLow': price_data.get('todayLow'),
                    'yesterdayClose' : price_data.get('yesterdayClose'),
                    'createdAt': datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
                }
            }
            prediction_to_send.append({
                'Id': code.replace('.', '-'),
                'MessageBody': json.dumps(message_body)
            })
        
        # Queue로 전송
        for i in range(0, len(prediction_to_send), 10):
            batch = prediction_to_send[i:i+10]
            sqs_client.send_message_batch(QueueUrl=PREDICTION_SQS_QUEUE_URL, Entries=batch)
        logger.info("✅ Successfully sent all prediction init row stats to SQS.")
    except Exception as e:
        logger.exception("A critical error occurred in the lambda handler.")
        raise e

    return {
        'statusCode': 200,
        'body': f'Successfully processed and sent predictions to SQS.'
    }
    