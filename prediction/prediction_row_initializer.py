import os
import json
import logging
import psycopg2
import pytz
import exchange_calendars as xcals
from datetime import datetime, timedelta

# --- 초기 설정 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수에서 DB 연결 정보 가져오기
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT', 5432)

# 미국 증시 캘린더 및 타임존 초기화
US_CALENDAR = xcals.get_calendar("XNYS")
UTC_TZ = pytz.utc

def lambda_handler(event, context):
    """
    UTC 자정에 실행되어, 새로운 날이 거래일일 경우
    해당 날짜의 초기 예측 데이터를 생성합니다.
    """
    logger.info("Lambda handler started: Initializing daily prediction data.")

    try:
        # 1. UTC 기준 '오늘' 날짜 계산
        # EventBridge 스케줄러가 00:00 UTC에 정확히 실행하므로, now()를 사용
        today_utc = datetime.now(UTC_TZ)
        today_utc_str = today_utc.strftime('%Y-%m-%d')

        # 2. 새로운 날이 거래일인지 확인
        if not US_CALENDAR.is_session(today_utc_str):
            logger.warning(f"Skipping initialization. {today_utc_str} is a market holiday.")
            return {
                'statusCode': 200,
                'body': json.dumps(f'Skipped: {today_utc_str} is a holiday.')
            }

        logger.info(f"{today_utc_str} is a trading day. Proceeding with data initialization.")

        # 3. 데이터베이스 연결
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        
        # 4. 초기값 삽입 쿼리 실행
        # 해당 날짜에 데이터가 없는 ticker에 대해서만 초기값을 생성
        sql_query = """
            INSERT INTO predictions (
                id, prediction_date, positive_article_count, negative_article_count, 
                neutral_article_count, sentiment, strategy, score, 
                ticker_code, short_company_name, ticker_id, created_at
            )
            SELECT
                gen_random_uuid(), %s::TIMESTAMPTZ, 0, 0, 0, 0, '관망', 50,
                t.code, t.short_company_name, t.id, NOW() AT TIME ZONE 'Asia/Seoul'
            FROM
                ticker t
            WHERE NOT EXISTS (
                SELECT 1
                FROM predictions p
                WHERE p.ticker_id = t.id
                  AND p.prediction_date >= %s::date
                  AND p.prediction_date < (%s::date + INTERVAL '1 day')
            );
        """
        
        # 쿼리 파라미터로 오늘 날짜를 전달
        params = (today_utc_str, today_utc_str, today_utc_str)
        cur.execute(sql_query, params)
        
        # 삽입된 행의 수 확인
        inserted_rows = cur.rowcount
        conn.commit()
        
        logger.info(f"Successfully inserted initial data for {inserted_rows} tickers for the date {today_utc_str}.")

    except Exception as e:
        logger.exception("A critical error occurred during the lambda execution.")
        # DB 연결이 열려있으면 롤백
        if 'conn' in locals() and conn:
            conn.rollback()
        raise e
        
    finally:
        # 리소스 정리
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully initialized data for {inserted_rows} tickers.')
    }