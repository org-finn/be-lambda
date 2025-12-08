import json
import os
import logging
import uuid
import psycopg2
from datetime import datetime, timedelta

# 1. 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수 (RDS 접속 정보)
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')
DB_PORT = os.environ.get('DB_PORT')
TABLE_NAME = "exchange_rate"

def get_db_connection():
    """RDS PostgreSQL 연결 생성"""
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT
    )

def parse_currency(value_str):
    """'1,400.50' 같은 문자열을 float으로 변환"""
    if not value_str:
        return 0.0
    # 이미 숫자형일 경우 대비
    if isinstance(value_str, (int, float)):
        return float(value_str)
    return float(value_str.replace(',', ''))

def calculate_change_rate(current, previous):
    """등락률 계산 ((현재-과거)/과거 * 100)"""
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100

def lambda_handler(event, context):
    conn = None
    try:
        # 필수 환경 변수 확인
        if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
             raise ValueError("DB connection environment variables are missing.")

        # KST 날짜 (시간은 00:00:00으로 고정하여 날짜 식별자로 사용)
        utc_now = datetime.utcnow()
        kst_now = utc_now + timedelta(hours=9)
        today_str = kst_now.strftime('%Y-%m-%dT00:00:00') # 예: 2024-12-03T00:00:00
        current_timestamp = kst_now.isoformat()
        
        # DB 연결
        conn = get_db_connection()
        cur = conn.cursor()

        # SQS 메시지 처리
        for record in event.get('Records', []):
            try:
                message_body = json.loads(record['body'])
                logger.info(f"Processing message: {message_body.get('cur_unit')}")

                # 필요한 데이터 추출
                index_code = message_body.get('indexCode', 'C01')
                # indexInfo가 있으면 우선 사용, 없으면 cur_nm(기존 '미국 달러') 사용
                index_info = message_body.get('indexInfo', message_body.get('cur_nm'))
                deal_bas_r = parse_currency(message_body.get('deal_bas_r'))
                
                # 1. 오늘 날짜의 해당 indexCode 데이터 조회
                select_query = f"SELECT id, value FROM {TABLE_NAME} WHERE date = %s AND index_code = %s"
                cur.execute(select_query, (today_str, index_code))
                existing_row = cur.fetchone()
                
                final_change_rate = 0.0

                if existing_row:
                    # [CASE 1] 오늘 데이터가 있으면 -> Update
                    # 요구사항: change_rate는 기존 value(DB에 있던 값)와의 등락률 계산
                    row_id, old_value_decimal = existing_row
                    old_value = float(old_value_decimal) # Decimal -> float 변환
                    
                    final_change_rate = calculate_change_rate(deal_bas_r, old_value)
                    
                    update_query = f"""
                        UPDATE {TABLE_NAME} 
                        SET value = %s, change_rate = %s, index_info = %s, updated_at = %s 
                        WHERE id = %s
                    """
                    cur.execute(update_query, (deal_bas_r, final_change_rate, index_info, current_timestamp, row_id))
                    
                    logger.info(f"Updated row {row_id}: Value {old_value} -> {deal_bas_r}, Rate {final_change_rate:.2f}%, Info {index_info}")
                    
                else:
                    # [CASE 2] 오늘 데이터가 없으면 -> Create (Insert)
                    # 요구사항: 어제(혹은 가장 최근) 데이터 조회하여 등락률 계산
                    prev_select_query = f"""
                        SELECT value FROM {TABLE_NAME} 
                        WHERE index_code = %s AND date < %s 
                        ORDER BY date DESC LIMIT 1
                    """
                    cur.execute(prev_select_query, (index_code, today_str))
                    prev_row = cur.fetchone()
                    
                    if prev_row:
                        prev_value = float(prev_row[0])
                        final_change_rate = calculate_change_rate(deal_bas_r, prev_value)
                    else:
                        # [CASE 3] 이전 데이터가 아무것도 없으면 0.0000
                        final_change_rate = 0.0
                    
                    new_id = str(uuid.uuid4())
                    insert_query = f"""
                        INSERT INTO {TABLE_NAME} (id, date, index_code, index_info, value, change_rate, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cur.execute(insert_query, (
                        new_id, 
                        today_str, 
                        index_code, 
                        index_info, 
                        deal_bas_r, 
                        final_change_rate, 
                        current_timestamp,
                        current_timestamp  # 생성 시점에는 updated_at도 동일하게 설정
                    ))
                    
                    logger.info(f"Inserted new row: {today_str}, Value {deal_bas_r}, Rate {final_change_rate:.2f}%, Info {index_info}")
                
                # 레코드별 커밋 (부분 성공 허용 시)
                conn.commit()

            except Exception as item_error:
                conn.rollback() # 에러 시 롤백
                logger.error(f"Error processing record {record.get('messageId')}: {item_error}", exc_info=True)
                # 개별 메시지 처리 실패 시 다음 메시지로 넘어갈지, 전체 실패시킬지 결정 필요
                continue

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Processed successfully'})
        }

    except Exception as e:
        logger.error(f"Critical error in handler: {str(e)}", exc_info=True)
        raise e
    finally:
        if conn:
            conn.close()