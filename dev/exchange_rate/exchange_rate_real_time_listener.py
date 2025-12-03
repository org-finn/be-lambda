import json
import os
import logging
import uuid
from datetime import datetime, timedelta
from supabase import create_client, Client

# 1. 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
TABLE_NAME = "exchange_rate"

# 클라이언트 초기화 (Lambda 컨테이너 재사용 시 연결 유지)
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Environment variables (SUPABASE_URL, SUPABASE_KEY) are missing.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
    try:
        # KST 날짜 (시간은 00:00:00으로 고정하여 날짜 식별자로 사용)
        utc_now = datetime.utcnow()
        kst_now = utc_now + timedelta(hours=9)
        today_str = kst_now.strftime('%Y-%m-%dT00:00:00') # 예: 2024-12-03T00:00:00
        
        # SQS 메시지 처리
        for record in event.get('Records', []):
            try:
                message_body = json.loads(record['body'])
                logger.info(f"Processing message: {message_body.get('cur_unit')}")

                # 필요한 데이터 추출
                index_code = message_body.get('indexCode', 'C01')
                index_info = message_body.get('indexInfo', message_body.get('cur_nm'))
                deal_bas_r = parse_currency(message_body.get('deal_bas_r'))
                
                # 1. 오늘 날짜의 해당 indexCode 데이터 조회
                response = supabase.table(TABLE_NAME)\
                    .select("*")\
                    .eq("date", today_str)\
                    .eq("index_code", index_code)\
                    .execute()
                
                existing_rows = response.data
                final_change_rate = 0.0

                if existing_rows and len(existing_rows) > 0:
                    # [CASE 1] 오늘 데이터가 있으면 -> Update
                    # 요구사항: change_rate는 기존 value(DB에 있던 값)와의 등락률 계산
                    row = existing_rows[0]
                    row_id = row['id']
                    old_value = float(row['value'])
                    
                    final_change_rate = calculate_change_rate(deal_bas_r, old_value)
                    
                    update_data = {
                        "value": deal_bas_r,
                        "change_rate": final_change_rate,
                        "index_info": index_info,
                        "updated_at" : kst_now.isoformat()
                    }
                    
                    supabase.table(TABLE_NAME)\
                        .update(update_data)\
                        .eq("id", row_id)\
                        .execute()
                        
                    logger.info(f"Updated row {row_id}: Value {old_value} -> {deal_bas_r}, Rate {final_change_rate:.2f}%")
                    
                else:
                    # [CASE 2] 오늘 데이터가 없으면 -> Create (Insert)
                    # 요구사항: 어제(혹은 가장 최근) 데이터 조회하여 등락률 계산
                    prev_response = supabase.table(TABLE_NAME)\
                        .select("value")\
                        .eq("index_code", index_code)\
                        .lt("date", today_str)\
                        .order("date", desc=True)\
                        .limit(1)\
                        .execute()
                    
                    prev_rows = prev_response.data
                    
                    if prev_rows and len(prev_rows) > 0:
                        prev_value = float(prev_rows[0]['value'])
                        final_change_rate = calculate_change_rate(deal_bas_r, prev_value)
                    else:
                        # [CASE 3] 이전 데이터가 아무것도 없으면 0.0000
                        final_change_rate = 0.0
                    
                    insert_data = {
                        "id": str(uuid.uuid4()), # ID 생성
                        "date": today_str,
                        "index_code": index_code,
                        "index_info": index_info,
                        "value": deal_bas_r,
                        "change_rate": final_change_rate,
                        "created_at": kst_now.isoformat(),
                        "updated_at": kst_now.isoformat()
                    }
                    
                    supabase.table(TABLE_NAME).insert(insert_data).execute()
                    logger.info(f"Inserted new row: {today_str}, Value {deal_bas_r}, Rate {final_change_rate:.2f}%")

            except Exception as item_error:
                logger.error(f"Error processing record {record.get('messageId')}: {item_error}", exc_info=True)
                # 개별 메시지 처리 실패 시 다음 메시지로 넘어갈지, 전체 실패시킬지 결정 필요
                # 여기서는 로깅 후 계속 진행(DLQ 설정 권장)
                continue

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Processed successfully'})
        }

    except Exception as e:
        logger.error(f"Critical error in handler: {str(e)}", exc_info=True)
        raise e