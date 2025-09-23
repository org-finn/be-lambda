import boto3
import json
import pytz
import exchange_calendars as xcals
from datetime import datetime, timedelta
from decimal import Decimal
import logging
import os


# --- 초기 설정 (핸들러 함수 외부에서 실행되어 재사용) ---
logger = logging.getLogger()
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logger.setLevel(log_level)

# DynamoDB 및 기타 리소스 초기화
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('exponent_real_time') # DynamoDB 테이블 이름
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



def lambda_handler(event, context):
    """SQS 메시지를 받아 DynamoDB에 지수 데이터를 저장하는 메인 핸들러"""
    logger.info(f"Lambda invocation started. Processing {len(event.get('Records', []))} SQS records.")
    
    for record in event.get('Records', []):
        try:
            # 1. SQS 메시지 파싱
            message_body = json.loads(record['body'])
            exponent_id = message_body['exponentId']
            price_date_str = message_body['priceDate']
            new_price_data = message_body['priceData']
            
            log_context = {"exponentId": exponent_id, "priceDate": price_date_str, "hours": new_price_data.get('hours')}
            logger.info("Processing message.", extra=log_context)

            # 2. 시장 정보 조회
            market_info = get_market_info(price_date_str)
            if not market_info:
                continue

            market_open_et = market_info['open_time_et']
            max_len = market_info['max_len']

            # 3. 고정 인덱스 계산 (ET 기준)
            # 1. 받은 시간 문자열(UTC 기준)으로 순수(naive) datetime 객체 생성
            hours_str = new_price_data['hours'] # (EST)가 없으므로 split 불필요
            naive_dt_utc = datetime.strptime(f"{price_date_str} {hours_str}", "%Y-%m-%d %H:%M:%S")
            
            # 2. 이 객체가 UTC 시간임을 명확하게 지정
            data_time_utc = UTC_TZ.localize(naive_dt_utc)
            
            # 3. index 계산을 위해 개장 시간도 UTC로 변환
            market_open_utc = market_open_et.astimezone(UTC_TZ)
            
            # 4. UTC 시간끼리 비교하여 정확한 인덱스 계산
            data_time_utc = data_time_utc.replace(second=0, microsecond=0)
            market_open_utc = market_open_utc.replace(second=0, microsecond=0)

            time_diff_minutes = (data_time_utc - market_open_utc).total_seconds() / 60
            fixed_index = int(time_diff_minutes / 5)

            new_record = {
                'index': fixed_index,
                'price': Decimal(str(new_price_data['price'])),
                'hours': new_price_data['hours']
            }
            log_context['calculated_index'] = fixed_index

            # --- ✅ 로직 통합: 하나의 UpdateItem으로 생성과 업데이트 모두 처리 ---
            
            # TTL 값은 최초 생성 시에만 설정되도록 if_not_exists 사용
            start_of_day_et = US_EASTERN_TZ.localize(datetime.strptime(price_date_str, "%Y-%m-%d"))
            expiry_time = start_of_day_et + timedelta(days=7)
            ttl_timestamp = int(expiry_time.timestamp())

            # UpdateExpression 정의
            update_expression = (
                "SET priceDataList = list_append(if_not_exists(priceDataList, :empty_list), :newData), "
                "#ML = if_not_exists(#ML, :maxLenVal), "
                "#T = if_not_exists(#T, :ttlVal)"
            )

            # ExpressionAttributeValues 정의
            expression_values = {
                ':newData': [new_record],
                ':empty_list': [],
                ':maxLenVal': max_len,
                ':ttlVal': ttl_timestamp
            }

            table.update_item(
                Key={'exponentId': exponent_id, 'priceDate': price_date_str},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ExpressionAttributeNames={
                    '#ML': 'maxLen',
                    '#T': 'ttl'
                }
            )
            
            logger.info("Successfully CREATED or APPENDED data.", extra=log_context)

        except Exception:
            # 에러 발생 시, 예외 정보와 실패한 메시지 내용을 함께 기록
            logger.exception("Error processing SQS record.", extra={"failed_record_body": record.get('body')})
            continue
            
    return {'statusCode': 200, 'body': json.dumps('Processing complete')}