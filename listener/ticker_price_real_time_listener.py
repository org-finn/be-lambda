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
table = dynamodb.Table('ticker_price_real_time') # DynamoDB 테이블 이름
US_CALENDAR = xcals.get_calendar("XNYS")
US_EASTERN_TZ = pytz.timezone("America/New_York")

def get_market_info(target_date_str: str) -> dict | None:
    """미국 동부 시간(ET) 기준으로 시장 정보를 계산합니다."""
    if not US_CALENDAR.is_session(target_date_str):
        logger.warning(f"Market is closed on {target_date_str}. No data will be processed.")
        return None

    schedule = US_CALENDAR.schedule.loc[target_date_str]
    market_open_et = schedule.market_open.astimezone(US_EASTERN_TZ)
    market_close_et = schedule.market_close.astimezone(US_EASTERN_TZ)
    
    duration_minutes = (market_close_et - market_open_et).total_seconds() / 60
    max_len = int(duration_minutes / 5)

    return {"open_time_et": market_open_et, "max_len": max_len}


def lambda_handler(event, context):
    """SQS 메시지를 받아 DynamoDB에 주가 데이터를 저장하는 메인 핸들러"""
    logger.info(f"Lambda invocation started. Processing {len(event.get('Records', []))} SQS records.")
    
    for record in event.get('Records', []):
        try:
            # 1. SQS 메시지 파싱
            message_body = json.loads(record['body'])
            ticker_id = message_body['tickerId']
            price_date_str = message_body['priceDate']
            new_price_data = message_body['priceData']
            
            log_context = {"tickerId": ticker_id, "priceDate": price_date_str, "hours": new_price_data.get('hours')}
            logger.info("Processing message.", extra=log_context)

            # 2. 시장 정보 조회
            market_info = get_market_info(price_date_str)
            if not market_info:
                continue

            market_open_et = market_info['open_time_et']
            max_len = market_info['max_len']

            # 3. 고정 인덱스 계산
            hours_str = new_price_data['hours'].split('(')[0]
            data_time_et = US_EASTERN_TZ.localize(datetime.strptime(f"{price_date_str} {hours_str}", "%Y-%m-%d %H:%M:%S"))
            time_diff_minutes = (data_time_et - market_open_et).total_seconds() / 60
            fixed_index = int(time_diff_minutes / 5)

            new_record = {
                'index': fixed_index,
                'price': Decimal(str(new_price_data['price'])),
                'hours': new_price_data['hours']
            }
            log_context['calculated_index'] = fixed_index

            # 4. 인덱스 값에 따라 로직 분기
            if fixed_index == 0:
                # 데이터 날짜를 기준으로 7일 뒤의 Unix 타임스탬프 계산
                start_of_day_et = US_EASTERN_TZ.localize(datetime.strptime(price_date_str, "%Y-%m-%d"))
                expiry_time = start_of_day_et + timedelta(days=7)
                ttl_timestamp = int(expiry_time.timestamp())
                
                item_to_create = {
                    'tickerId': ticker_id,
                    'priceDate': price_date_str,
                    'maxLen': max_len,
                    'priceDataList': [new_record],
                    'ttl': ttl_timestamp
                }
                table.put_item(Item=item_to_create)
                logger.info("Successfully CREATED new item for the day.", extra=log_context)

            else:
                table.update_item(
                    Key={'tickerId': ticker_id, 'priceDate': price_date_str},
                    UpdateExpression="SET priceDataList = list_append(priceDataList, :newData)",
                    ExpressionAttributeValues={':newData': [new_record]}
                )
                logger.info("Successfully APPENDED data to existing item.", extra=log_context)

        except Exception:
            # 에러 발생 시, 예외 정보와 실패한 메시지 내용을 함께 기록
            logger.exception("Error processing SQS record.", extra={"failed_record_body": record.get('body')})
            continue
            
    return {'statusCode': 200, 'body': json.dumps('Processing complete')}