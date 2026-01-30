import json
import urllib3
import os
import boto3
from datetime import datetime, timedelta

# 환경 변수 로드
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')
LOG_GROUP_NAME = os.environ.get('LOG_GROUP_NAME') 

logs_client = boto3.client('logs')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    sns_raw_message = event['Records'][0]['Sns']['Message']
    sns_message = json.loads(sns_raw_message)
    
    alarm_name = sns_message.get('AlarmName')
    new_state = sns_message.get('NewStateValue')
    reason = sns_message.get('NewStateReason')

    log_content = "조회된 에러 로그가 없습니다."
    
    if new_state == 'ALARM':
        try:
            # 다음 줄([메시지])을 포함하기 위해 필터 패턴 없이 최근 로그를 가져옵니다.
            end_time = int(datetime.now().timestamp() * 1000)
            start_time = int((datetime.now() - timedelta(minutes=5)).timestamp() * 1000)

            response = logs_client.filter_log_events(
                logGroupName=LOG_GROUP_NAME,
                filterPattern="", # 다음 줄을 가져오기 위해 패턴을 비웁니다.
                startTime=start_time,
                endTime=end_time,
                limit=50 # 넉넉하게 가져와서 내부 필터링
            )

            events = response.get('events', [])
            error_pairs = []
            
            # 2. ERROR 라인과 그 바로 다음 라인을 추출하는 로직
            for i in range(len(events)):
                msg = events[i]['message']
                if "ERROR" in msg:
                    timestamp = datetime.fromtimestamp(events[i]['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
                    # ERROR 메인 라인 추가
                    entry = f"[{timestamp}] {msg}"
                    
                    # 바로 다음 라인이 존재하면 '[메시지]' 내용 추가
                    if i + 1 < len(events):
                        next_msg = events[i+1]['message']
                        # 성공 로그가 섞이지 않도록 다음 줄에 특정 키워드가 있는지 확인하거나 그냥 포함
                        entry += f"\n ↳ {next_msg}"
                    
                    error_pairs.append(entry)

            if error_pairs:
                # 가장 최근 발생한 에러 쌍 2~3개만 추출
                log_content = "\n\n".join(error_pairs[-2:]) 
            else:
                log_content = "ERROR 레벨의 로그와 상세 메시지를 찾을 수 없습니다."

        except Exception as e:
            log_content = f"로그 조회 중 오류 발생: {str(e)}"

    # 3. 슬랙 메시지 구성
    msg = {
        "text": f"🚨 *알람 발생: {alarm_name}*",
        "attachments": [{
            "color": "#eb4034" if new_state == "ALARM" else "#34eb46",
            "fields": [
                {"title": "Status", "value": new_state, "short": True},
                {"title": "Reason", "value": reason, "short": False},
                {"title": "Detailed Error Logs", "value": f"```{log_content}```", "short": False}
            ]
        }]
    }

    encoded_msg = json.dumps(msg).encode('utf-8')
    resp = http.request('POST', SLACK_WEBHOOK_URL, body=encoded_msg, headers={'Content-Type': 'application/json'})

    return {"status": resp.status}