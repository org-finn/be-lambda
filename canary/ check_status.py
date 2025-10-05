import boto3
import os

# Boto3 클라이언트 초기화
autoscaling = boto3.client('autoscaling')
elbv2 = boto3.client('elbv2')

# Lambda 함수의 환경 변수에서 리소스 정보 가져오기
PROD_ASG_NAME = os.environ['PROD_ASG_NAME']
CANARY_TG_ARN = os.environ['CANARY_TG_ARN']

def lambda_handler(event, context):
    check_type = event.get('check_type')
    
    print(f"Executing status check for: {check_type}")

    # --- 1. 카나리 인스턴스의 Health Check 상태를 확인하는 동작 ---
    if check_type == 'CANARY_HEALTH':
        try:
            # Canary 대상 그룹에 등록된 타겟들의 상태를 조회합니다.
            response = elbv2.describe_target_health(TargetGroupArn=CANARY_TG_ARN)
            
            # 등록된 타겟 중 하나라도 'healthy' 상태이면 성공으로 간주합니다.
            for target in response.get('TargetHealthDescriptions', []):
                if target.get('TargetHealth', {}).get('State') == 'healthy':
                    print("Found at least one healthy canary instance.")
                    return {"status": "SUCCEEDED"}
            
            # healthy 인스턴스가 하나도 없으면 계속 진행 중으로 간주합니다.
            print("No healthy canary instances found yet. Still in progress.")
            return {"status": "IN_PROGRESS"}

        except Exception as e:
            print(f"Error checking canary health: {e}")
            raise e

    # --- 2. 프로덕션 ASG의 '인스턴스 새로 고침' 완료 여부를 확인하는 동작 ---
    elif check_type == 'INSTANCE_REFRESH':
        try:
            # Production ASG에서 진행 중인 인스턴스 새로고침 기록을 조회합니다.
            response = autoscaling.describe_instance_refreshes(AutoScalingGroupName=PROD_ASG_NAME)
            
            if not response.get('InstanceRefreshes'):
                print("No instance refresh activities found.")
                return {"status": "IN_PROGRESS"}

            # 가장 최근의 새로고침 상태를 확인합니다.
            latest_refresh = response['InstanceRefreshes'][0]
            status = latest_refresh.get('Status')
            print(f"Latest instance refresh status: {status}")
            
            if status == 'Successful':
                return {"status": "SUCCEEDED"}
            elif status in ['Failed', 'Cancelling', 'Cancelled']:
                # 실패 또는 취소 시, Step Functions 워크플로우를 중단시키기 위해 에러를 발생시킵니다.
                raise Exception(f"Instance refresh failed with status: {status}")
            else:
                # 'InProgress', 'Pending' 등의 상태일 경우 계속 진행 중으로 간주합니다.
                return {"status": "IN_PROGRESS"}

        except Exception as e:
            print(f"Error checking instance refresh status: {e}")
            raise e
            
    else:
        raise ValueError("Invalid check_type specified")