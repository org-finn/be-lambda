import boto3
import os
import json

# Boto3 클라이언트 초기화
ec2 = boto3.client('ec2')
autoscaling = boto3.client('autoscaling')

LAUNCH_TEMPLATE_ID = os.environ.get('LAUNCH_TEMPLATE_ID')
PROD_ASG_NAME = os.environ.get('PROD_ASG_NAME')
CANARY_ASG_NAME = os.environ.get('CANARY_ASG_NAME')

def lambda_handler(event, context):
    action = event.get('action')
    
    print(f"Executing action: {action}")
    
    # --- 1. 카나리 인스턴스를 시작하는 동작 ---
    if action == 'START_CANARY':
        lt_version = event.get('launchTemplateVersion')
        image_tag = event.get('imageTag')

        if not lt_version:
            raise ValueError("launchTemplateVersion is required for START_CANARY action.")
        
        print(f"Updating Canary ASG ({CANARY_ASG_NAME}) to use Launch Template Version: {lt_version} and set DesiredCapacity=1")
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=CANARY_ASG_NAME,
            DesiredCapacity=1,
            LaunchTemplate={
                'LaunchTemplateId': LAUNCH_TEMPLATE_ID,
                'Version': str(lt_version)
            }
        )
        return {"status": "CANARY_STARTING"}
        
    # --- 2. 프로덕션 환경을 새 버전으로 업데이트하는 동작 ---
    elif action == 'PROMOTE_TO_PRODUCTION':
        lt_version = event.get('launchTemplateVersion')

        print(f"Updating Production ASG ({PROD_ASG_NAME}) to Launch Template Version: {lt_version}")
        # Production ASG가 새로운 버전의 시작 템플릿을 사용하도록 업데이트합니다.
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=PROD_ASG_NAME,
            LaunchTemplate={
                'LaunchTemplateId': LAUNCH_TEMPLATE_ID,
                'Version': lt_version
            }
        )
        
        print(f"Starting Instance Refresh for Production ASG ({PROD_ASG_NAME})")
        # 인스턴스 새로고침을 시작하여, 구버전 인스턴스를 새 버전으로 교체합니다.
        autoscaling.start_instance_refresh(
            AutoScalingGroupName=PROD_ASG_NAME,
            Strategy='Rolling'
        )
        return {"status": "INSTANCE_REFRESH_STARTED"}

    # --- 3. 카나리 인스턴스를 정리하는 동작 ---
    elif action == 'CLEANUP_CANARY':
        print(f"Updating Canary ASG ({CANARY_ASG_NAME}) to DesiredCapacity=0")
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=CANARY_ASG_NAME,
            DesiredCapacity=0
        )
        return {"status": "CANARY_CLEANED_UP"}
        
    else:
        raise ValueError("Invalid action specified")