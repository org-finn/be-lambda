import boto3
import os
import json
import base64

# Boto3 클라이언트 초기화
ec2 = boto3.client('ec2')
autoscaling = boto3.client('autoscaling')

LIVE_LAUNCH_TEMPLATE_ID = os.environ['LIVE_LAUNCH_TEMPLATE_ID']
CANARY_LAUNCH_TEMPLATE_ID = os.environ['CANARY_LAUNCH_TEMPLATE_ID']
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
                'LaunchTemplateId': CANARY_LAUNCH_TEMPLATE_ID,
                'Version': str(lt_version)
            }
        )
        return {"status": "CANARY_STARTING"}
        
    # --- 2. 프로덕션 환경을 새 버전으로 업데이트하는 동작 ---
    elif action == 'PROMOTE_TO_PRODUCTION':
        image_tag = event.get('imageTag')

        # --- ⭐️ Live Template 업데이트 로직 ⭐️ ---
        # 1. live-template의 최신 User Data를 가져옵니다.
        latest_version = ec2.describe_launch_template_versions(LaunchTemplateId=LIVE_LAUNCH_TEMPLATE_ID, Versions=['$Latest'])
        base64_user_data = latest_version['LaunchTemplateVersions'][0]['LaunchTemplateData']['UserData']
        decoded_user_data = base64.b64decode(base64_user_data).decode('utf-8')

        # 2. 플레이스홀더를 새 이미지 태그로 교체합니다.
        modified_user_data = decoded_user_data.replace("DOCKER_IMAGE_TAG_PLACEHOLDER", image_tag)
        new_base64_user_data = base64.b64encode(modified_user_data.encode('utf-8')).decode('utf-8')

        # 3. 수정된 User Data로 live-template의 새 버전을 생성합니다.
        new_version_response = ec2.create_launch_template_version(
            LaunchTemplateId=LIVE_LAUNCH_TEMPLATE_ID,
            SourceVersion='$Latest',
            VersionDescription=f"Image tag {image_tag}",
            LaunchTemplateData={'UserData': new_base64_user_data}
        )
        new_version_number = new_version_response['LaunchTemplateVersion']['VersionNumber']

        # 4. 방금 만든 새 버전을 live-template의 기본값으로 설정합니다.
        ec2.modify_launch_template(
            LaunchTemplateId=LIVE_LAUNCH_TEMPLATE_ID,
            DefaultVersion=str(new_version_number)
        )
        # ------------------------------------
        
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