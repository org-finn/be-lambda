import boto3
import os

# Boto3 클라이언트 초기화
elbv2 = boto3.client('elbv2')

# Lambda 함수의 환경 변수에서 리소스 ARN 가져오기
LISTENER_ARN = os.environ['LISTENER_ARN']
PROD_TG_ARN = os.environ['PROD_TG_ARN']
CANARY_TG_ARN = os.environ['CANARY_TG_ARN']

def lambda_handler(event, context):
    # Step Functions로부터 카나리로 보낼 트래픽 비율을 받습니다.
    canary_percentage = event.get('canary_percentage', 0)
    
    # 0~100 사이의 값인지 확인
    if not 0 <= canary_percentage <= 100:
        raise ValueError("canary_percentage must be between 0 and 100.")
        
    # 프로덕션으로 보낼 트래픽 비율을 계산합니다.
    prod_percentage = 100 - canary_percentage
    
    print(f"Adjusting traffic -> Production: {prod_percentage}%, Canary: {canary_percentage}%")
    
    try:
        # ALB 리스너의 기본 규칙을 수정하여 트래픽 가중치를 변경합니다.
        elbv2.modify_listener(
            ListenerArn=LISTENER_ARN,
            DefaultActions=[{
                'Type': 'forward',
                'ForwardConfig': {
                    'TargetGroups': [
                        {'TargetGroupArn': PROD_TG_ARN, 'Weight': prod_percentage},
                        {'TargetGroupArn': CANARY_TG_ARN, 'Weight': canary_percentage}
                    ]
                }
            }]
        )
        
        print("Successfully modified listener traffic weights.")
        
        # 다음 단계를 위해 현재 카나리 퍼센티지를 반환합니다.
        return {
            'canary_percentage': canary_percentage
        }
        
    except Exception as e:
        print(f"Error modifying listener: {e}")
        raise e