import boto3
import os
import json

LISTENER_ARN = os.environ['LISTENER_ARN']
LIVE_TG_ARN = os.environ['LIVE_TG_ARN']
CANARY_TG_ARN = os.environ['CANARY_TG_ARN']
elbv2 = boto3.client('elbv2')

def lambda_handler(event, context):
    canary_percentage = event.get('canary_percentage', 0)
    live_percentage = 100 - canary_percentage
    print(f"Adjusting traffic -> Live: {live_percentage}%, Canary: {canary_percentage}%")

    elbv2.modify_listener(
        ListenerArn=LISTENER_ARN,
        DefaultActions=[{
            'Type': 'forward',
            'ForwardConfig': {
                'TargetGroups': [
                    {'TargetGroupArn': LIVE_TG_ARN, 'Weight': live_percentage},
                    {'TargetGroupArn': CANARY_TG_ARN, 'Weight': canary_percentage}
                ]
            }
        }]
    )
    # 다음 단계를 위해 현재 퍼센티지를 반환합니다.
    return { 'canary_percentage': canary_percentage }