import boto3
import os

elbv2 = boto3.client('elbv2')
SQS_RULE_ARN = os.environ['SQS_RULE_ARN']
PROD_TG_ARN = os.environ['PROD_TG_ARN']
CANARY_TG_ARN = os.environ['CANARY_TG_ARN']

def lambda_handler(event, context):
    target = event.get('target')
    if target is None and 'Payload' in event:
        target = event.get('Payload', {}).get('target')

    if target == 'canary':
        # SQS 트래픽을 100% Canary-TG로 보냄
        prod_weight = 0
        canary_weight = 1
    elif target == 'production':
        # SQS 트래픽을 100% Production-TG로 되돌림
        prod_weight = 1
        canary_weight = 0
    else:
        raise ValueError("Target must be 'canary' or 'production'")

    print(f"Modifying SQS rule to -> Production: {prod_weight*100}%, Canary: {canary_weight*100}%")

    elbv2.modify_rule(
        RuleArn=SQS_RULE_ARN,
        Actions=[{
            'Type': 'forward',
            # ⭐️ 'ForwardConfig'와 'TargetGroups'를 사용하여 가중치를 명시합니다.
            'ForwardConfig': {
                'TargetGroups': [
                    {'TargetGroupArn': PROD_TG_ARN, 'Weight': prod_weight},
                    {'TargetGroupArn': CANARY_TG_ARN, 'Weight': canary_weight}
                ]
            }
        }]
    )
    return {"status": f"Rule modified to point to {target}"}