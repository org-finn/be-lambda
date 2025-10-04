import boto3
import os

LISTENER_ARN = os.environ['LISTENER_ARN']
LIVE_TG_ARN = os.environ['LIVE_TG_ARN']
CANARY_TG_ARN = os.environ['CANARY_TG_ARN']
STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']
elbv2 = boto3.client('elbv2')
sfn = boto3.client('stepfunctions')

def lambda_handler(event, context):
    print("ALARM DETECTED! Initiating emergency rollback.")
    # 1. 즉시 트래픽을 100% 운영 서버(Live)로 되돌림
    elbv2.modify_listener(
        ListenerArn=LISTENER_ARN,
        DefaultActions=[{
            'Type': 'forward', 'ForwardConfig': { 'TargetGroups': [
                {'TargetGroupArn': LIVE_TG_ARN, 'Weight': 100},
                {'TargetGroupArn': CANARY_TG_ARN, 'Weight': 0}
            ]}
        }]
    )
    print("Traffic successfully reverted to 100% live.")
    # 2. 현재 실행 중인 Step Functions 워크플로우를 찾아 강제 중지
    executions = sfn.list_executions(stateMachineArn=STATE_MACHINE_ARN, statusFilter='RUNNING')
    if executions['executions']:
        execution_arn = executions['executions'][0]['executionArn']
        sfn.stop_execution(executionArn=execution_arn, error='RollbackTriggered', cause='CloudWatch alarm was triggered.')
        print(f"Successfully stopped Step Functions execution: {execution_arn}")