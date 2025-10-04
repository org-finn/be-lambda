import json

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
import boto3

codedeploy = boto3.client('codedeploy')

def lambda_handler(event, context):
    deployment_id = event['detail']['deploymentId'] # EventBridge에서 전달된 원본 이벤트 정보 사용
    if not deployment_id:
        raise ValueError("Deployment ID is missing.")
    print(f"Finalizing CodeDeploy deployment: {deployment_id}")
    codedeploy.continue_deployment(
        deploymentId=deployment_id,
        deploymentWaitType='READY_WAIT'
    )
    return f"Deployment {deployment_id} finalization process initiated."