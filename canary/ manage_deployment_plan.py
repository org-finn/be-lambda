import math
DEFAULT_DURATION = 9

def lambda_handler(event, context):
    duration = event.get('total_duration_hours', DEFAULT_DURATION)
    increment = math.ceil(100 / duration) if duration > 0 else 100
    
    return {
        "plan": {
            "hourly_increment": increment,
            "total_duration": duration
        },
        "progress": {
            "current_percentage": 0,
            "current_hour": 0
        },
        "deploymentId": event.get('detail', {}).get('deploymentId'),
        "imageTag": event.get('imageTag')
    }