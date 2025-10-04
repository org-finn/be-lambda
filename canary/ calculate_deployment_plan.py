import math

# 기본 배포 시간 (입력값이 없을 경우)
DEFAULT_DURATION_HOURS = 9

def lambda_handler(event, context):
    # Step Functions 시작 시 입력받은 총 배포 시간
    total_duration = event.get('total_duration_hours', DEFAULT_DURATION_HOURS)

    if total_duration <= 0:
        raise ValueError("Total duration must be a positive number of hours.")

    # 시간당 트래픽 증가분 계산 (소수점 올림 처리)
    # 예: 100 / 6 = 16.66... -> 17
    hourly_increment = math.ceil(100 / total_duration)

    print(f"Total duration: {total_duration} hours. Calculated hourly increment: {hourly_increment}%")

    # 워크플로우의 다음 단계에서 사용할 정보들을 반환
    return {
        "hourly_increment": hourly_increment,
        "canary_percentage": 0,
        "total_duration_hours": total_duration,
        "current_hour": 0,
        "deploymentId": event.get('deploymentId') # 원본 deploymentId를 계속 전달
    }