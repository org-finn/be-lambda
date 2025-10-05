import math

def lambda_handler(event, context):
    # Step Functions로부터 '현재 퍼센티지'와 '증가분'을 입력받습니다.
    current_percentage = event.get('current_percentage', 0)
    increment = event.get('increment', 0)
    
    # 다음 퍼센티지를 계산합니다.
    next_percentage = current_percentage + increment
    
    # 계산 결과가 100을 초과하지 않도록 보정합니다. (예: 95 + 17 = 112 -> 100)
    if next_percentage > 100:
        next_percentage = 100
        
    print(f"Current: {current_percentage}%, Increment: {increment}%, Next: {next_percentage}%")
    
    # 계산 결과만 JSON 형태로 반환합니다.
    return {
        "next_percentage": next_percentage
    }