import os
import json
import logging
import boto3
from google import genai
from google.genai import types
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

# --- 설정 및 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
ARTICLE_LLM_RESULT_SQS_QUEUE_URL = os.environ.get('ARTICLE_LLM_RESULT_SQS_QUEUE_URL')

# 클라이언트 초기화
sqs_client = boto3.client('sqs')
client = genai.Client(api_key=GEMINI_API_KEY)

def call_gemini_model(articles):
    """
    Gemini API를 호출하여 뉴스를 분석하고 구조화된 JSON 데이터를 반환합니다.
    """
    # 1. 분석할 뉴스 데이터 텍스트 변환
    articles_text = ""
    for idx, art in enumerate(articles, 1):
        title = art.get('title', 'N/A')
        desc = art.get('description', 'N/A')
        articles_text += f"[{idx}] 제목: {title}\n    설명: {desc}\n\n"

    # 2. 프롬프트 구성
    prompt = f"""
    이전의 모든 지침들은 잊어버리세요.
    다음은 시장 종합 최신 뉴스 데이터입니다.
    
    [뉴스 데이터]
    {articles_text}

    [지시사항]
    1. 위 뉴스들을 종합적으로 분석하여 긍정적인 요인과 부정적인 요인을 각각 파악하세요. (한글로 작성)
    2. 'positiveReasoning': 긍정적인 요인들을 최대 3개 추출하세요. 각 문장은 엔터 단위로 구분해서 해당 필드에 모두 담아주세요. (해당 내용이 없으면 null 반환)
    3. 'negativeReasoning': 부정적인 요인들을 최대 3개 추출하세요. 각 문장은 엔터 단위로 구분해서 해당 필드에 모두 담아주세요. (해당 내용이 없으면 null 반환)
    4. 뉴스에서 핵심 키워드를 추출하고, 각 키워드의 성격(긍정, 부정)에 따라 분류하세요. 키워드의 길이는 한글/영문 모두 5자로 제한해주고, 키워드 개수는 각 성격 별로 최대 5개로 제한해주세요. (해당 내용이 없으면 null 반환)
    """

    # 3. 응답 스키마 정의 (JSON 강제)
    # 사용자가 요청한 필드 구조에 맞춰 스키마를 정의합니다.
    response_schema = {
        "type": "OBJECT",
        "properties": {
            "positiveReasoning": {"type": "STRING", "nullable": True, "description": "긍정 요인 요약"},
            "negativeReasoning": {"type": "STRING", "nullable": True, "description": "부정 요인 요약"},
            "positiveKeywords": {
                "type": "ARRAY", "items": {"type": "STRING"}, "nullable": True, "description": "긍정적 키워드 리스트"
            },
            "negativeKeywords": {
                "type": "ARRAY", "items": {"type": "STRING"}, "nullable": True, "description": "부정적 키워드 리스트"
            }
        },
        "required": ["positiveReasoning", "negativeReasoning", "positiveKeywords", "negativeKeywords"]
    }

    try:
        # 4. Gemini API 호출
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=response_schema
            )
        )
        
        # 5. 결과 파싱 (JSON 문자열 -> Dict)
        # SDK가 자동으로 객체로 변환해주지 않는 경우 text를 파싱
        if hasattr(response, 'parsed') and response.parsed:
             return response.parsed
        else:
             return json.loads(response.text)

    except Exception as e:
        logger.error(f"Gemini API Call Failed: {e}")
        raise e


def lambda_handler(event, context):
    logger.info("Starting LLM processing handler...")
    
    success_count = 0

    # SQS 메시지 처리 (Trigger로 실행되므로 Records 리스트 순회)
    for record in event.get('Records', []):
        try:
            # SQS 메시지 파싱
            body = json.loads(record['body'])
            articles = body.get('articles', [])
            request_id = body.get('requestId')
            
            # --- Gemini API 호출 ---
            ai_result = call_gemini_model(articles)
            
            # --- 결과 데이터 가공 ---
            if ai_result.get('positiveKeywords'):
                pos_keywords_str = ", ".join(ai_result.get('positiveKeywords'))
            else:
                pos_keywords_str = None
            
            if ai_result.get('negativeKeywords'):
                neg_keywords_str = ", ".join(ai_result.get('negativeKeywords'))
            else:
                neg_keywords_str = None
                
            # 다음 SQS로 보낼 최종 메시지 구성
            # (ticker_id는 요청하신 대로 제외했습니다)
            result_message = {
                "requestId": request_id,
                "positiveReasoning": ai_result.get('positiveReasoning', ''),
                "negativeReasoning": ai_result.get('negativeReasoning', ''),
                "positiveKeywords": pos_keywords_str,
                "negativeKeywords": neg_keywords_str,
                "summaryDate": datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()
            }

            # --- [수정됨] 단건 메시지 전송 (send_message) ---
            sqs_client.send_message(
                QueueUrl=ARTICLE_LLM_RESULT_SQS_QUEUE_URL,
                MessageBody=json.dumps(result_message, ensure_ascii=False)
            )
            
            success_count += 1
            logger.info(f"Successfully processed and sent result for request {request_id}")

        except Exception as e:
            logger.error(f"Error processing record {record.get('messageId')}: {e}")
            # 개별 실패 시 로그만 남기고 계속 진행 (또는 필요시 raise e)
            continue

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {success_count} messages.")
    }