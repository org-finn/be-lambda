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

def call_gemini_model(company_name, articles):
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
    다음은 '{company_name}' 기업에 대한 최신 뉴스 데이터입니다.
    
    [뉴스 데이터]
    {articles_text}

    [지시사항]
    1. 위 뉴스들을 종합적으로 분석하여 해당 기업의 주가나 평판에 긍정적인 요인과 부정적인 요인을 각각 파악하세요. (한글로 작성)
    2. 'positiveReasoning': 긍정적인 요인들을 최대 3개 추출하세요. 각 문장은 엔터 단위로 구분해서 해당 필드에 모두 담아주세요. (없으면 '특이사항 없음'으로 작성)
    3. 'negativeReasoning': 부정적인 요인들을 최대 3개 추출하세요. 각 문장은 엔터 단위로 구분해서 해당 필드에 모두 담아주세요. (없으면 '특이사항 없음'으로 작성)
    4. 뉴스에서 핵심 키워드를 추출하고, 각 키워드의 성격(긍정, 부정, 중립)에 따라 분류하세요. 키워드의 길이는 한글/영문 모두 5자로 제한해주고, 키워드 개수는 각 성격 별로 최대 5개로 제한해주세요.
    """

    # 3. 응답 스키마 정의 (JSON 강제)
    # 사용자가 요청한 필드 구조에 맞춰 스키마를 정의합니다.
    response_schema = {
        "type": "OBJECT",
        "properties": {
            "positiveReasoning": {"type": "STRING", "description": "긍정 요인 요약 텍스트"},
            "negativeReasoning": {"type": "STRING", "description": "부정 요인 요약 텍스트"},
            "positiveKeywords": {
                "type": "ARRAY", "items": {"type": "STRING"}, "description": "긍정적 영향을 주는 키워드 리스트"
            },
            "negativeKeywords": {
                "type": "ARRAY", "items": {"type": "STRING"}, "description": "부정적 영향을 주는 키워드 리스트"
            },
            "neutralKeywords": {
                "type": "ARRAY", "items": {"type": "STRING"}, "description": "중립적이거나 일반적인 키워드 리스트"
            }
        },
        "required": ["positiveReasoning", "negativeReasoning", "positiveKeywords", "negativeKeywords", "neutralKeywords"]
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
    
    processed_messages = []
    entries_to_send = []

    # SQS 배치 처리 (여러 메시지가 한 번에 올 수 있음)
    for record in event.get('Records', []):
        try:
            # SQS 메시지 파싱
            body = json.loads(record['body'])
            
            ticker_id = body.get('tickerId')
            short_company_name = body.get('shortCompanyName')
            articles = body.get('articles', [])
            request_id = body.get('requestId')
            sqs_msg_id = record['messageId']

            logger.info(f"Processing request for {short_company_name} (ID: {ticker_id})")

            if not articles:
                logger.warning(f"No articles for {short_company_name}. Skipping.")
                continue

            # --- Gemini API 호출 ---
            ai_result = call_gemini_model(short_company_name, articles)
            
            # --- 결과 데이터 가공 ---
            # 키워드 리스트를 콤마 구분 문자열로 변환
            pos_keywords_str = ", ".join(ai_result.get('positiveKeywords', []))
            neg_keywords_str = ", ".join(ai_result.get('negativeKeywords', []))
            neu_keywords_str = ", ".join(ai_result.get('neutralKeywords', []))

            # 다음 SQS로 보낼 최종 메시지 구성
            result_message = {
                "tickerId": ticker_id,
                "shortCompanyName": short_company_name,
                "requestId": request_id,
                # 분석 결과 필드
                "positiveReasoning": ai_result.get('positiveReasoning', ''),
                "negativeReasoning": ai_result.get('negativeReasoning', ''),
                "positiveKeywords": pos_keywords_str,
                "negativeKeywords": neg_keywords_str,
                "neutralKeywords": neu_keywords_str,
                "summaryDate": datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            }

            entries_to_send.append({
                'Id': sqs_msg_id,
                'MessageBody': json.dumps(result_message, ensure_ascii=False)
            })
            
            processed_messages.append(sqs_msg_id)

        except Exception as e:
            logger.error(f"Error processing record {record.get('messageId')}: {e}")
            # 개별 메시지 실패 시, 여기서 예외를 던지지 않고 로그만 남기면
            # 해당 메시지는 '성공' 처리되어 큐에서 삭제됩니다.
            # 재시도를 원하면 raise e를 하거나 BatchItemFailures를 반환해야 합니다.
            continue

    # --- 결과 SQS 전송 ---
    if entries_to_send:
        try:
            # 10개 단위 배치 전송
            for i in range(0, len(entries_to_send), 10):
                batch = entries_to_send[i:i+10]
                sqs_client.send_message_batch(
                    QueueUrl=ARTICLE_LLM_RESULT_SQS_QUEUE_URL,
                    Entries=batch
                )
            logger.info(f"Successfully sent {len(entries_to_send)} analyzed results to next queue.")
        except ClientError as e:
            logger.error(f"Failed to send messages to result queue: {e}")
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {len(processed_messages)} messages.")
    }