import os
import json
import logging
import uuid
import boto3
from google import genai
from google.genai import types

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
SAVE_QUEUE_URL = os.environ.get('SAVE_QUEUE_URL')
MAX_KEYWORDS = int(os.environ.get('MAX_KEYWORDS', '10'))

client = genai.Client(api_key=GEMINI_API_KEY)
sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')


def get_tickers_from_parameter_store():
    logger.info("Fetching tickers from Parameter Store.")
    try:
        response = ssm_client.get_parameters(
            Names=['/articker/tickers', '/articker/tickers/2']
        )
        
        all_tickers = []
        
        # 성공적으로 조회된 파라미터들을 순회하며 하나의 리스트로 병합합니다.
        for param in response.get('Parameters', []):
            try:
                cached_data = json.loads(param['Value'])
                all_tickers.extend(cached_data.get('tickers', []))
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON for parameter: {param.get('Name')}")
                
        # 만약 생성되지 않았거나 오타가 있는 파라미터가 있다면 경고 로그를 남깁니다.
        invalid_params = response.get('InvalidParameters', [])
        if invalid_params:
            logger.warning(f"Invalid or missing parameters: {invalid_params}")
            
        all_tickers = cached_data.get('tickers', [])

        ticker_map = {}
        ticker_desc_list = []

        for item in all_tickers:
            t_id = item[0]
            t_code = item[1]
            t_name = item[2] if len(item) > 2 else t_code
            
            ticker_map[t_code] = t_id
            ticker_desc_list.append(f"{t_code}({t_name})")
            
        return ticker_map, ", ".join(ticker_desc_list)
    except Exception as e:
        logger.exception("Failed to get tickers.")
        return {}, ""


def call_gemini_for_keywords(articles, ticker_context_str):
    if not articles:
        return []

    articles_text = ""
    for art in articles:
        articles_text += f"[articleId: {art.get('id')}]\nTitle: {art.get('title', 'N/A')}\nDescription: {art.get('description', 'N/A')}\n\n"

    # 스키마는 기존 유지 (LLM은 ID 리스트만 반환하도록 하여 오류/토큰 절감)
    response_schema = types.Schema(
        type=types.Type.OBJECT,
        properties={
            "results": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "keyword": types.Schema(type=types.Type.STRING),
                        "articles": types.Schema(type=types.Type.ARRAY, items=types.Schema(type=types.Type.STRING)),
                        "sentiment": types.Schema(type=types.Type.INTEGER),
                        "ticker_code": types.Schema(
                            type=types.Type.STRING,
                            description="The single most relevant stock ticker code from the Target Tickers list for this keyword. If not applicable, return null."
                        )
                    },
                    required=["keyword", "articles", "sentiment"]
                )
            )
        },
        required=["results"]
    )

    prompt = f"""
        금일의 뉴스들을 목록으로 제공합니다. 각 뉴스들은 articleId라는 식별자를 가집니다.
        뉴스들로부터 전체 흐름을 관통하는 핵심 키워드를 최대 {MAX_KEYWORDS}개 추출하고, 해당 키워드와 연관된 뉴스들의 식별자(articleId)를 연결시켜주세요.
        
        [Target Tickers]
        {ticker_context_str}

        [Instructions]
        1. 키워드(keyword) 추출 규칙:
        - ❌ 절대 기업명(예: Apple, Tesla)이나 티커(예: AAPL, TSLA) 자체를 키워드로 사용하지 마세요.
        - ⭕ 뉴스의 핵심 사건인 '이벤트', '제품/기술명', '재무 상태', '거시경제 트렌드' 등을 짧고 핵심을 드러내는 형태의 키워드로 도출하세요. (예: "반도체", "금리 인상", "고유가", "전쟁")
        - 키워드는 가급적 자연스러운 한글로 표현하되, 전문 고유명사나 한글 번역이 어색한 경우에만 영어를 사용하세요.
        
        2. 종목 매핑 (ticker_code):
        - 추출한 핵심 키워드가 위의 [Target Tickers] 목록에 명시된 특정 종목과 뚜렷한 연관성이 있다면, 해당 종목의 Ticker Code를 `ticker_code` 필드에 기입해 주세요.
        - 특정 종목과 연관이 없거나(예: 거시경제 뉴스) 매핑하기 모호하다면 `ticker_code` 값에 null을 반환하세요.
        
        3. 감정 정보 (sentiment):
        - 각 키워드가 해당 종목이나 시장에 미치는 영향을 평가하여 1(긍정), 0(중립), -1(부정)로 추가해 주세요.
        
        [News List]
        {articles_text}
    """

    try:
        response = client.models.generate_content(
            model='gemini-3.1-flash-lite', 
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type='application/json',
                response_schema=response_schema,
                temperature=0.2
            )
        )
        return json.loads(response.text).get('results', [])
    except Exception as e:
        logger.error(f"Gemini API Error: {e}")
        return []


def lambda_handler(event, context):
    logger.info("Lambda 2: Processing SQS trigger for LLM Request & Ticker Mapping...")
    
    ticker_map, ticker_context_str = get_tickers_from_parameter_store()
    
    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            articles = body.get('articles', [])
            
            if not articles:
                continue
            
            # ⭐️ 원본 뉴스 데이터에서 articleId를 Key, title을 Value로 가지는 매핑 딕셔너리 생성
            article_title_map = {str(art.get('id')): art.get('title', '제목 없음') for art in articles}
                
            keyword_results = call_gemini_for_keywords(articles, ticker_context_str)
            
            entries_to_send = []
            for item in keyword_results:
                llm_ticker_code = item.get('ticker_code')
                ticker_id = ticker_map.get(llm_ticker_code) if llm_ticker_code else None
                
                # ⭐️ LLM이 추출한 ID 목록을 순회하며 JSON 구조의 리스트로 조립
                llm_extracted_ids = item.get('articles', [])
                json_articles_list = [
                    {
                        "articleId": a_id,
                        "title": article_title_map.get(str(a_id), "제목 없음")
                    } for a_id in llm_extracted_ids
                ]
                
                save_payload = {
                    "keyword": item.get('keyword'),
                    "articles": json_articles_list, # 리스트 형태의 JSON 오브젝트 전달
                    "sentiment": item.get('sentiment', 0),
                    "tickerId": ticker_id
                }
                
                entries_to_send.append({
                    'Id': str(uuid.uuid4()),
                    'MessageBody': json.dumps(save_payload)
                })
                
            for i in range(0, len(entries_to_send), 10):
                batch = entries_to_send[i:i+10]
                sqs_client.send_message_batch(QueueUrl=SAVE_QUEUE_URL, Entries=batch)
                
            logger.info(f"Successfully mapped tickers and queued {len(entries_to_send)} keyword messages.")
            
        except Exception as e:
            logger.error(f"Lambda 2 Error handling record: {e}")
            raise e
            
    return {'statusCode': 200, 'body': 'LLM and Ticker Mapping complete.'}