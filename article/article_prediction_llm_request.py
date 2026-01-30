import os
import json
import logging
import boto3
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from google import genai
from google.genai import types

# --- 설정 및 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
PREDICTION_SQS_QUEUE_URL = os.environ.get('PREDICTION_SQS_QUEUE_URL')

# 클라이언트 초기화
sqs_client = boto3.client('sqs')
ssm_client = boto3.client('ssm')
client = genai.Client(api_key=GEMINI_API_KEY)


def get_tickers_from_parameter_store():
    """Parameter Store에서 저장된 티커 목록을 가져옵니다."""
    logger.info("Fetching tickers from Parameter Store.")
    try:
        param = ssm_client.get_parameter(Name='/articker/tickers')
        cached_data = json.loads(param['Parameter']['Value'])
        all_tickers = cached_data.get('tickers', [])

        ticker_map = {}
        ticker_desc_list = []

        # ticker 구조: [ID, CODE, NAME] 가정
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

def call_gemini_model_with_clustering(articles, ticker_context_str):
    """
    Gemini에게 뉴스 목록을 전달하여 시맨틱 클러스터링을 수행합니다.
    유사한 뉴스는 하나의 그룹으로 묶고, 그룹의 감정과 포함된 기사 개수를 반환받습니다.
    """
    
    # 1. 입력 데이터 텍스트화
    articles_text = ""
    for idx, art in enumerate(articles, 1):
        headline = art.get('headline', 'N/A')
        symbol_hint = art.get('symbol', '')
        # 뉴스 ID나 인덱스를 주어 LLM이 식별하기 편하게 함
        articles_text += f"[{idx}] (SymbolHint: {symbol_hint}) {headline}\n"

    # 2. 응답 스키마 정의 (Semantic Grouping)
    response_schema = types.Schema(
        type=types.Type.ARRAY,
        items=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "ticker_code": types.Schema(
                    type=types.Type.STRING,
                    description="The stock ticker code from the provided target list."
                ),
                "sentiment": types.Schema(
                    type=types.Type.STRING,
                    enum=["POSITIVE", "NEGATIVE", "NEUTRAL"],
                    description="The representative sentiment of this news cluster."
                ),
                "summary": types.Schema(
                    type=types.Type.STRING,
                    description="A short summary of this news cluster."
                ),
                "article_count": types.Schema(
                    type=types.Type.INTEGER,
                    description="Number of headlines that belong to this semantic cluster."
                )
            },
            required=["ticker_code", "sentiment", "article_count"]
        )
    )

    # 3. 프롬프트 구성 (클러스터링 지시 포함)
    prompt = f"""
    You are an expert financial news aggregator.
    
    [Target Tickers]
    {ticker_context_str}

    [News Headlines]
    {articles_text}

    [Instructions]
    1. **Semantic Clustering**: Group the headlines that are talking about the **same event or topic** for the same company.
       - Example: "Apple releases iPhone 16" and "iPhone 16 launched by Apple" should be ONE cluster.
    2. **Ticker Identification**: For each cluster, identify the relevant Ticker Code from the [Target Tickers] list. If not in list, ignore.
    3. **Sentiment Analysis**: Determine the sentiment (POSITIVE, NEGATIVE, NEUTRAL) for that cluster.
    4. **Counting**: Count how many headlines belong to this cluster (`article_count`).
    
    Return the result as a JSON list of clusters.
    """

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash', 
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type='application/json',
                response_schema=response_schema,
                temperature=0.1 # 클러스터링은 논리적이여야 하므로 창의성(Temperature)을 낮춤
            )
        )
        return json.loads(response.text)

    except Exception as e:
        logger.error(f"Gemini API Error: {e}")
        return []

def send_batch_to_sqs(messages):
    """SQS 배치 전송"""
    if not messages:
        return
    
    chunk_size = 10
    for i in range(0, len(messages), chunk_size):
        chunk = messages[i:i + chunk_size]
        entries = []
        for msg in chunk:
            entries.append({
                'Id': str(uuid.uuid4()),
                'MessageBody': json.dumps(msg)
            })
        try:
            sqs_client.send_message_batch(
                QueueUrl=PREDICTION_SQS_QUEUE_URL,
                Entries=entries
            )
            logger.info(f"Sent {len(entries)} aggregated messages to SQS.")
        except Exception as e:
            logger.error(f"SQS Send Error: {e}")

def lambda_handler(event, context):
    """
    Main Handler
    1. SQS에서 뉴스 수신
    2. Gemini로 '시맨틱 클러스터링' 및 감정 분석
    3. 종목별 점수 집계 (Aggregation)
    4. 결과 전송
    """
    ticker_map, ticker_context_str = get_tickers_from_parameter_store()
    if not ticker_map:
        return {'statusCode': 500, 'body': 'Failed to load tickers.'}

    # 1. SQS 메시지 파싱 및 중복 제거 (단순 문자열 일치 제거)
    unique_articles = {}
    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            headline = body.get('headline')
            # 완전히 똑같은 문장의 기사는 LLM에 보낼 필요도 없이 여기서 1차 필터링
            if headline and headline not in unique_articles:
                unique_articles[headline] = body
        except json.JSONDecodeError:
            continue
    
    articles_list = list(unique_articles.values())
    
    if not articles_list:
        return {'statusCode': 200, 'body': 'No articles to process.'}

    # 2. Gemini 클러스터링 & 분석 호출
    logger.info(f"Clustering and Analyzing {len(articles_list)} unique headlines...")
    clustered_results = call_gemini_model_with_clustering(articles_list, ticker_context_str)
    
    # 3. 결과 집계 (Aggregation)
    # Gemini가 묶어준 Cluster 단위로 점수를 합산합니다.
    # 예: A클러스터(긍정, 3개) + B클러스터(긍정, 2개) -> AAPL 긍정 5개
    aggregation = defaultdict(lambda: {'positive': 0, 'negative': 0, 'neutral': 0})
    
    for cluster in clustered_results:
        code = cluster.get('ticker_code')
        sentiment = cluster.get('sentiment')
        count = cluster.get('article_count', 1) # LLM이 카운팅한 개수 반영
        
        if code in ticker_map and sentiment:
            if sentiment == 'POSITIVE':
                aggregation[code]['positive'] += count
            elif sentiment == 'NEGATIVE':
                aggregation[code]['negative'] += count
            else:
                aggregation[code]['neutral'] += count

    # 4. SQS 메시지 생성
    sqs_messages = []
    prediction_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    current_time_iso = datetime.now(timezone.utc).isoformat()

    for ticker_code, counts in aggregation.items():
        ticker_id = ticker_map.get(ticker_code)
        
        if not ticker_id:
            continue
            
        message_body = {
            'tickerId': ticker_id,
            'type': 'article',
            'payload': {
                'positiveArticleCount': counts['positive'],
                'negativeArticleCount': counts['negative'],
                'neutralArticleCount': counts['neutral'],
                'predictionDate': prediction_date,
                'createdAt': current_time_iso
            }
        }
        sqs_messages.append(message_body)

    if sqs_messages:
        send_batch_to_sqs(sqs_messages)
        return {
            'statusCode': 200, 
            'body': json.dumps(f"Success. Processed {len(articles_list)} headlines into {len(sqs_messages)} ticker updates.")
        }
    
    return {'statusCode': 200, 'body': 'Done. No relevant tickers found.'}