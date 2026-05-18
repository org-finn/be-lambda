import os
import json
import logging
import uuid
import boto3
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client
from google import genai
from google.genai import types

# --- 설정 및 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수 (Supabase & Gemini)
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

# 키워드 추출 최대 개수 (기본값 10)
MAX_KEYWORDS = int(os.environ.get('MAX_KEYWORDS', '10'))

# 클라이언트 초기화
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client = genai.Client(api_key=GEMINI_API_KEY)
ssm_client = boto3.client('ssm')  # Parameter Store 조회를 위한 AWS 클라이언트 추가


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


def fetch_recent_articles():
    """Supabase를 사용하여 article 테이블에서 최근 1일(24시간) 뉴스 조회"""
    yesterday_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    
    try:
        response = supabase.table('article') \
            .select("id, title, description") \
            .gte('published_date', yesterday_iso) \
            .order('published_date', desc=True) \
            .limit(50) \
            .execute()
            
        return response.data
    except Exception as e:
        logger.error(f"Failed to fetch articles from Supabase: {e}")
        raise e


def call_gemini_for_keywords(articles, ticker_context_str):
    """Gemini API를 호출하여 뉴스 목록에서 키워드, 관련 기사 ID, 감정, 종목 티커를 추출"""
    if not articles:
        return []

    articles_text = ""
    for art in articles:
        art_id = art.get('id')
        title = art.get('title') or "N/A"
        desc = art.get('description') or "N/A"
        articles_text += f"[articleId: {art_id}]\nTitle: {title}\nDescription: {desc}\n\n"

    # 응답 스키마 정의 (ticker_code 필드 추가)
    response_schema = types.Schema(
        type=types.Type.OBJECT,
        properties={
            "results": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "keyword": types.Schema(type=types.Type.STRING, description="Extracted core keyword"),
                        "articles": types.Schema(type=types.Type.ARRAY, items=types.Schema(type=types.Type.STRING), description="List of associated articleIds (UUIDs)"),
                        "sentiment": types.Schema(type=types.Type.INTEGER, description="Sentiment score: 1 (Positive), 0 (Neutral), -1 (Negative)"),
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

    # 프롬프트 구성 (Target Tickers 주입 및 매핑 지시 추가)
    prompt = f"""
    금일의 뉴스들을 목록으로 제공합니다. 각 뉴스들은 articleId라는 식별자를 가집니다.
    뉴스들로부터 핵심 키워드를 최대 {MAX_KEYWORDS}개 추출하고, 해당 키워드와 연관된 뉴스들의 식별자(articleId)를 연결시켜주세요.
    키워드들은 가급적 한글로 표현하되, 한글로 표현이 어려운 경우는 영어로 표현해주세요. 
    그리고 각 키워드들의 감정 정보를 추가해주세요. (1: 긍정, 0: 중립, -1: 부정)
    
    [Target Tickers]
    {ticker_context_str}

    [Instructions]
    - 추출한 핵심 키워드가 위의 [Target Tickers] 목록에 명시된 특정 종목과 뚜렷한 연관성이 있다면, 해당 종목의 Ticker Code(예: NVDA, AAPL)를 `ticker_code` 필드에 정확히 기입해 주세요.
    - 어떤 종목과도 연관이 없거나 매핑하기 모호하다면 `ticker_code` 값에 null을 반환하세요.
    
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
        parsed_data = json.loads(response.text)
        return parsed_data.get('results', [])
        
    except Exception as e:
        logger.error(f"Gemini API Error: {e}")
        return []


def lambda_handler(event, context):
    logger.info("Starting Article Keyword Extraction handler using Supabase...")
    
    # 1. Parameter Store에서 티커 맵 정보 동적 로드
    ticker_map, ticker_context_str = get_tickers_from_parameter_store()
    
    try:
        # 2. 최근 뉴스 DB 조회 (Supabase select)
        logger.info("Fetching recent articles from DB...")
        articles = fetch_recent_articles()
        
        if not articles:
            logger.info("No recent articles found.")
            return {'statusCode': 200, 'body': 'No articles to process.'}
            
        logger.info(f"Fetched {len(articles)} articles. Calling Gemini API...")

        # 3. LLM 호출하여 결과 및 티커 코드 매핑 도출
        keyword_results = call_gemini_for_keywords(articles, ticker_context_str)
        
        if not keyword_results:
            logger.info("No keywords extracted from LLM.")
            return {'statusCode': 200, 'body': 'No keywords generated.'}

        # 4. DB 삽입을 위한 데이터 딕셔너리 매핑
        records_to_insert = []
        current_time = datetime.now(timezone.utc).isoformat()
        utc_hourly_date = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()
        
        for item in keyword_results:
            article_ids_list = item.get('articles')
            article_ids_str = ",".join(article_ids_list) if article_ids_list else None
            
            # LLM이 전달한 ticker_code를 기반으로 Parameter Store 맵에서 고유 ID 추출
            llm_ticker_code = item.get('ticker_code')
            ticker_id = ticker_map.get(llm_ticker_code) if llm_ticker_code else None
            
            row = {
                "id": str(uuid.uuid4()),
                "keyword": item.get('keyword'),
                "articles": article_ids_str,
                "sentiment": item.get('sentiment', 0),
                "date": utc_hourly_date,
                "created_at": current_time,
                "ticker_id": ticker_id  # 매핑된 ticker_id 반영 (없으면 null 처리)
            }
            records_to_insert.append(row)

        # 5. DB에 결과값 저장 (Supabase insert)
        if records_to_insert:
            logger.info(f"Inserting {len(records_to_insert)} keyword records into DB...")
            supabase.table('articles_with_keyword').insert(records_to_insert).execute()
            logger.info("Successfully inserted keywords into Supabase.")

        return {
            'statusCode': 200,
            'body': json.dumps(f"Success. Extracted and saved {len(records_to_insert)} keywords.")
        }

    except Exception as e:
        logger.error(f"Lambda Handler Error: {e}")
        raise e