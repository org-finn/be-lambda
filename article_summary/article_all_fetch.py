import os
import json
import logging
import boto3
import uuid
from polygon import RESTClient
from datetime import datetime, timedelta, timezone

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
LLM_REQUEST_SQS_QUEUE_URL = os.environ.get('LLM_REQUEST_SQS_QUEUE_URL')
sqs_client = boto3.client('sqs')
polygon_client = RESTClient(POLYGON_API_KEY)


def fetch_articles(published_utc_gte, limit):
    """Polygon API를 호출하여 뉴스를 가져옵니다."""
    try:
        
        response = polygon_client.list_ticker_news(
            limit=limit,
            order='asc',
            published_utc_gte=published_utc_gte
        )
        if response:
            return list(response)
    except Exception as e:
        logger.error("Polygon news API failed", e)
    return []


def lambda_handler(event, context):
    """여러 종목 정보가 포함된 뉴스를 각 티커별로 그룹화하여 SQS로 전송합니다."""
    logger.info("Article collection Lambda handler started.")
    
    try:
        # 1. 최근 24시간 기준 시간 계산 (UTC)
        now = datetime.now(timezone.utc)
        time_24h_ago = now - timedelta(hours=6)
        published_utc_gte = time_24h_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"Will fetch article >= {published_utc_gte} (UTC)")    
        
        # 3. 전체 최신 뉴스 수집(최대 20개로 제한)
        logger.info("Fetching general articles...")
        articles_raw = fetch_articles(published_utc_gte, 20)

        if not articles_raw:
            logger.info("No articles found in the last 6 hours.")
            return {'statusCode': 200, 'body': 'No articles found.'}

        # 2. 전체 기사 리스트 구성 (필요한 필드만 추출)
        all_articles = []
        for article in articles_raw:
            title = article.title
            description = article.description
            
            # 제목이나 설명이 없는 기사는 제외
            if title and description:
                all_articles.append({
                    'title': title,
                    'description': description
                })

        logger.info(f"Processed {len(all_articles)} valid articles for summary.")

        if not all_articles:
            return {'statusCode': 200, 'body': 'No valid articles found.'}

        # 3. 메시지 본문 생성 (단일 메시지)
        message_body = {
            'type': 'global_market_summary', # 메시지 타입 구분
            'articles': all_articles,        # 전체 기사 리스트 (Max 20)
            'articleCount': len(all_articles),
            'requestId': str(uuid.uuid4())
        }

        # 4. SQS로 단일 메시지 전송
        try:
            sqs_client.send_message(
                QueueUrl=LLM_REQUEST_SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body, ensure_ascii=False)
            )
            logger.info(f"Successfully sent global summary message with {len(all_articles)} articles.")
        except Exception as e:
            logger.error(f"Failed to send message to SQS: {e}")
            raise e

        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully sent global summary message.")
        }

    except Exception as e:
        logger.exception("Critical error in lambda_handler")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }