import os
import logging
import time
import psycopg2

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 설정값
BATCH_SIZE = 1000  # 한 번에 삭제할 행 개수 (너무 크면 DB 부하, 너무 작으면 느림)
SAFETY_BUFFER_MS = 5000  # 람다 종료 전 여유 시간 (5초)

def lambda_handler(event, context):
    """
    Expired Token Batch Deletion Handler
    """
    conn = None
    total_deleted = 0
    
    try:
        # 1. DB 연결
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            dbname=os.environ.get('DB_NAME'),
            port=os.environ.get('DB_PORT', '5432'),
            connect_timeout=5
        )
        
        # 2. 배치 삭제 루프 시작
        with conn.cursor() as cur:
            logger.info("Starting batch deletion process...")
            
            while True:
                # [안전 장치] 람다 남은 시간이 5초 미만이면 루프 중단 (타임아웃 방지)
                remaining_time = context.get_remaining_time_in_millis()
                if remaining_time < SAFETY_BUFFER_MS:
                    logger.warning(f"Time limit approaching ({remaining_time}ms left). Stopping safely.")
                    break

                # [PostgreSQL 전용 쿼리] 서브쿼리를 이용해 ID를 먼저 조회하고 삭제
                # PostgreSQL은 DELETE문에 직접 LIMIT을 쓸 수 없으므로 이 방식이 표준입니다.
                query = """
                    DELETE FROM user_token
                    WHERE id IN (
                        SELECT id
                        FROM user_token
                        WHERE created_at < NOW() - INTERVAL '14 days'
                        LIMIT %s
                    );
                """
                
                cur.execute(query, (BATCH_SIZE,))
                deleted_count = cur.rowcount
                conn.commit() # 배치마다 커밋하여 DB Lock 해제 및 트랜잭션 로그 비우기
                
                total_deleted += deleted_count
                
                if deleted_count > 0:
                    logger.info(f"Batch deleted: {deleted_count} rows. (Total: {total_deleted})")
                    # DB 부하 조절을 위해 아주 잠깐 대기 (선택 사항, 0.1초)
                    time.sleep(0.1)
                
                # 삭제된 행이 0개거나 배치 사이즈보다 작으면 더 이상 삭제할 게 없다는 뜻
                if deleted_count < BATCH_SIZE:
                    logger.info("No more rows to delete.")
                    break
                    
        return {
            'statusCode': 200,
            'body': {
                'message': 'Batch cleanup complete',
                'totalDeleted': total_deleted
            }
        }

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        return {'statusCode': 500, 'body': f"Database error: {str(e)}"}
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {'statusCode': 500, 'body': f"Error: {str(e)}"}
        
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")