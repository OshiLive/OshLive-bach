import requests
import psycopg2
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# 1. 환경 변수 로드
load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT")
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("enrich_parallel_load.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_db_connection():
    dsn = (f"host={DB_CONFIG['host']} dbname={DB_CONFIG['database']} "
           f"user={DB_CONFIG['user']} password={DB_CONFIG['password']} "
           f"port={DB_CONFIG['port']} options='-c client_encoding=utf8'")
    return psycopg2.connect(dsn)

def get_target_channels():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # 데이터가 비어있는 채널 추출
        query = "SELECT channel_id FROM oshilive.channels WHERE banner_img_url IS NULL OR top_topics IS NULL;"
        cur.execute(query)
        return [row[0] for row in rows] if (rows := cur.fetchall()) else []
    finally:
        if conn: conn.close()

def enrich_channel_data(channel_id):
    """
    이 함수는 각 스레드에서 실행됩니다. 
    호출될 때마다 DB 연결을 맺으므로 max_workers 관리가 필수입니다.
    """
    headers = {"X-APIKEY": API_KEY}
    url = f"https://holodex.net/api/v2/channels/{channel_id}"
    
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        remaining = resp.headers.get('X-RateLimit-Remaining', 'unknown')
        
        if resp.status_code == 429:
            return f"🚫 {channel_id} 한도 초과!"
        if resp.status_code != 200:
            return f"⚠️ {channel_id} 실패 ({resp.status_code})"
            
        data = resp.json()
        
        # 데이터 가공
        banner = data.get('banner') or data.get('header')
        views = data.get('view_count') or 0
        handle = data.get('yt_handle')[0] if data.get('yt_handle') else None
        
        group_list = [g for g in [data.get('group'), data.get('suborg')] if g]
        unique_groups = list(set(group_list)) if group_list else None

        # --- DB 업데이트 작업 ---
        update_query = """
        UPDATE oshilive.channels SET
            banner_img_url = %s, total_view_count = %s, yt_handle = %s,
            sub_groups = %s, top_topics = %s, description = %s, 
            lang = %s, published_at = %s, updated_at = CURRENT_TIMESTAMP
        WHERE channel_id = %s;
        """
        
        conn = None
        try:
            conn = get_db_connection() # 스레드마다 새로운 커넥션 생성
            cur = conn.cursor()
            cur.execute(update_query, (
                banner, views, handle, unique_groups, data.get('top_topics'), 
                data.get('description'), data.get('lang'), 
                data.get('published_at'), channel_id
            ))
            conn.commit()
        finally:
            if conn: conn.close() # 작업 즉시 반납 (매우 중요)
        
        return f"✅ {data.get('name')} 완료 (잔여: {remaining})"

    except Exception as e:
        return f"❌ {channel_id} 에러: {str(e)}"

def run_full_parallel_enrichment():
    targets = get_target_channels()
    if not targets:
        logging.info("✨ 보충할 데이터가 없습니다.")
        return

    SAFE_WORKERS = 7
    logging.info(f"🚀 총 {len(targets)}개 채널 병렬 보충 시작 (안전 모드 Worker: {SAFE_WORKERS})")

    with ThreadPoolExecutor(max_workers=SAFE_WORKERS) as executor:
        future_to_id = {executor.submit(enrich_channel_data, c_id): c_id for c_id in targets}
        
        for count, future in enumerate(as_completed(future_to_id), 1):
            logging.info(f"[{count}/{len(targets)}] {future.result()}")

    logging.info("🏁 모든 보충이 완료되었습니다!")

if __name__ == "__main__":
    run_full_parallel_enrichment()