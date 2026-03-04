import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# 1. .env 파일 로드
load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT")
}

# --- 로깅 설정 ---
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
    """DB에서 상세 정보(배너, 토픽 등)가 비어있는 모든 채널을 가져옵니다."""
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
    SELECT channel_id FROM oshilive.channels 
    WHERE banner_img_url IS NULL 
       OR top_topics IS NULL;
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]

def enrich_channel_data(channel_id):
    headers = {"X-APIKEY": API_KEY}
    url = f"https://holodex.net/api/v2/channels/{channel_id}"
    
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        
        # API 한도 정보 추출
        remaining = resp.headers.get('X-RateLimit-Remaining', 'unknown')
        
        if resp.status_code == 429:
            return f"🚫 {channel_id} 한도 초과! 잠시 중단이 필요합니다."
        if resp.status_code != 200:
            return f"⚠️ {channel_id} 실패 (Status: {resp.status_code})"
            
        data = resp.json()
        
        # 데이터 추출 및 가공
        banner = data.get('banner') or data.get('header')
        views = data.get('view_count') or 0
        handle = data.get('yt_handle')[0] if data.get('yt_handle') else None
        topics = data.get('top_topics')
        
        group_list = []
        if data.get('group'): group_list.append(data.get('group'))
        if data.get('suborg'): group_list.append(data.get('suborg'))
        unique_groups = list(set(group_list)) if group_list else None

        # DB 업데이트 (top_topics 포함)
        update_query = """
        UPDATE oshilive.channels SET
            banner_img_url = %s, total_view_count = %s, yt_handle = %s,
            sub_groups = %s, top_topics = %s, description = %s, 
            lang = %s, published_at = %s, updated_at = CURRENT_TIMESTAMP
        WHERE channel_id = %s;
        """
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(update_query, (
            banner, views, handle, unique_groups, topics, 
            data.get('description'), data.get('lang'), 
            data.get('published_at'), channel_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        
        return f"✅ {data.get('name')} 완료 (잔여 한도: {remaining})"

    except Exception as e:
        return f"❌ {channel_id} 에러: {str(e)}"

def run_full_parallel_enrichment():
    targets = get_target_channels()
    total = len(targets)
    
    if not targets:
        logging.info("✨ 보충할 데이터가 없습니다. 이미 모든 정보가 가득 찼습니다!")
        return

    logging.info(f"🚀 총 {total}개 채널 병렬 보충 시작 (Worker: 10)")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_id = {executor.submit(enrich_channel_data, c_id): c_id for c_id in targets}
        
        count = 0
        for future in as_completed(future_to_id):
            count += 1
            result = future.result()
            # 10개마다 진행 상황을 크게 한 번씩 찍어줌
            logging.info(f"[{count}/{total}] {result}")

    logging.info("🏁 모든 상세 정보 보충이 완료되었습니다!")

if __name__ == "__main__":
    run_full_parallel_enrichment()