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

def get_target_channels(only_missing_info=False):
    """
    only_missing_info=True: (1분 배치용) 빈칸이 있는 신규 채널만 가져옴
    only_missing_info=False: (자정 배치용) 구독자 갱신을 위해 모든 채널을 가져옴
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        if only_missing_info:
            query = """
                SELECT channel_id, name, subscriber_count, video_count 
                FROM oshilive.channels 
                WHERE banner_img_url IS NULL OR top_topics IS NULL;
            """
        else:
            query = "SELECT channel_id, name, subscriber_count, video_count FROM oshilive.channels;"
            
        cur.execute(query)
        return cur.fetchall() 
    finally:
        if conn: conn.close()

def enrich_channel_data(target_data):
    # 전달받은 기존 DB 데이터 언패킹 (구독자 변동 로그용)
    channel_id, ch_name, old_subs, old_vids = target_data
    old_subs = old_subs or 0
    old_vids = old_vids or 0

    headers = {"X-APIKEY": API_KEY}
    url = f"https://holodex.net/api/v2/channels/{channel_id}"
    
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        remaining = resp.headers.get('X-RateLimit-Remaining', 'unknown')
        
        if resp.status_code == 429:
            return f"🚫 {ch_name} 한도 초과!"
        if resp.status_code != 200:
            return f"⚠️ {ch_name} 실패 ({resp.status_code})"
            
        data = resp.json()
        
        # 데이터 가공
        banner = data.get('banner') or data.get('header')
        views = data.get('view_count') or 0
        handle = data.get('yt_handle')[0] if data.get('yt_handle') else None
        
        group_list = [g for g in [data.get('group'), data.get('suborg')] if g]
        unique_groups = list(set(group_list)) if group_list else None
        
        # 새로운 구독자 수와 영상 수 추출
        new_subs = data.get('subscriber_count') or old_subs
        new_vids = data.get('video_count') or old_vids

        # 기존 값과 비교하여 변동이 있으면 로그 출력
        if old_subs != new_subs or old_vids != new_vids:
            sub_diff = new_subs - old_subs
            vid_diff = new_vids - old_vids
            logging.info(
                f"📈 [{ch_name}] 정보 갱신: "
                f"구독자 {old_subs:,} ➔ {new_subs:,} ({sub_diff:+d}), "
                f"영상 {old_vids:,} ➔ {new_vids:,} ({vid_diff:+d})"
            )

        # --- DB 업데이트 작업 ---
        update_query = """
        UPDATE oshilive.channels SET
            banner_img_url = %s, total_view_count = %s, yt_handle = %s,
            sub_groups = %s, top_topics = %s, description = %s, 
            lang = %s, published_at = %s, 
            subscriber_count = %s, video_count = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE channel_id = %s;
        """
        
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(update_query, (
                banner, views, handle, unique_groups, data.get('top_topics'), 
                data.get('description'), data.get('lang'), data.get('published_at'), 
                new_subs, new_vids, channel_id
            ))
            conn.commit()
        finally:
            if conn: conn.close()
        
        return f"✅ {ch_name} 완료 (잔여: {remaining})"

    except Exception as e:
        return f"❌ {ch_name} 에러: {str(e)}"

def run_full_parallel_enrichment(only_missing_info=False):
    targets = get_target_channels(only_missing_info)
    if not targets:
        logging.info("✨ 보충할 데이터가 없습니다.")
        return

    SAFE_WORKERS = 7
    mode_text = "빈칸 보충" if only_missing_info else "전체 갱신"
    logging.info(f"🚀 총 {len(targets)}개 채널 병렬 {mode_text} 시작 (안전 모드 Worker: {SAFE_WORKERS})")

    with ThreadPoolExecutor(max_workers=SAFE_WORKERS) as executor:
        # target이 (id, name, subs, vids) 튜플이므로 전체를 넘깁니다.
        future_to_id = {executor.submit(enrich_channel_data, target): target[0] for target in targets}
        
        for count, future in enumerate(as_completed(future_to_id), 1):
            logging.info(f"[{count}/{len(targets)}] {future.result()}")

    logging.info(f"🏁 모든 {mode_text} 작업이 완료되었습니다!")

if __name__ == "__main__":
    # 단독으로 실행할 때는 모든 채널의 구독자 수를 갱신합니다.
    run_full_parallel_enrichment(only_missing_info=False)