import requests
import psycopg2
from psycopg2.extras import execute_values
import time
import logging
import os
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
        logging.FileHandler("batch_update.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_db_connection():
    dsn = (f"host={DB_CONFIG['host']} dbname={DB_CONFIG['database']} "
           f"user={DB_CONFIG['user']} password={DB_CONFIG['password']} "
           f"port={DB_CONFIG['port']} options='-c client_encoding=utf8'")
    return psycopg2.connect(dsn)

def get_existing_data():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT channel_id, name, subscriber_count, video_count FROM oshilive.channels")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {row[0]: {"name": row[1], "subs": row[2], "vids": row[3]} for row in rows}

def create_tuple(channel):
    # 불필요한 값(banner, views 등)은 빼고 확실한 것만!
    return (
        channel.get('id'),
        channel.get('name'),
        channel.get('english_name'),
        channel.get('org'),
        channel.get('photo'),
        channel.get('twitter'),
        channel.get('subscriber_count', 0),
        channel.get('video_count', 0),
        True
    )

def save_to_db(values):
    query = """
    INSERT INTO oshilive.channels (
        channel_id, name, english_name, org, 
        profile_img_url, twitter_id, 
        subscriber_count, video_count, is_active
    ) VALUES %s
    ON CONFLICT (channel_id) DO UPDATE SET
        name = EXCLUDED.name,
        english_name = EXCLUDED.english_name,
        org = EXCLUDED.org,
        profile_img_url = EXCLUDED.profile_img_url,
        twitter_id = EXCLUDED.twitter_id,
        subscriber_count = EXCLUDED.subscriber_count,
        video_count = EXCLUDED.video_count,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        execute_values(cur, query, values)
        conn.commit()
        logging.info(f"✅ DB 저장 성공 (총 {len(values)}건)")
    except Exception as e:
        logging.error(f"❌ DB 저장 에러: {e}")
    finally:
        if conn: conn.close()

def run_batch():
    headers = {"X-APIKEY": API_KEY}
    exclude_keywords = ['clip', '클립', '切抜き', '切り抜き', 'fan', 'archive', '다시보기', 'vod', 'replay']
    
    try:
        existing_data = get_existing_data()
        logging.info(f"📊 기존 데이터 {len(existing_data)}건 로드 완료")
    except Exception as e:
        logging.error(f"❌ 로드 실패: {e}")
        return

    update_values = []
    limit = 100
    offset = 0
    stats = {"new": 0, "updated": 0, "skipped": 0}

    while True:
        list_url = "https://holodex.net/api/v2/channels"
        params = {"limit": limit, "offset": offset, "type": "vtuber", "lang": "ja"}
        
        resp = requests.get(list_url, headers=headers, params=params)
        if resp.status_code != 200: break
        data = resp.json()
        if not data: break

        for channel in data:
            c_id = channel.get('id')
            c_name = channel.get('name')
            new_subs = channel.get('subscriber_count', 0)
            new_vids = channel.get('video_count', 0)

            if any(k in (c_name or "").lower() for k in exclude_keywords):
                continue

            # 1. 신규 채널 로그
            if c_id not in existing_data:
                logging.info(f"✨ [신규] {c_name} ({c_id}) 추가")
                stats["new"] += 1
                update_values.append(create_tuple(channel))
            
            # 2. 기존 채널 변경사항 로그
            else:
                old = existing_data[c_id]
                if old['name'] != c_name or old['subs'] != new_subs or old['vids'] != new_vids:
                    
                    diff_log = []
                    if old['name'] != c_name: diff_log.append(f"이름 변경")
                    if old['subs'] != new_subs: diff_log.append(f"구독자: {old['subs']} -> {new_subs}")
                    if old['vids'] != new_vids: diff_log.append(f"비디오: {old['vids']} -> {new_vids}")
                    
                    logging.info(f"🔄 [수정] {c_name}: {', '.join(diff_log)}")
                    stats["updated"] += 1
                    update_values.append(create_tuple(channel))
                else:
                    stats["skipped"] += 1

        offset += limit

    if update_values:
        save_to_db(update_values)
    
    logging.info(f"🏁 배치 완료 | 신규: {stats['new']} | 수정: {stats['updated']} | 유지: {stats['skipped']}")

if __name__ == "__main__":
    run_batch()