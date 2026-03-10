import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
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
    # options 설정을 통해 인코딩 및 타임아웃 보조 설정 가능
    dsn = (f"host={DB_CONFIG['host']} dbname={DB_CONFIG['database']} "
           f"user={DB_CONFIG['user']} password={DB_CONFIG['password']} "
           f"port={DB_CONFIG['port']} options='-c client_encoding=utf8'")
    return psycopg2.connect(dsn)

def get_existing_data():
    """기존 데이터를 가져올 때만 잠깐 연결하고 바로 닫음"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT channel_id, name, subscriber_count, video_count FROM oshilive.channels")
        rows = cur.fetchall()
        return {row[0]: {"name": row[1], "subs": row[2], "vids": row[3]} for row in rows}
    except Exception as e:
        logging.error(f"❌ 기존 데이터 로드 중 에러: {e}")
        return {}
    finally:
        if conn:
            cur.close()
            conn.close()

def create_tuple(channel):
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
    """실제 저장이 필요할 때만 연결해서 처리 (점유 시간 최소화)"""
    if not values:
        return

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
        if conn: conn.rollback()
        logging.error(f"❌ DB 저장 에러: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            logging.info("🔒 DB 커넥션 반납 완료")

def run_batch():
    headers = {"X-APIKEY": API_KEY}
    exclude_keywords = ['clip', '클립', '切抜き', '切り抜き', 'fan', 'archive', '다시보기', 'vod', 'replay']
    
    # 1. 시작 시점에 기존 데이터 로드 (연결 바로 닫힘)
    existing_data = get_existing_data()
    if not existing_data and os.getenv("DB_HOST"): # DB 연결 실패 대응
        logging.warning("⚠️ 기존 데이터를 불러오지 못했습니다. 신규 등록 위주로 진행합니다.")

    update_values = []
    limit = 100
    offset = 0
    stats = {"new": 0, "updated": 0, "skipped": 0}

    # 2. API 데이터 수집 루프 (DB 연결 없음)
    logging.info("🌐 Holodex API 데이터 수집 시작...")
    while True:
        list_url = "https://holodex.net/api/v2/channels"
        params = {"limit": limit, "offset": offset, "type": "vtuber", "lang": "ja"}
        
        try:
            # 타임아웃을 30초로 넉넉하게 설정
            resp = requests.get(list_url, headers=headers, params=params, timeout=30)
            if resp.status_code != 200: 
                logging.error(f"API 응답 에러: {resp.status_code}")
                break
            
            data = resp.json()
            if not data: break

            for channel in data:
                c_id = channel.get('id')
                c_name = channel.get('name')
                new_subs = channel.get('subscriber_count', 0)
                new_vids = channel.get('video_count', 0)

                if any(k in (c_name or "").lower() for k in exclude_keywords):
                    continue

                if c_id not in existing_data:
                    logging.info(f"✨ [신규] {c_name}")
                    stats["new"] += 1
                    update_values.append(create_tuple(channel))
                else:
                    old = existing_data[c_id]
                    if old['name'] != c_name or old['subs'] != new_subs or old['vids'] != new_vids:
                        stats["updated"] += 1
                        update_values.append(create_tuple(channel))
                    else:
                        stats["skipped"] += 1

            offset += limit
            # API 과부하 방지를 위한 미세한 대기 (선택 사항)
            # time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            logging.error(f"🌐 API 호출 중 네트워크 에러: {e}")
            break

    # 3. 수집 완료 후 한 번에 DB 저장
    if update_values:
        logging.info(f"💾 DB 저장 시작 (업데이트 대상: {len(update_values)}건)...")
        save_to_db(update_values)
    else:
        logging.info("ℹ️ 변경사항이 없어 DB 저장을 건너뜁니다.")
    
    logging.info(f"🏁 배치 완료 | 신규: {stats['new']} | 수정: {stats['updated']} | 유지: {stats['skipped']}")

if __name__ == "__main__":
    run_batch()