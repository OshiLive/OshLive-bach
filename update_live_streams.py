import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# 1. 환경 변수 및 로그 설정
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
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_channel_info(channel_id):
    """DB 연결 없이 순수하게 API 데이터만 가져옴"""
    url = f"https://holodex.net/api/v2/channels/{channel_id}"
    headers = {"X-APIKEY": API_KEY}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logging.error(f" └─ ❌ API 호출 실패 ({channel_id}): {e}")
    return None

def update_streams():
    # 2. API 데이터 먼저 가져오기 (DB 연결 전)
    now_utc = datetime.now(timezone.utc)
    from_date = (now_utc - timedelta(hours=6)).strftime('%Y-%m-%dT%H:%M:%SZ')
    to_date = (now_utc + timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')

    url = "https://holodex.net/api/v2/live"
    params = {
        "type": "stream", "status": "live,upcoming",
        "from": from_date, "to": to_date,
        "org": "Hololive", "limit": 100
    }
    headers = {"X-APIKEY": API_KEY}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        streams = resp.json() if resp.status_code == 200 else []
        if not streams:
            logging.info("ℹ️ 현재 수집된 활성 스트림이 없습니다.")
            return

        # 3. DB 연결 시작 (필요한 시점에 최소한으로 열기)
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            # 채널 ID 추출
            all_ids = {s['channel']['id'] for s in streams}
            for s in streams:
                if s.get('mentions'):
                    for m in s['mentions']: all_ids.add(m['id'])
            
            # 기존 채널 확인
            cur.execute("SELECT channel_id FROM oshilive.channels WHERE channel_id = ANY(%s)", (list(all_ids),))
            existing_ids = {r[0] for r in cur.fetchall()}
            missing_ids = all_ids - existing_ids

            # 신규 채널 등록 (API 호출 시에는 커넥션을 끊거나 짧게 유지하는게 좋으나 일단 유지)
            for c_id in missing_ids:
                c_data = fetch_channel_info(c_id)
                if c_data:
                    query = """
                        INSERT INTO oshilive.channels (
                            channel_id, name, english_name, org, profile_img_url, 
                            banner_img_url, description, twitter_id, lang, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (channel_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP;
                    """
                    cur.execute(query, (
                        c_data['id'], c_data['name'], c_data.get('english_name'), c_data.get('org'),
                        c_data.get('photo'), c_data.get('banner'), c_data.get('description'), 
                        c_data.get('twitter'), c_data.get('lang')
                    ))
                    logging.info(f" └─ ✨ 신규 채널 등록: {c_data['name']}")
            
            conn.commit() # 채널 등록 확정

            # 4. 스트림 데이터 가공 및 UPSERT
            stream_values = []
            collab_values = []
            stats_values = []
            active_ids = [s['id'] for s in streams]

            for s in streams:
                stream_values.append((
                    s['id'], s['channel']['id'], s['title'], s.get('topic_id'), s['status'],
                    s.get('start_scheduled'), s.get('start_actual'), s.get('end_actual'),
                    f"https://i.ytimg.com/vi/{s['id']}/maxresdefault.jpg",
                    s.get('live_viewers', 0)
                ))
                #합방 데이터
                if s.get('mentions'):
                    for m in s['mentions']: collab_values.append((s['id'], m['id']))
                #시청자 수 수집
                if s.get('status') == 'live':
                    stats_values.append((s['id'], s.get('live_viewers', 0)))

            upsert_query = """
                INSERT INTO oshilive.streams (
                    stream_id, channel_id, title, topic_id, status, 
                    start_scheduled, start_actual, end_actual, thumbnail_url, current_viewers
                ) VALUES %s
                ON CONFLICT (stream_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    status = EXCLUDED.status,
                    start_actual = COALESCE(oshilive.streams.start_actual, EXCLUDED.start_actual),
                    end_actual = EXCLUDED.end_actual,
                    current_viewers = EXCLUDED.current_viewers,
                    updated_at = CURRENT_TIMESTAMP;
            """
            execute_values(cur, upsert_query, stream_values)

            # 합방 INSERT
            if collab_values:
                execute_values(cur, "INSERT INTO oshilive.stream_collabs VALUES %s ON CONFLICT DO NOTHING", collab_values)

            # 시청자수 INSERT
            if stats_values:
                execute_values(cur, "INSERT INTO oshilive.stream_stats (stream_id, viewer_count) VALUES %s", stats_values)


            # 5. 종료 상태 업데이트
            cur.execute("""
                UPDATE oshilive.streams 
                SET status = 'past', 
                    end_actual = COALESCE(end_actual, CURRENT_TIMESTAMP),
                    updated_at = CURRENT_TIMESTAMP
                WHERE status IN ('live', 'upcoming') AND NOT (stream_id = ANY(%s));
            """, (active_ids,))

            conn.commit()
            logging.info(f"🚀 배치 완료: {len(stream_values)}개 갱신")

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()
            logging.info("🔒 DB 커넥션 반납 완료")

    except Exception as e:
        logging.error(f"💥 배치 실행 중 에러: {e}")

if __name__ == "__main__":
    update_streams()