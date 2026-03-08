import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from datetime import datetime, timezone
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

def fetch_and_register_channel(cur, channel_id):
    """DB에 없는 채널(주인공/게스트) 발견 시 정식 등록"""
    url = f"https://holodex.net/api/v2/channels/{channel_id}"
    headers = {"X-APIKEY": API_KEY}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            c = resp.json()
            query = """
                INSERT INTO oshilive.channels (
                    channel_id, name, english_name, org, profile_img_url, 
                    banner_img_url, description, twitter_id, lang, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (channel_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP;
            """
            cur.execute(query, (
                c['id'], c['name'], c.get('english_name'), c.get('org'),
                c.get('photo'), c.get('banner'), c.get('description'), 
                c.get('twitter'), c.get('lang')
            ))
            logging.info(f"  └─ ✨ 신규 채널 등록: {c['name']}")
    except Exception as e:
        logging.error(f"  └─ ❌ 채널 등록 실패: {e}")

def update_streams():
    # 2. 오늘/내일 범위만 가져오기 (UTC 기준)
    now_utc = datetime.now(timezone.utc)
    from_date = (now_utc - timedelta(hours=6)).strftime('%Y-%m-%dT%H:%M:%SZ') # 유령 라이브 방어용 (최근 6시간 내 시작된 것부터)
    to_date = (now_utc + timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')

    url = "https://holodex.net/api/v2/live"
    params = {
        "type": "stream",
        "status": "live,upcoming",
        "from": from_date,
        "to": to_date,
        "org": "Hololive", # 원하는 그룹명
        "limit": 100
    }
    headers = {"X-APIKEY": API_KEY}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        streams = resp.json() if resp.status_code == 200 else []
        if not streams:
            logging.info("ℹ️ 현재 수집된 활성 스트림이 없습니다.")
            return

        conn = get_db_connection()
        cur = conn.cursor()

        # 3. 채널 선등록 (FK 에러 방지)
        all_ids = {s['channel']['id'] for s in streams}
        for s in streams:
            if s.get('mentions'):
                for m in s['mentions']: all_ids.add(m['id'])
        
        cur.execute("SELECT channel_id FROM oshilive.channels WHERE channel_id = ANY(%s)", (list(all_ids),))
        existing_ids = {r[0] for r in cur.fetchall()}
        for c_id in (all_ids - existing_ids):
            fetch_and_register_channel(cur, c_id)
        conn.commit()

        # 4. 데이터 가공 및 UPSERT
        stream_values = []
        collab_values = []
        active_ids = []

        for s in streams:
            # 유령 대기방 방어: live인데 시청자가 0명이면 DB에는 status만 live로 넣고 UI에서 필터링 권장
            active_ids.append(s['id'])
            stream_values.append((
                s['id'], s['channel']['id'], s['title'], s.get('topic_id'), s['status'],
                s.get('start_scheduled'), s.get('start_actual'), s.get('end_actual'),
                f"https://i.ytimg.com/vi/{s['id']}/maxresdefault.jpg",
                s.get('live_viewers', 0)
            ))
            if s.get('mentions'):
                for m in s['mentions']: collab_values.append((s['id'], m['id']))

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

        if collab_values:
            execute_values(cur, "INSERT INTO oshilive.stream_collabs VALUES %s ON CONFLICT DO NOTHING", collab_values)

        # 5. [핵심] 종료 시간(end_actual) 강제 업데이트
        # 이번 API 응답에 없는데 DB에는 live/upcoming인 것들을 'past'로 변경
        cur.execute("""
            UPDATE oshilive.streams 
            SET status = 'past', 
                end_actual = COALESCE(end_actual, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            WHERE status IN ('live', 'upcoming') AND NOT (stream_id = ANY(%s));
        """, (active_ids,))

        conn.commit()
        logging.info(f"🚀 배치 완료: {len(stream_values)}개 갱신, 종료 감지 업데이트 수행됨")

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        logging.error(f"💥 배치 실행 중 에러: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    from datetime import timedelta
    update_streams()