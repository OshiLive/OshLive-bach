import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
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
    handlers=[logging.StreamHandler()]
)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_and_register_channel(cur, channel_id):
    """테이블 정의(oshilive.channels)에 맞춰 신규 채널 등록"""
    url = f"https://holodex.net/api/v2/channels/{channel_id}"
    headers = {"X-APIKEY": API_KEY}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            c = resp.json()
            
            # 테이블 정의서의 컬럼명과 정확히 매칭 (profile_img_url, banner_img_url, twitter_id 등)
            query = """
                INSERT INTO oshilive.channels (
                    channel_id, name, english_name, org, 
                    profile_img_url, banner_img_url, description, 
                    twitter_id, lang, subscriber_count, video_count,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (channel_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    profile_img_url = EXCLUDED.profile_img_url,
                    banner_img_url = EXCLUDED.banner_img_url,
                    subscriber_count = EXCLUDED.subscriber_count,
                    updated_at = CURRENT_TIMESTAMP;
            """
            
            # Holodex API 응답값과 DB 컬럼 매핑
            cur.execute(query, (
                c['id'], 
                c['name'], 
                c.get('english_name'), 
                c.get('org'),
                c.get('photo'),        # Holodex의 photo -> profile_img_url
                c.get('banner'),       # Holodex의 banner -> banner_img_url
                c.get('description'),
                c.get('twitter'),      # Holodex의 twitter -> twitter_id
                c.get('lang'),
                c.get('subscriber_count', 0),
                c.get('video_count', 0)
            ))
            logging.info(f"✨ 신규 채널 정식 등록 완료: {c['name']} ({channel_id})")
        else:
            logging.error(f"❌ 채널 정보 조회 실패 ({channel_id}): {resp.status_code}")
    except Exception as e:
        logging.error(f"❌ 채널 등록 중 오류: {e}")

def fetch_live_streams():
    """Holodex API에서 현재 라이브 및 예정된 방송 목록 페이징 수집"""
    url = "https://holodex.net/api/v2/live"
    all_streams = []
    offset = 0
    limit = 50

    while True:
        params = {
            "status": "live,upcoming",
            "type": "stream",
            "limit": limit,
            "offset": offset,
            "org": "Hololive"  # 필요한 경우 수정/삭제 가능
        }
        headers = {"X-APIKEY": API_KEY}
        
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                if not data: break
                all_streams.extend(data)
                if len(data) < limit: break
                offset += limit
            else:
                logging.error(f"❌ API 호출 실패: {resp.status_code}")
                break
        except Exception as e:
            logging.error(f"❌ 요청 중 에러: {e}")
            break
            
    logging.info(f"✅ 총 {len(all_streams)}개의 스트림 수집 완료")
    return all_streams

def update_streams_db(streams):
    if not streams:
        logging.info("ℹ️ 현재 수집된 라이브 스트림이 없습니다.")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # --- [핵심 추가] FK 에러 방지용 사전 채널 체크 로직 ---
        stream_channel_ids = list(set([s['channel']['id'] for s in streams]))
        
        # 현재 DB에 있는 채널 조회
        cur.execute("SELECT channel_id FROM oshilive.channels WHERE channel_id = ANY(%s)", (stream_channel_ids,))
        existing_ids = {r[0] for r in cur.fetchall()}
        
        # 없는 채널들만 골라서 정식 등록 진행
        missing_ids = [c_id for c_id in stream_channel_ids if c_id not in existing_ids]
        for m_id in missing_ids:
            fetch_and_register_channel(cur, m_id)
        
        # 커밋하여 채널들을 확정 (이후 streams INSERT를 위해)
        conn.commit() 
        # --------------------------------------------------

        # 1. 데이터 가공
        values = []
        for s in streams:
            values.append((
                s['id'], s['channel']['id'], s['title'], s.get('topic_id'), s['status'],
                s.get('start_scheduled'), s.get('start_actual'), s.get('end_actual'),
                f"https://i.ytimg.com/vi/{s['id']}/maxresdefault.jpg",
                s.get('live_viewers', 0)
            ))

        # 2. Streams Upsert
        upsert_query = """
        INSERT INTO oshilive.streams (
            stream_id, channel_id, title, topic_id, status, 
            start_scheduled, start_actual, end_actual, thumbnail_url, current_viewers
        ) VALUES %s
        ON CONFLICT (stream_id) DO UPDATE SET
            title = EXCLUDED.title,
            topic_id = EXCLUDED.topic_id,
            status = EXCLUDED.status,
            start_actual = COALESCE(EXCLUDED.start_actual, oshilive.streams.start_actual),
            end_actual = EXCLUDED.end_actual,
            current_viewers = EXCLUDED.current_viewers,
            thumbnail_url = EXCLUDED.thumbnail_url,
            updated_at = CURRENT_TIMESTAMP;
        """
        execute_values(cur, upsert_query, values)
        
        # 3. 종료된 방송 처리
        active_ids = [s['id'] for s in streams]
        cur.execute("""
            UPDATE oshilive.streams SET status = 'past', updated_at = CURRENT_TIMESTAMP
            WHERE status IN ('live', 'upcoming') AND NOT (stream_id = ANY(%s));
        """, (active_ids,))
        
        conn.commit()
        logging.info(f"✅ {len(values)}개의 스트림 업데이트 및 종료 처리 완료")

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ DB 업데이트 에러: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    live_data = fetch_live_streams()
    update_streams_db(live_data)