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

# --- 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_live_streams():
    """Holodex API에서 현재 라이브 및 예정된 모든 방송 목록을 가져옵니다."""
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
            "org": "Hololive" # 테스트 중에는 특정 그룹을 지정하는 게 안전합니다. 전체를 보려면 이 줄을 삭제하세요.
        }
        headers = {"X-APIKEY": API_KEY}
        
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                if not data: # 더 이상 가져올 데이터가 없으면 루프 종료
                    break
                all_streams.extend(data)
                logging.info(f"📡 데이터 수집 중... (현재까지 {len(all_streams)}개)")
                
                # 만약 가져온 데이터가 limit보다 적으면 다음 페이지가 없다는 뜻
                if len(data) < limit:
                    break
                
                offset += limit # 다음 페이지로 이동
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

    # 1. 데이터 가공 (총 10개 필드)
    values = []
    for s in streams:
        values.append((
            s['id'],
            s['channel']['id'],
            s['title'],
            s.get('topic_id'),
            s['status'],
            s.get('start_scheduled'),
            s.get('start_actual'),
            s.get('end_actual'),
            f"https://i.ytimg.com/vi/{s['id']}/maxresdefault.jpg",
            s.get('live_viewers', 0)
        ))

    # 2. Upsert 쿼리 (INSERT 컬럼 10개와 VALUES 10개 일치시킴)
    upsert_query = """
    INSERT INTO oshilive.streams (
        stream_id, channel_id, title, topic_id, status, 
        start_scheduled, start_actual, end_actual, 
        thumbnail_url, current_viewers
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

    try:
        # 데이터 삽입/업데이트
        execute_values(cur, upsert_query, values)
        conn.commit()
        logging.info(f"✅ {len(values)}개의 스트림 DB 저장 완료")
        
        # 3. 종료된 방송 처리 (Cleanup)
        # 현재 API 응답에 없는 stream_id 중 DB에 live/upcoming인 놈들을 past로 변경
        active_ids = [s['id'] for s in streams]
        
        cur.execute("""
            UPDATE oshilive.streams 
            SET status = 'past', updated_at = CURRENT_TIMESTAMP
            WHERE status IN ('live', 'upcoming') 
              AND NOT (stream_id = ANY(%s));
        """, (active_ids,))
        
        conn.commit()
        logging.info("🧹 종료된 방송 상태 갱신 완료")

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ DB 업데이트 에러: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    live_data = fetch_live_streams()
    update_streams_db(live_data)