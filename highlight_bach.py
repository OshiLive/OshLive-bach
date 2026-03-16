import psycopg2
from psycopg2.extras import Json, execute_values
import logging
import os
import time
import pytchat
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import sys

# ==========================================
# 1. 환경 변수 및 로깅 설정
# ==========================================
load_dotenv()
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
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("highlight_bach.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# 불필요한 HTTP 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("pytchat").setLevel(logging.WARNING) # pytchat 자체 로그도 제어

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# ==========================================
# 2. 핵심 로직: 유튜브 채팅 수집기 (안전장치 강화)
# ==========================================
def analyze_chat_replay(stream_id, current_idx, total_idx):
    prog = f"({current_idx}/{total_idx})"
    logging.info(f"[{stream_id}] {prog} 💬 채팅 수집 시작...")
    
    try:
        try:
            chat = pytchat.create(video_id=stream_id, interruptable=False)
        except Exception as e:
            logging.warning(f"[{stream_id}] {prog} ⚠️ 초기화 실패: {e}")
            return None, 0
        
        if not chat.is_alive():
            logging.warning(f"[{stream_id}] {prog} ⚠️ 채팅창 비활성화/비공개.")
            return None, 0
        
        timeline_buckets = {}
        total_duration = 0
        msg_count = 0
        last_logged_min = -1
        empty_retry_count = 0
        
        while chat.is_alive():
            items = chat.get().sync_items()
            if not items:
                empty_retry_count += 1
                if empty_retry_count >= 15: break
                time.sleep(1)
                continue
            
            empty_retry_count = 0
            for c in items:
                # --- [시간 변환 안전장치 시작] ---
                if not c.elapsedTime: continue
                
                try:
                    # '-' 제거 후 콜론으로 자르고, 실제 숫자인 것만 추출 ('' 에러 방지)
                    raw_parts = c.elapsedTime.replace("-", "").split(":")
                    time_parts = [p.strip() for p in raw_parts if p.strip().isdigit()]
                    
                    if not time_parts: continue # 숫자가 없으면 스킵
                    
                    if len(time_parts) == 3: # HH:MM:SS
                        sec = int(time_parts[0])*3600 + int(time_parts[1])*60 + int(time_parts[2])
                    elif len(time_parts) == 2: # MM:SS
                        sec = int(time_parts[0])*60 + int(time_parts[1])
                    else: # SS
                        sec = int(time_parts[0])
                except (ValueError, IndexError):
                    continue # 변환 에러 시 해당 채팅만 버림
                # --- [시간 변환 안전장치 끝] ---

                msg_count += 1
                if sec > total_duration: total_duration = sec
                
                bucket_sec = (sec // 60) * 60
                timeline_buckets[bucket_sec] = timeline_buckets.get(bucket_sec, 0) + 1
                
                current_min = sec // 60
                if current_min > last_logged_min:
                    logging.info(f"[{stream_id}] {prog} ⏳ 수집 중... ({current_min}분 통과 / 누적 {msg_count:,}개)")
                    last_logged_min = current_min

        timeline_data = [{"time_sec": k, "messages": v} for k, v in timeline_buckets.items()]
        timeline_data.sort(key=lambda x: x["time_sec"])
        
        if msg_count > 0:
            logging.info(f"[{stream_id}] {prog} 🏁 수집 완료! ({msg_count:,}개)")
        return timeline_data, total_duration

    except Exception as e:
        logging.error(f"[{stream_id}] {prog} ❌ 심각한 에러: {e}")
        return None, 0

# ==========================================
# 3. 핫클립 추출기
# ==========================================
def extract_segments(timeline_data, threshold_multiplier=2.0, top_n=5):
    if not timeline_data: return []
    total_msgs = sum(item["messages"] for item in timeline_data)
    avg_msgs = total_msgs / len(timeline_data) if timeline_data else 0
    
    peaks = [item for item in timeline_data if item["messages"] >= (avg_msgs * threshold_multiplier)]
    peaks.sort(key=lambda x: x["messages"], reverse=True)
    
    segments = []
    for peak in peaks[:top_n]:
        p_time = peak["time_sec"]
        start_time = max(0, p_time - 30)
        end_time = p_time + 90
        mini_chart = [item["messages"] for item in timeline_data if start_time <= item["time_sec"] <= end_time]
        segments.append({"start": start_time, "end": end_time, "mini_chart": mini_chart})
    return segments

# ==========================================
# 4. 큐 처리기
# ==========================================
def process_queue(worker_id):
    conn = None
    task_info = None
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 실시간 진행률 계산용
        cur.execute("SELECT COUNT(*) FROM oshilive.highlight_batch_tasks WHERE status IN (0, 2);")
        remaining = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM oshilive.highlight_batch_tasks WHERE status = 1;")
        finished = cur.fetchone()[0]
        
        total = remaining + finished
        current = finished + 1

        cur.execute("""
            SELECT stream_id FROM oshilive.highlight_batch_tasks 
            WHERE status = 0 ORDER BY created_at ASC 
            LIMIT 1 FOR UPDATE SKIP LOCKED;
        """)
        row = cur.fetchone()
        
        if row:
            stream_id = row[0]
            cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 2 WHERE stream_id = %s;", (stream_id,))
            conn.commit()
            task_info = (stream_id, current, total)
        
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"👷 [워커-{worker_id}] 큐 가져오기 실패: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

    if not task_info: return False

    s_id, c_idx, t_idx = task_info
    logging.info(f"👷 [워커-{worker_id}] 🎯 작업 시작 ({c_idx}/{t_idx}): {s_id}")
    
    # 분석
    timeline_data, duration = analyze_chat_replay(s_id, c_idx, t_idx)
    
    # 결과 저장
    conn2 = None
    try:
        conn2 = get_db_connection()
        cur2 = conn2.cursor()
        
        if timeline_data:
            segments = extract_segments(timeline_data)
            cur2.execute("""
                INSERT INTO oshilive.stream_highlights (stream_id, duration_sec, timeline_data)
                VALUES (%s, %s, %s) ON CONFLICT (stream_id) DO UPDATE SET
                duration_sec = EXCLUDED.duration_sec, timeline_data = EXCLUDED.timeline_data, updated_at = CURRENT_TIMESTAMP;
            """, (s_id, duration, Json(timeline_data)))
            
            if segments:
                cur2.execute("DELETE FROM oshilive.highlight_segments WHERE stream_id = %s;", (s_id,))
                execute_values(cur2, "INSERT INTO oshilive.highlight_segments (stream_id, start_time_sec, end_time_sec, mini_chart_data) VALUES %s;",
                               [(s_id, s["start"], s["end"], Json(s["mini_chart"])) for s in segments])
            
            cur2.execute("UPDATE oshilive.highlight_batch_tasks SET status = 1, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (s_id,))
            logging.info(f"👷 [워커-{worker_id}] ✅ ({c_idx}/{t_idx}) 저장 완료!")
        else:
            cur2.execute("UPDATE oshilive.highlight_batch_tasks SET status = 9, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (s_id,))
        
        conn2.commit()
    except Exception as e:
        if conn2: conn2.rollback()
        logging.error(f"👷 [워커-{worker_id}] DB 저장 실패: {e}")
    finally:
        if conn2:
            cur2.close()
            conn2.close()
    return True

# ==========================================
# 5. 실행 루프
# ==========================================
def worker_loop(worker_id):
    time.sleep(worker_id * 1.5)
    while True:
        has_work = process_queue(worker_id)
        if not process_queue(worker_id):
            break
        else:
            time.sleep(2)

if __name__ == "__main__":
    logging.info("🚀 [하이라이트 공장] 가동 시작...")
    WORKER_COUNT = 3 
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        for i in range(WORKER_COUNT):
            executor.submit(worker_loop, i + 1)