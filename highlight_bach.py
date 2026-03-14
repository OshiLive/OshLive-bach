import psycopg2
from psycopg2.extras import Json, execute_values
import logging
import os
import time
import pytchat
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

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
    datefmt='%Y-%m-%d %H:%M:%S'
)
# 보기 싫은 유튜브 HTTP 접속 로그 숨기기
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# ==========================================
# 2. 핵심 로직: 유튜브 채팅 수집기 (안전장치 포함)
# ==========================================
def analyze_chat_replay(stream_id):
    """pytchat을 이용해 채팅을 수집하고 1분 단위로 묶어줍니다."""
    logging.info(f"[{stream_id}] 💬 채팅 데이터 수집 및 분석 시작...")
    try:
        chat = pytchat.create(video_id=stream_id, interruptable=False)
        
        timeline_buckets = {}
        total_duration = 0
        msg_count = 0
        last_logged_min = -1
        empty_retry_count = 0  # 🛑 무한 멈춤 방지용 카운터
        
        while chat.is_alive():
            items = chat.get().sync_items()
            
            # 🛑 새로 가져온 채팅이 한 개도 없다면? (유튜브 지연/막힘 대응)
            if not items:
                empty_retry_count += 1
                if empty_retry_count >= 15:  # 약 30초 동안 빈손이면 강제 탈출
                    logging.warning(f"[{stream_id}] ⚠️ 응답 없음(채팅 준비 안됨). 대기 탈출!")
                    break
                time.sleep(2)
                continue
                
            # 정상적으로 채팅이 들어오면 카운터 초기화
            empty_retry_count = 0
            
            for c in items:
                msg_count += 1
                
                # 시간(초) 변환 로직
                time_parts = c.elapsedTime.replace("-", "").split(":")
                if len(time_parts) == 3:
                    sec = int(time_parts[0])*3600 + int(time_parts[1])*60 + int(time_parts[2])
                elif len(time_parts) == 2:
                    sec = int(time_parts[0])*60 + int(time_parts[1])
                else:
                    sec = int(time_parts[0])
                
                if sec > total_duration:
                    total_duration = sec

                # 📊 1분(60초) 단위 버킷으로 묶기
                bucket_sec = (sec // 60) * 60
                if bucket_sec not in timeline_buckets:
                    timeline_buckets[bucket_sec] = 0
                timeline_buckets[bucket_sec] += 1
                
                # ⏳ 1분 구간마다 작업 현황 생중계 (질문자님 요청 반영!)
                current_min = sec // 60
                if current_min > last_logged_min:
                    logging.info(f"[{stream_id}] ⏳ 수집 중... (영상 {current_min}분 통과 / 누적 {msg_count:,}개)")
                    last_logged_min = current_min

        # 최종 타임라인 데이터 정렬
        timeline_data = [{"time_sec": k, "messages": v} for k, v in timeline_buckets.items()]
        timeline_data.sort(key=lambda x: x["time_sec"])
        
        if msg_count > 0:
            logging.info(f"[{stream_id}] 🏁 수집 완료! (총 {total_duration // 60}분 / 총 {msg_count:,}개)")
            
        return timeline_data, total_duration

    except Exception as e:
        logging.error(f"[{stream_id}] ❌ 채팅 수집 중 에러: {e}")
        return None, 0

# ==========================================
# 3. 핫클립(피크) 구간 추출기
# ==========================================
def extract_segments(timeline_data, threshold_multiplier=2.0, top_n=5):
    """채팅량이 평균의 N배 이상인 핫클립 구간(1분)을 추출합니다."""
    if not timeline_data: return []
    
    total_msgs = sum(item["messages"] for item in timeline_data)
    avg_msgs = total_msgs / len(timeline_data)
    
    peaks = [item for item in timeline_data if item["messages"] >= (avg_msgs * threshold_multiplier)]
    peaks.sort(key=lambda x: x["messages"], reverse=True)
    top_peaks = peaks[:top_n]
    
    segments = []
    for peak in top_peaks:
        p_time = peak["time_sec"]
        # 피크 1분(60초) 구간의 시작점(p_time) 기준
        # 30초 전부터 시작해서, 피크 구간(60초)을 포함하고, 그 뒤로 30초 더!
        start_time = max(0, p_time - 30) 
        end_time = p_time + 90           # 30(전) + 60(피크) + 30(후) = 총 120초(2분)
        
        mini_chart = [item["messages"] for item in timeline_data if start_time <= item["time_sec"] <= end_time]
        
        segments.append({
            "start": start_time,
            "end": end_time,
            "mini_chart": mini_chart
        })
        
    return segments

# ==========================================
# 4. 큐(Queue) 작업 처리기 (DB 연결 분리)
# ==========================================
def process_queue(worker_id):
    # [1단계] 잠깐 DB 연결해서 일거리 하나만 훔쳐오기
    conn = None
    stream_id = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT stream_id FROM oshilive.highlight_batch_tasks 
            WHERE status = 0 
            ORDER BY created_at ASC 
            LIMIT 1 FOR UPDATE SKIP LOCKED;
        """)
        row = cur.fetchone()
        
        if not row:
            return False
            
        stream_id = row[0]
        # 찜한 작업은 상태를 2(처리중)로 바꿔서 남들이 못 건드리게 함
        cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 2 WHERE stream_id = %s;", (stream_id,))
        conn.commit()
        logging.info(f"👷 [워커-{worker_id}] 🎯 작업 시작: {stream_id}")
        
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"👷 [워커-{worker_id}] 💥 큐 가져오기 에러: {e}")
        return False
    finally:
        if conn:
            cur.close()
            conn.close() # 작업 가져왔으면 미련 없이 DB 선 뽑기!

    if not stream_id:
        return False

    # [2단계] DB 선 뽑힌 상태로 맘편히 유튜브 긁어오기 (제일 오래 걸림)
    timeline_data, duration = analyze_chat_replay(stream_id)
    
    # [3단계] 다시 DB 연결해서 결과 저장하기
    conn2 = None
    try:
        conn2 = get_db_connection()
        cur2 = conn2.cursor()
        
        if timeline_data: # 수집 성공 시
            segments = extract_segments(timeline_data)
            
            cur2.execute("""
                INSERT INTO oshilive.stream_highlights (stream_id, duration_sec, timeline_data)
                VALUES (%s, %s, %s)
                ON CONFLICT (stream_id) DO UPDATE SET
                    duration_sec = EXCLUDED.duration_sec,
                    timeline_data = EXCLUDED.timeline_data,
                    updated_at = CURRENT_TIMESTAMP;
            """, (stream_id, duration, Json(timeline_data)))
            
            if segments:
                cur2.execute("DELETE FROM oshilive.highlight_segments WHERE stream_id = %s;", (stream_id,))
                
                segment_values = [
                    (stream_id, seg["start"], seg["end"], Json(seg["mini_chart"]))
                    for seg in segments
                ]
                insert_query = "INSERT INTO oshilive.highlight_segments (stream_id, start_time_sec, end_time_sec, mini_chart_data) VALUES %s;"
                execute_values(cur2, insert_query, segment_values)
            
            # 성공 상태(1) 업데이트
            cur2.execute("UPDATE oshilive.highlight_batch_tasks SET status = 1, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
            conn2.commit()
            logging.info(f"👷 [워커-{worker_id}] ✅ [{stream_id}] 핫클립 {len(segments)}개 DB 저장 완료!")
        else:
            # 수집 실패 혹은 데이터 없음 (상태 9)
            cur2.execute("UPDATE oshilive.highlight_batch_tasks SET status = 9, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
            conn2.commit()
            logging.warning(f"👷 [워커-{worker_id}] ⚠️ [{stream_id}] 분석 실패/스킵 처리 완료.")

        return True 

    except Exception as e:
        if conn2: conn2.rollback()
        logging.error(f"👷 [워커-{worker_id}] 💥 DB 저장 중 에러: {e}")
        return False
    finally:
        if conn2:
            cur2.close()
            conn2.close()

# ==========================================
# 5. 다중 워커 실행부 (알바생 무한 루프)
# ==========================================
def worker_loop(worker_id):
    """개별 알바생(쓰레드)이 무한 반복할 작업"""
    time.sleep(worker_id * 2)
    logging.info(f"👷 [워커-{worker_id}] 업무 대기 중...")
    while True:
        has_work = process_queue(worker_id)
        if not has_work:
            time.sleep(15) # 큐에 일 없으면 15초 대기
        else:
            time.sleep(2)  # 연속으로 일할 땐 유튜브 API 과부하 방지로 2초 휴식

if __name__ == "__main__":
    logging.info("🚀 [다중 하이라이트 공장] 가동 시작...")
    
    # 🌟 동시에 돌릴 알바생 수 (원하시는 만큼 늘려도 됩니다. 기본 3명)
    WORKER_COUNT = 3 
    
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        for i in range(WORKER_COUNT):
            executor.submit(worker_loop, i + 1)