import psycopg2
from psycopg2 import pool
from psycopg2.extras import Json, execute_values
import logging
import os
import time
import pytchat
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# 1. 설정 및 환경 변수
# ==========================================
load_dotenv()

class Config:
    DB_CONFIG = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": os.getenv("DB_PORT")
    }
    WORKER_COUNT = 2
    HIGHLIGHT_COUNT = 5
    THRESHOLD_MULTIPLIER = 2.0
    
    # 하이라이트 가중치 키워드 (일본어 방송 기준)
    KEYWORDS = {
        "w": 0.5,
        "笑": 0.5,
        "草": 0.8,
        "888": 0.5,
        "きた": 1.0,
        "きちゃ": 1.0,
        "おめ": 1.0,
        "!": 0.2,
        "?": 0.2,
        "かわいい": 1.0,
        "てぇてぇ": 1.5,
        "たすかる": 1.2,
        "神": 1.5
    }

# 로깅 설정 (콘솔 + 파일)
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 파일 핸들러 (로그를 highlight_batch.log 파일에 저장)
file_handler = logging.FileHandler('highlight_batch.log', encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

# 콘솔 핸들러 (화면 출력은 중요한 것만)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.WARNING) # 콘솔은 WARNING 이상만 출력하여 SSH 트래픽 절감

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# ==========================================
# 2. 데이터베이스 매니저 (커넥션 풀 관리)
# ==========================================
class DatabaseManager:
    _pool = None

    @classmethod
    def initialize(cls):
        if not cls._pool:
            logging.info("[DB] 커넥션 풀 초기화 중...")
            cls._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=Config.WORKER_COUNT + 2,
                **Config.DB_CONFIG
            )

    @classmethod
    def get_connection(cls):
        return cls._pool.getconn()

    @classmethod
    def release_connection(cls, conn):
        cls._pool.putconn(conn)

    @classmethod
    def get_queue_stats(cls):
        """현재 큐의 상태를 확인합니다."""
        conn = cls.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE status = 0) as pending,
                    COUNT(*) FILTER (WHERE status = 2) as processing,
                    COUNT(*) FILTER (WHERE status = 1) as completed
                FROM oshilive.highlight_batch_tasks;
            """)
            row = cur.fetchone()
            return {"pending": row[0], "processing": row[1], "completed": row[2]}
        finally:
            cls.release_connection(conn)

# ==========================================
# 3. 하이라이트 분석 엔진
# ==========================================
class HighlightAnalyzer:
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.timeline_buckets = {}  # {time_sec: {"count": N, "score": S}}
        self.total_duration = 0
        self.msg_count = 0

    def analyze(self):
        """유튜브 채팅을 수집하고 점수를 계산합니다."""
        logging.info(f"[{self.stream_id}] 채팅 데이터 분석 시작...")
        try:
            chat = pytchat.create(video_id=self.stream_id, interruptable=False)
            empty_retry = 0
            last_logged_min = -1

            while chat.is_alive():
                items = chat.get().sync_items()
                if not items:
                    empty_retry += 1
                    if empty_retry >= 15: break
                    time.sleep(2)
                    continue
                
                empty_retry = 0
                for c in items:
                    self.msg_count += 1
                    sec = self._parse_time(c.elapsedTime)
                    if sec > self.total_duration: self.total_duration = sec

                    # 점수 계산 (기본 1점 + 키워드 가중치)
                    score = 1.0
                    for kw, weight in Config.KEYWORDS.items():
                        if kw in c.message:
                            score += weight

                    bucket_sec = (sec // 60) * 60
                    if bucket_sec not in self.timeline_buckets:
                        self.timeline_buckets[bucket_sec] = {"messages": 0, "score": 0.0}
                    
                    self.timeline_buckets[bucket_sec]["messages"] += 1
                    self.timeline_buckets[bucket_sec]["score"] += score

                    # 진행 상황 로그 (파일에만 기록됨)
                    current_min = sec // 60
                    if current_min > last_logged_min:
                        logging.info(f"[{self.stream_id}] 수집 중... ({current_min}분 지점 / 메시지 {self.msg_count:,}개)")
                        last_logged_min = current_min
                time.sleep(1)
            return self._finalize_data()

        except Exception as e:
            logging.error(f"[{self.stream_id}] 분석 중 오류 발생: {e}")
            return None, 0

    def _parse_time(self, time_str):
        parts = time_str.replace("-", "").split(":")
        if len(parts) == 3:
            return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0])*60 + int(parts[1])
        return int(parts[0])

    def _finalize_data(self):
        if not self.timeline_buckets: return [], 0
        
        timeline_data = [
            {"time_sec": k, "messages": v["messages"], "score": round(v["score"], 2)} 
            for k, v in self.timeline_buckets.items()
        ]
        timeline_data.sort(key=lambda x: x["time_sec"])
        
        logging.info(f"[{self.stream_id}] 분석 완료! (총 {self.total_duration // 60}분)")
        return timeline_data, self.total_duration

    def extract_segments(self, timeline_data):
        """점수가 높은 하이라이트 구간을 추출합니다. (중복 방지 로직 포함)"""
        if not timeline_data: return []

        # 평균 점수 기반 필터링
        avg_score = sum(item["score"] for item in timeline_data) / len(timeline_data)
        peaks = [item for item in timeline_data if item["score"] >= (avg_score * Config.THRESHOLD_MULTIPLIER)]
        peaks.sort(key=lambda x: x["score"], reverse=True)

        selected_segments = []
        for peak in peaks:
            if len(selected_segments) >= Config.HIGHLIGHT_COUNT: break
            
            p_time = peak["time_sec"]
            start = max(0, p_time - 30)
            end = p_time + 90
            
            # 이미 선택된 구간과 겹치는지 확인 (최소 1분 간격 유지)
            is_overlap = False
            for seg in selected_segments:
                if not (end < seg["start"] - 30 or start > seg["end"] + 30):
                    is_overlap = True
                    break
            
            if not is_overlap:
                mini_chart = [item["messages"] for item in timeline_data if start <= item["time_sec"] <= end]
                selected_segments.append({
                    "start": start,
                    "end": end,
                    "score": peak["score"],
                    "mini_chart": mini_chart
                })

        return selected_segments

# ==========================================
# 4. 워커 클래스 (작업 관리)
# ==========================================
class HighlightWorker:
    def __init__(self, worker_id):
        self.worker_id = worker_id

    def run(self):
        logging.info(f"[워커-{self.worker_id}] 업무 대기 중...")
        time.sleep(self.worker_id * 2) # 시작 시간 분산
        
        while True:
            try:
                self.process_next_task()
            except Exception as e:
                logging.error(f"[워커-{self.worker_id}] 치명적 오류 발생: {e}")
            
            time.sleep(10)

    def process_next_task(self):
        # 작업 현황 로그 (진행 중인 작업이 있을 때만 가끔 출력)
        stats = DatabaseManager.get_queue_stats()
        total_remaining = stats['pending'] + stats['processing']
        if total_remaining > 0:
            logging.info(f"[워커-{self.worker_id}] 현황: 대기({stats['pending']}), 처리중({stats['processing']}), 완료({stats['completed']}) -> 남은 작업: {total_remaining}")

        conn = DatabaseManager.get_connection()
        stream_id = None
        try:
            cur = conn.cursor()
            # 작업 하나 가져오기
            cur.execute("""
                SELECT stream_id FROM oshilive.highlight_batch_tasks 
                WHERE status = 0 
                ORDER BY created_at ASC 
                LIMIT 1 FOR UPDATE SKIP LOCKED;
            """)
            row = cur.fetchone()
            
            if not row:
                return
                
            stream_id = row[0]
            cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 2, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
            conn.commit()
            logging.info(f"[워커-{self.worker_id}] 작업 시작: {stream_id}")
            
        finally:
            DatabaseManager.release_connection(conn)

        if not stream_id: return

        # 분석 실행
        analyzer = HighlightAnalyzer(stream_id)
        timeline_data, duration = analyzer.analyze()
        
        # 결과 저장
        self.save_results(stream_id, timeline_data, duration, analyzer)

    def save_results(self, stream_id, timeline_data, duration, analyzer):
        conn = DatabaseManager.get_connection()
        try:
            cur = conn.cursor()
            if timeline_data:
                segments = analyzer.extract_segments(timeline_data)
                
                # 1. 전체 하이라이트 통계 저장
                cur.execute("""
                    INSERT INTO oshilive.stream_highlights (stream_id, duration_sec, timeline_data)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (stream_id) DO UPDATE SET
                        duration_sec = EXCLUDED.duration_sec,
                        timeline_data = EXCLUDED.timeline_data,
                        updated_at = CURRENT_TIMESTAMP;
                """, (stream_id, duration, Json(timeline_data)))
                
                # 2. 개별 세그먼트 저장
                if segments:
                    cur.execute("DELETE FROM oshilive.highlight_segments WHERE stream_id = %s;", (stream_id,))
                    segment_values = [
                        (stream_id, seg["start"], seg["end"], Json(seg["mini_chart"]))
                        for seg in segments
                    ]
                    execute_values(cur, "INSERT INTO oshilive.highlight_segments (stream_id, start_time_sec, end_time_sec, mini_chart_data) VALUES %s;", segment_values)
                
                cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 1, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
                logging.info(f"[워커-{self.worker_id}] 성공: [{stream_id}] 분석 및 저장 완료!")
            else:
                cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 9, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
                logging.warning(f"[워커-{self.worker_id}] 스킵: [{stream_id}] 분석 데이터가 없습니다.")
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"[워커-{self.worker_id}] 결과 저장 중 오류 발생: {e}")
        finally:
            DatabaseManager.release_connection(conn)

# ==========================================
# 5. 메인 실행부
# ==========================================
if __name__ == "__main__":
    DatabaseManager.initialize()
    logging.info(f"하이라이트 배치 시스템 v2 시작 (워커 수: {Config.WORKER_COUNT})")
    
    with ThreadPoolExecutor(max_workers=Config.WORKER_COUNT) as executor:
        for i in range(Config.WORKER_COUNT):
            worker = HighlightWorker(i + 1)
            executor.submit(worker.run)