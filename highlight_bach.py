import psycopg2
from psycopg2 import pool
from psycopg2.extras import Json, execute_values
import logging
import os
import time
import pytchat
from pytchat.exceptions import ChatDataFinished
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
    WORKER_COUNT = 3
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
# sys.stdout.reconfigure(line_buffering=True) 대신 python -u 옵션 사용 권장
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 파일 핸들러 (로그를 highlight_batch.log 파일에 저장)
file_handler = logging.FileHandler('highlight_batch.log', encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

# 콘솔 핸들러 (화면 출력 최소화)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler],
    force=True  # 기존 설정 초기화 후 재설정
)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.CRITICAL)
logging.getLogger("httpcore").setLevel(logging.CRITICAL)

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
            # retry_count 컬럼 자동 마이그레이션
            try:
                conn = cls._pool.getconn()
                with conn.cursor() as cur:
                    cur.execute("ALTER TABLE oshilive.highlight_batch_tasks ADD COLUMN IF NOT EXISTS retry_count smallint DEFAULT 0;")
                conn.commit()
                cls._pool.putconn(conn)
                logging.info("[DB] retry_count 컬럼 마이그레이션 확인 완료")
            except Exception as e:
                logging.warning(f"[DB] retry_count 마이그레이션 스킵: {e}")

    @classmethod
    def get_connection(cls):
        """끊긴 연결을 감지하고 필요 시 재연결하여 안전한 연결을 반환합니다."""
        if not cls._pool:
            cls.initialize()
        
        try:
            conn = cls._pool.getconn()
            # 간단한 쿼리로 연결 상태 확인
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except Exception:
            logging.warning("[DB] 연결 유실 감지, 커넥션 풀 재초기화 중...")
            try:
                if cls._pool: cls._pool.closeall()
            except: pass
            cls._pool = None
            cls.initialize()
            return cls._pool.getconn()

    @classmethod
    def release_connection(cls, conn):
        if cls._pool and conn:
            try:
                # 풀에 반환하기 전에 혹시 남아있을 수 있는 트랜잭션/락 상태를 깨끗이 정리합니다.
                try:
                    conn.rollback()
                except Exception:
                    pass
                cls._pool.putconn(conn)
            except:
                pass

    @classmethod
    def reset_stuck_tasks(cls):
        """프로그램 시작 시 '처리중(2)' 상태로 멈춘 유령 작업들을 '대기(0)'로 되돌립니다."""
        conn = cls.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 0 WHERE status = 2;")
            count = cur.rowcount
            if count > 0:
                logging.info(f"[DB] 유령 작업 {count}개를 대기 상태로 복구했습니다.")
            conn.commit()
        finally:
            cls.release_connection(conn)

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
        
        last_continuation = None
        consecutive_errors = 0
        max_consecutive_errors = 5
        empty_retry = 0
        last_logged_min = -1
        chat = None
        pending_error = None

        try:
            chat = pytchat.create(video_id=self.stream_id, interruptable=False)
            
            while True:
                # 만약 chat이 활성화되어 있지 않거나 에러가 있으면 재연결 시도
                if chat is None or not chat.is_alive() or pending_error is not None:
                    err = pending_error
                    pending_error = None  # 소비했으므로 초기화
                    
                    if err is None and chat is not None:
                        try:
                            chat.raise_for_status()
                        except ChatDataFinished:
                            logging.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                            break
                        except Exception as e:
                            err = e
                    
                    if err is not None:
                        # ChatDataFinished는 정상 완료 신호 → 즉시 성공 처리
                        if isinstance(err, ChatDataFinished):
                            logging.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                            break
                        
                        # 에러가 발생해서 종료된 경우 -> 재연결 시도
                        if last_continuation:
                            consecutive_errors += 1
                            if consecutive_errors > max_consecutive_errors:
                                logging.error(f"[{self.stream_id}] 연속 오류 횟수 초과 ({consecutive_errors}회) → 분석 실패 처리")
                                raise err
                            
                            logging.warning(f"[{self.stream_id}] 재연결 시도 중... ({consecutive_errors}/{max_consecutive_errors})")
                            time.sleep(5)
                            try:
                                chat = pytchat.create(video_id=self.stream_id, replay_continuation=last_continuation, interruptable=False)
                                continue
                            except ChatDataFinished:
                                logging.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                                break
                            except Exception as reconnect_err:
                                logging.error(f"[{self.stream_id}] 재연결 실패: {reconnect_err}")
                                pending_error = reconnect_err
                                continue
                        else:
                            # continuation이 없는 상태에서 처음부터 에러 발생
                            raise err
                    else:
                        # 에러 없이 정상적으로 종료된 경우 -> 완료!
                        logging.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (더 이상 데이터 없음)")
                        break

                try:
                    # sync_items() 대신 items를 사용하여 실시간 대기 없이 즉시 모든 데이터 수집
                    data = chat.get()
                    if data is None:
                        empty_retry += 1
                        if empty_retry >= 20: 
                            logging.warning(f"[{self.stream_id}] 20회 연속 데이터 없음 -> 분석 조기 종료")
                            break
                        time.sleep(5) 
                        continue
                    
                    items = data.items
                    if not items:
                        empty_retry += 1
                        if empty_retry >= 20: 
                            logging.warning(f"[{self.stream_id}] 20회 연속 빈 데이터 -> 분석 조기 종료")
                            break
                        time.sleep(5)
                        continue
                except ChatDataFinished:
                    logging.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                    break
                except Exception as e:
                    logging.warning(f"[{self.stream_id}] 데이터 수집 오류 ({type(e).__name__}). 재연결 대기 중...")
                    time.sleep(5)
                    if chat:
                        try: chat.terminate()
                        except: pass
                    pending_error = e
                    continue

                # 정상적으로 데이터를 수집했으므로 연속 에러 횟수 및 빈 데이터 횟수 초기화
                consecutive_errors = 0
                empty_retry = 0

                # 마지막 continuation 파라미터 업데이트
                if chat.continuation:
                    last_continuation = chat.continuation
                
                for c in items:
                    if c is None: continue
                    try:
                        self.msg_count += 1
                        # 안전하게 속성 가져오기
                        msg = getattr(c, 'message', '')
                        elapsed = getattr(c, 'elapsedTime', '')
                        
                        if not elapsed: continue
                        sec = self._parse_time(elapsed)
                        if sec > self.total_duration: self.total_duration = sec

                        # 점수 계산 (기본 1점 + 키워드 가중치)
                        score = 1.0
                        if msg:
                            for kw, weight in Config.KEYWORDS.items():
                                if kw in msg:
                                    score += weight

                        bucket_sec = (sec // 30) * 30
                        if bucket_sec not in self.timeline_buckets:
                            self.timeline_buckets[bucket_sec] = {"messages": 0, "score": 0.0}
                        
                        self.timeline_buckets[bucket_sec]["messages"] += 1
                        self.timeline_buckets[bucket_sec]["score"] += score

                        # 진행 상황 로그 (파일에만 기록됨)
                        current_min = sec // 60
                        if current_min > last_logged_min:
                            logging.info(f"[{self.stream_id}] 수집 중... ({current_min}분 지점 / 메시지 {self.msg_count:,}개)")
                            last_logged_min = current_min
                    except Exception as e:
                        # 개별 메시지 처리 실패 시 로그만 남기고 다음 메시지로 진행
                        logging.debug(f"[{self.stream_id}] 메시지 처리 스킵: {e}")
                        continue
                # [트래픽/CPU 최적화] 데이터 요청 한 번당 1초 지연
                time.sleep(1)
            
            return self._finalize_data()

        except ChatDataFinished:
            # 안전장치: 루프 밖에서 ChatDataFinished가 잡힌 경우에도 정상 완료 처리
            logging.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
            return self._finalize_data()

        except Exception as e:
            err_msg = str(e)
            err_type = type(e).__name__
            # 무효한 영상, 비공개, 채팅 불가능 등의 영구적인 에러는 None, 0을 반환하여 status = 9(스킵) 처리
            if "Cannot find channel id" in err_msg or "NoChatData" in err_type or "InvalidVideo" in err_type or "Unavailable" in err_type:
                logging.error(f"[{self.stream_id}] 무효한 영상 또는 차단됨/채팅 없음: {err_msg} ({err_type})")
                return None, 0
            
            # 네트워크 끊김 등 일시적인 에러는 예외를 위로 던져 retry_count 기반 재시도/스킵 처리
            logging.error(f"[{self.stream_id}] 분석 실패 ({err_type}): {e}")
            raise e

    def _parse_time(self, time_str):
        if not time_str: return 0
        try:
            parts = time_str.replace("-", "").split(":")
            # 빈 부분 제거 및 숫자로 변환 가능한 것만 필터링
            parts = [p for p in parts if p.strip()]
            if not parts: return 0
            
            if len(parts) == 3:
                return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
            elif len(parts) == 2:
                return int(parts[0])*60 + int(parts[1])
            return int(parts[0])
        except Exception:
            return 0

    def _finalize_data(self):
        if not self.timeline_buckets: return [], 0
        
        # 30초 단위 데이터를 1분 단위로 합산 (프론트엔드 그래프 호환성 유지)
        minute_buckets = {}
        for k, v in self.timeline_buckets.items():
            min_sec = (k // 60) * 60
            if min_sec not in minute_buckets:
                minute_buckets[min_sec] = {"messages": 0, "score": 0.0}
            minute_buckets[min_sec]["messages"] += v["messages"]
            minute_buckets[min_sec]["score"] += v["score"]

        timeline_data = [
            {"time_sec": k, "messages": v["messages"], "score": round(v["score"], 2)} 
            for k, v in minute_buckets.items()
        ]
        timeline_data.sort(key=lambda x: x["time_sec"])
        
        logging.info(f"[{self.stream_id}] 분석 완료! (총 {self.total_duration // 60}분)")
        return timeline_data, self.total_duration

    def extract_segments(self, timeline_data=None):
        """30초 버킷 데이터를 기반으로 정밀한 하이라이트 구간(60초)을 추출합니다."""
        if not self.timeline_buckets: return []

        # 30초 단위 데이터를 리스트로 변환 및 정렬
        buckets_30s = [
            {"time_sec": k, "messages": v["messages"], "score": round(v["score"], 2)}
            for k, v in self.timeline_buckets.items()
        ]
        buckets_30s.sort(key=lambda x: x["time_sec"])

        # 평균 점수 기반 필터링 (30초 단위 평균 점수 사용)
        avg_score = sum(item["score"] for item in buckets_30s) / len(buckets_30s)
        peaks = [item for item in buckets_30s if item["score"] >= (avg_score * Config.THRESHOLD_MULTIPLIER)]
        peaks.sort(key=lambda x: x["score"], reverse=True)

        selected_segments = []
        for peak in peaks:
            if len(selected_segments) >= Config.HIGHLIGHT_COUNT: break
            
            p_time = peak["time_sec"]      # 30초 피크 버킷의 시작 시각
            start = max(0, p_time - 15)   # 앞 15초 추가 빌드업
            end = p_time + 45             # 피크 30초 + 뒤 15초 = 총 60초 (1분) 클립 완성
            
            # 이미 선택된 구간과 겹치는지 확인 (최소 1분 간격 유지)
            is_overlap = False
            for seg in selected_segments:
                if not (end < seg["start"] - 30 or start > seg["end"] + 30):
                    is_overlap = True
                    break
            
            if not is_overlap:
                # 미니 차트는 30초 단위 버킷에서 추출
                mini_chart = [item["messages"] for item in buckets_30s if start <= item["time_sec"] <= end]
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

    def handle_task_failure(self, stream_id):
        """작업 실패 시 retry_count를 확인하여 재시도 또는 영구 실패(status=9) 처리합니다."""
        conn = DatabaseManager.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT retry_count FROM oshilive.highlight_batch_tasks WHERE stream_id = %s;", (stream_id,))
            row = cur.fetchone()
            current_retry = (row[0] or 0) if row else 0
            
            if current_retry >= 3:
                # 3회 이상 실패 → 분석 불가능한 영상으로 판단, 영구 스킵
                cur.execute("""
                    UPDATE oshilive.highlight_batch_tasks 
                    SET status = 9, updated_at = CURRENT_TIMESTAMP 
                    WHERE stream_id = %s;
                """, (stream_id,))
                conn.commit()
                logging.warning(f"[워커-{self.worker_id}] [{stream_id}] 재시도 한도 초과 ({current_retry}회) → 분석 불가(status=9) 처리")
            else:
                # 재시도 횟수 증가 후 대기 상태로 복구
                cur.execute("""
                    UPDATE oshilive.highlight_batch_tasks 
                    SET status = 0, retry_count = retry_count + 1, updated_at = CURRENT_TIMESTAMP 
                    WHERE stream_id = %s;
                """, (stream_id,))
                conn.commit()
                logging.info(f"[워커-{self.worker_id}] [{stream_id}] 재시도 대기 ({current_retry + 1}/3)")
        except Exception as e:
            logging.error(f"[워커-{self.worker_id}] [{stream_id}] 상태 업데이트 실패: {e}")
        finally:
            DatabaseManager.release_connection(conn)

    def run(self):
        logging.info(f"[워커-{self.worker_id}] 업무 대기 중...")
        time.sleep(self.worker_id * 2) # 시작 시간 분산
        
        while True:
            try:
                self.process_next_task()
            except psycopg2.InterfaceError:
                logging.error(f"[워커-{self.worker_id}] DB 인터페이스 에러 - 재시도 대기")
                time.sleep(30)
            except Exception as e:
                logging.error(f"[워커-{self.worker_id}] 예상치 못한 오류 발생: {e}")
                time.sleep(10)
            
            time.sleep(10)

    def process_next_task(self):
        conn = DatabaseManager.get_connection()
        stream_id = None
        retry_count = 0
        try:
            cur = conn.cursor()
            # 작업 하나 가져오기 (30분 유예 시간 조건 및 retry_count 함께 조회)
            cur.execute("""
                SELECT t.stream_id, t.retry_count FROM oshilive.highlight_batch_tasks t
                LEFT JOIN oshilive.streams s ON t.stream_id = s.stream_id
                WHERE t.status = 0 
                  AND (s.end_actual IS NULL OR s.end_actual <= NOW() - INTERVAL '30 minutes')
                ORDER BY t.created_at ASC 
                LIMIT 1 FOR UPDATE SKIP LOCKED;
            """)
            row = cur.fetchone()
            
            if not row:
                return
                
            stream_id = row[0]
            retry_count = (row[1] or 0) if row else 0
            cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 2, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
            conn.commit()
            
            # 작업을 시작할 때만 현황 로그를 출력하도록 개선
            stats = DatabaseManager.get_queue_stats()
            logging.info(f"[워커-{self.worker_id}] 작업 시작: {stream_id} (대기: {stats['pending']}, 처리중: {stats['processing']}, 완료: {stats['completed']})")
            
        finally:
            DatabaseManager.release_connection(conn)

        if not stream_id: return

        try:
            # 분석 실행
            analyzer = HighlightAnalyzer(stream_id)
            timeline_data, duration = analyzer.analyze()
            
            # 결과 저장
            self.save_results(stream_id, timeline_data, duration, analyzer, retry_count)
        except Exception as e:
            logging.error(f"[워커-{self.worker_id}] [{stream_id}] 작업 실패: {e}")
            self.handle_task_failure(stream_id)
            time.sleep(60)  # 유튜브 레이트 리밋 완화를 위한 대기시간 추가

    def save_results(self, stream_id, timeline_data, duration, analyzer, retry_count=0):
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
                
                cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 1, retry_count = 0, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
                logging.info(f"[워커-{self.worker_id}] 성공: [{stream_id}] 분석 및 저장 완료!")
            else:
                if retry_count < 3:
                    # 아직 재시도 횟수가 남았다면 예외를 발생시켜 handle_task_failure에서 재시도 하도록 유도
                    raise ValueError(f"분석 데이터가 비어 있습니다 (retry_count: {retry_count}). 유튜브 채팅 인코딩 지연 가능성.")
                else:
                    cur.execute("UPDATE oshilive.highlight_batch_tasks SET status = 9, updated_at = CURRENT_TIMESTAMP WHERE stream_id = %s;", (stream_id,))
                    logging.warning(f"[워커-{self.worker_id}] 스킵: [{stream_id}] 3회 재시도에도 분석 데이터가 없습니다.")
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            if isinstance(e, ValueError) and "분석 데이터가 비어 있습니다" in str(e):
                raise e
            logging.error(f"[워커-{self.worker_id}] 결과 저장 중 오류 발생: {e}")
        finally:
            DatabaseManager.release_connection(conn)

# ==========================================
# 5. 메인 실행부
# ==========================================
if __name__ == "__main__":
    DatabaseManager.initialize()
    DatabaseManager.reset_stuck_tasks() # 멈춘 작업 자동 복구
    logging.info(f"하이라이트 배치 시스템 v2 시작 (워커 수: {Config.WORKER_COUNT})")
    
    with ThreadPoolExecutor(max_workers=Config.WORKER_COUNT) as executor:
        for i in range(Config.WORKER_COUNT):
            worker = HighlightWorker(i + 1)
            executor.submit(worker.run)