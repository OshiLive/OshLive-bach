import gc
import json
import time
from concurrent.futures import ThreadPoolExecutor
import pytchat
from pytchat.exceptions import ChatDataFinished
from core.config import Config
from core.logger import get_logger
from core.db import get_db_cursor, get_db_connection

logger = get_logger("HighlightDaemon")

# ==========================================
# 1. 하이라이트 가중치 키워드 및 설정
# ==========================================
WORKER_COUNT = 3
HIGHLIGHT_COUNT = 5
THRESHOLD_MULTIPLIER = 2.0

KEYWORDS = {
    "w": 1.0,
    "笑": 1.0,
    "草": 1.5,
    "888": 0.2,
    "きた": 0.3,
    "きちゃ": 0.3,
    "おめ": 1.0,
    "!": 0.2,
    "?": 0.3,
    "!?": 1.5,
    "????": 1.5,
    "かわいい": 1.0,
    "てぇてぇ": 1.5,
    "たすかる": 1.5,
    "神": 2.0
}

# ==========================================
# 2. 하이라이트 분석 엔진
# ==========================================
class HighlightAnalyzer:
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.timeline_buckets = {}  # {time_sec: {"messages": N, "score": S}}
        self.total_duration = 0
        self.msg_count = 0

    def analyze(self):
        """유튜브 채팅 데이터를 수집하고 30초 단위 버킷 기반 점수를 계산"""
        logger.info(f"[{self.stream_id}] 채팅 데이터 분석 수집 시작")

        last_continuation = None
        consecutive_errors = 0
        max_consecutive_errors = 5
        empty_retry = 0
        chat = None
        pending_error = None

        try:
            chat = pytchat.create(video_id=self.stream_id, interruptable=False)

            while True:
                if chat is None or not chat.is_alive() or pending_error is not None:
                    err = pending_error
                    pending_error = None

                    if err is None and chat is not None:
                        try:
                            chat.raise_for_status()
                        except ChatDataFinished:
                            logger.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                            break
                        except Exception as e:
                            err = e

                    if err is not None:
                        if isinstance(err, ChatDataFinished):
                            logger.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                            break

                        if last_continuation:
                            consecutive_errors += 1
                            if consecutive_errors > max_consecutive_errors:
                                logger.error(f"[{self.stream_id}] 연속 오류 횟수 초과 ({consecutive_errors}회)")
                                raise err

                            logger.warning(f"[{self.stream_id}] 재연결 시도 중... ({consecutive_errors}/{max_consecutive_errors})")
                            time.sleep(5)
                            try:
                                chat = pytchat.create(video_id=self.stream_id, replay_continuation=last_continuation, interruptable=False)
                                continue
                            except ChatDataFinished:
                                logger.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                                break
                            except Exception as reconnect_err:
                                pending_error = reconnect_err
                                continue
                        else:
                            raise err
                    else:
                        logger.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (데이터 끝)")
                        break

                try:
                    data = chat.get()
                    if data is None:
                        empty_retry += 1
                        if empty_retry >= 20:
                            logger.warning(f"[{self.stream_id}] 20회 연속 빈 데이터로 조기 종료")
                            break
                        time.sleep(5)
                        continue

                    items = data.items
                    if not items:
                        empty_retry += 1
                        if empty_retry >= 20:
                            logger.warning(f"[{self.stream_id}] 20회 연속 데이터 없음으로 조기 종료")
                            break
                        time.sleep(5)
                        continue
                except ChatDataFinished:
                    logger.info(f"[{self.stream_id}] 채팅 수집 정상 완료 (ChatDataFinished)")
                    break
                except Exception as e:
                    time.sleep(5)
                    if chat:
                        try: chat.terminate()
                        except: pass
                    pending_error = e
                    continue

                consecutive_errors = 0
                empty_retry = 0

                if chat.continuation:
                    last_continuation = chat.continuation

                for c in items:
                    if c is None: continue
                    msg = getattr(c, 'message', '')
                    elapsed = getattr(c, 'elapsedTime', '')
                    if not elapsed: continue

                    sec = self._parse_time(elapsed)
                    if sec > self.total_duration:
                        self.total_duration = sec

                    score = 1.0
                    if msg:
                        for kw, weight in KEYWORDS.items():
                            if kw in msg:
                                score += weight

                    bucket_sec = (sec // 30) * 30
                    if bucket_sec not in self.timeline_buckets:
                        self.timeline_buckets[bucket_sec] = {"messages": 0, "score": 0.0}

                    self.timeline_buckets[bucket_sec]["messages"] += 1
                    self.timeline_buckets[bucket_sec]["score"] += score

        finally:
            if chat:
                try: chat.terminate()
                except: pass

        return self.calculate_highlights()

    def _parse_time(self, elapsed_str):
        try:
            parts = elapsed_str.split(':')
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            elif len(parts) == 1:
                return int(parts[0])
        except Exception:
            pass
        return 0

    def calculate_highlights(self):
        if not self.timeline_buckets:
            return {"timeline": [], "highlights": []}

        timeline = []
        for sec in sorted(self.timeline_buckets.keys()):
            b = self.timeline_buckets[sec]
            timeline.append({
                "time_sec": sec,
                "timestamp_sec": sec,
                "messages": b["messages"],
                "score": round(b["score"], 2)
            })

        scores = [b["score"] for b in self.timeline_buckets.values()]
        avg_score = sum(scores) / len(scores) if scores else 0
        threshold = max(avg_score * THRESHOLD_MULTIPLIER, 5.0)

        candidates = [t for t in timeline if t["score"] >= threshold]
        candidates.sort(key=lambda x: x["score"], reverse=True)

        selected_highlights = []
        for cand in candidates:
            c_sec = cand["timestamp_sec"]
            # 2분 이내 중복 하이라이트 필터링
            if any(abs(c_sec - h["timestamp_sec"]) < 120 for h in selected_highlights):
                continue
            selected_highlights.append(cand)
            if len(selected_highlights) >= HIGHLIGHT_COUNT:
                break

        selected_highlights.sort(key=lambda x: x["timestamp_sec"])
        return {
            "duration_sec": max(self.total_duration, 0),
            "timeline": timeline,
            "highlights": selected_highlights
        }

# ==========================================
# 3. 데이터베이스 작업 함수들
# ==========================================
# STATUS CONSTANTS (smallint)
STATUS_PENDING = 0
STATUS_COMPLETED = 1
STATUS_IN_PROGRESS = 2
STATUS_FAILED = 3

def fetch_pending_task():
    """상태가 PENDING(0)인 가장 오래된 태스크 1건 점유 (FOR UPDATE SKIP LOCKED)"""
    sql = """
    UPDATE oshilive.highlight_batch_tasks
    SET status = 2,
        retry_count = retry_count + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE stream_id = (
        SELECT stream_id
        FROM oshilive.highlight_batch_tasks
        WHERE status = 0
        ORDER BY created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING stream_id, retry_count;
    """
    try:
        with get_db_cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row:
                return {"stream_id": row[0], "retry_count": row[1]}
    except Exception as e:
        logger.error(f"태스크 팝 실패: {e}")
    return None

def complete_task(stream_id, result_json):
    """분석 완료 태스크 저장 및 stream_highlights, highlight_segments DB 반영"""
    sql_task = """
    UPDATE oshilive.highlight_batch_tasks
    SET status = 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE stream_id = %s;
    """
    
    sql_highlight = """
    INSERT INTO oshilive.stream_highlights (stream_id, duration_sec, peak_viewers, timeline_data, updated_at)
    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (stream_id) DO UPDATE SET
        duration_sec = EXCLUDED.duration_sec,
        peak_viewers = EXCLUDED.peak_viewers,
        timeline_data = EXCLUDED.timeline_data,
        updated_at = CURRENT_TIMESTAMP;
    """

    sql_delete_segments = """
    DELETE FROM oshilive.highlight_segments WHERE stream_id = %s;
    """

    sql_insert_segment = """
    INSERT INTO oshilive.highlight_segments (stream_id, start_time_sec, end_time_sec, recommend_count, created_at)
    VALUES (%s, %s, %s, 0, CURRENT_TIMESTAMP);
    """

    try:
        duration_sec = int(result_json.get("duration_sec") or 0)
        timeline_json = json.dumps(result_json.get("timeline", []), ensure_ascii=False)
        highlights = result_json.get("highlights", [])

        with get_db_cursor() as cur:
            # 1. 태스크 완료 처리 (status = 1)
            cur.execute(sql_task, (stream_id,))
            
            # 2. stream_highlights 타임라인 데이터 저장
            cur.execute(sql_highlight, (stream_id, duration_sec, 0, timeline_json))

            # 3. highlight_segments 구간 데이터 저장
            cur.execute(sql_delete_segments, (stream_id,))
            for h in highlights:
                start_sec = h.get("timestamp_sec", 0)
                end_sec = start_sec + 30
                cur.execute(sql_insert_segment, (stream_id, start_sec, end_sec))

        logger.info(f"✅ 스트림 [{stream_id}] 분석 완료 및 stream_highlights / segments DB 저장 성공! (하이라이트 {len(highlights)}개)")
    except Exception as e:
        logger.error(f"❌ 완료 처리 실패 (Stream ID: {stream_id}): {e}")

def retry_task(stream_id, error_msg):
    """3회 미만 일시 오류 발생 시 PENDING(0)으로 되돌려 재시도 대기"""
    sql = """
    UPDATE oshilive.highlight_batch_tasks
    SET status = 0,
        updated_at = CURRENT_TIMESTAMP
    WHERE stream_id = %s;
    """
    try:
        with get_db_cursor() as cur:
            cur.execute(sql, (stream_id,))
        logger.warning(f"⚠️ 스트림 [{stream_id}] 재시도 예정 (PENDING 복구): {error_msg[:100]}")
    except Exception as e:
        logger.error(f"재시도 처리 에러: {e}")

def fail_task(stream_id, error_msg):
    """3회 이상 최종 실패 시 FAILED(3) 처리"""
    sql = """
    UPDATE oshilive.highlight_batch_tasks
    SET status = 3,
        updated_at = CURRENT_TIMESTAMP
    WHERE stream_id = %s;
    """
    try:
        with get_db_cursor() as cur:
            cur.execute(sql, (stream_id,))
        logger.error(f"❌ 스트림 [{stream_id}] 최종 FAILED 처리 완료: {error_msg[:100]}")
    except Exception as e:
        logger.error(f"실패 처리 에러: {e}")

# ==========================================
# 4. 워커 프로세싱 및 메인 루프
# ==========================================
def worker_process():
    while True:
        task = fetch_pending_task()
        if not task:
            time.sleep(3)
            continue

        stream_id = task["stream_id"]
        retry_count = task["retry_count"]

        logger.info(f"🚀 워커 태스크 할당 Stream: {stream_id} (시도 횟수: {retry_count})")

        analyzer = HighlightAnalyzer(stream_id)
        try:
            result = analyzer.analyze()
            complete_task(stream_id, result)
        except Exception as e:
            err_msg = str(e)
            logger.error(f"❌ 분석 실패 [Stream: {stream_id}]: {err_msg}")
            if retry_count >= 3:
                fail_task(stream_id, f"Max retries ({retry_count}) exceeded: {err_msg}")
            else:
                retry_task(stream_id, f"Attempt {retry_count} failed: {err_msg}")
        finally:
            del analyzer
            gc.collect()

def run_daemon():
    logger.info(f"=== OshiLive 하이라이트 분석 데몬 시작 (Worker={WORKER_COUNT}) ===")
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        for _ in range(WORKER_COUNT):
            executor.submit(worker_process)

if __name__ == "__main__":
    run_daemon()