import argparse
import sys
from datetime import datetime, timezone, timedelta
from psycopg2.extras import execute_values
from core.logger import get_logger
from core.db import get_db_cursor

logger = get_logger("CompressStats")

def detect_timestamp_column(cur):
    """stream_stats 테이블의 시간 타임스탬프 컬럼명 자동 감지"""
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'oshilive' AND table_name = 'stream_stats'
          AND column_name IN ('collected_at', 'recorded_at', 'created_at', 'timestamp', 'created_time');
    """)
    res = cur.fetchone()
    return res[0] if res else 'collected_at'

def run_compress_stats(days: int = 2, bucket_min: int = 15, execute: bool = False):
    mode_str = "실제 압축 실행 (EXECUTE)" if execute else "Dry-Run 조회 전용"
    logger.info("=" * 60)
    logger.info(f"🚀 OshiLive stream_stats 압축 배치 시작")
    logger.info(f"   - 실행 모드: [{mode_str}]")
    logger.info(f"   - 보존 기간: 종료 후 {days}일 미만 방송은 1분 원본 유지")
    logger.info(f"   - 압축 간격: 종료 후 {days}일 이상 지난 방송은 {bucket_min}분 단위 피크값으로 축약")
    logger.info("=" * 60)

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        with get_db_cursor() as cur:
            ts_col = detect_timestamp_column(cur)
            logger.info(f"🔍 감지된 타임스탬프 컬럼: [{ts_col}]")

            # 압축 대상 방송 조회 (종료된 방송 중 cutoff_date 이전)
            cur.execute("""
                SELECT stream_id, title, end_actual 
                FROM oshilive.streams 
                WHERE status = 'live' AND end_actual <= %s;
            """, (cutoff_date,))
            target_streams = cur.fetchall()

            if not target_streams:
                logger.info("ℹ️ 압축 대상 과거 방송 데이터가 없습니다.")
                return

            logger.info(f"📦 총 {len(target_streams)}개 과거 방송 압축 진행 중...")

            for s_id, title, end_time in target_streams:
                # 다운샘플링 피크 데이터 추출
                downsample_query = f"""
                    SELECT 
                        stream_id,
                        to_timestamp(floor(extract(epoch from {ts_col}) / %s) * %s) AT TIME ZONE 'UTC' as bucket_ts,
                        MAX(viewer_count) as peak_viewers
                    FROM oshilive.stream_stats
                    WHERE stream_id = %s
                    GROUP BY stream_id, bucket_ts
                    ORDER BY bucket_ts;
                """
                bucket_sec = bucket_min * 60
                cur.execute(downsample_query, (bucket_sec, bucket_sec, s_id))
                sampled_rows = cur.fetchall()

                if not sampled_rows:
                    continue

                if execute:
                    # 원본 삭제 후 축소 데이터 임서트
                    cur.execute(f"DELETE FROM oshilive.stream_stats WHERE stream_id = %s;", (s_id,))
                    insert_sql = f"""
                        INSERT INTO oshilive.stream_stats (stream_id, {ts_col}, viewer_count)
                        VALUES %s;
                    """
                    execute_values(cur, insert_sql, sampled_rows, page_size=200)

            logger.info(f"✅ stream_stats 시청자 데이터 {len(target_streams)}개 방송 압축 완료!")
    except Exception as e:
        logger.error(f"❌ stream_stats 압축 실패: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OshiLive stream_stats 압축 배치")
    parser.add_argument("--days", type=int, default=2, help="N일 이상 지난 과거 방송 대상")
    parser.add_argument("--bucket-min", type=int, default=15, help="압축 간격(분)")
    parser.add_argument("--execute", action="store_true", help="실제 실행 여부")
    args = parser.parse_args()

    run_compress_stats(days=args.days, bucket_min=args.bucket_min, execute=args.execute)
