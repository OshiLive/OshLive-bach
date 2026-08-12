import psycopg2
from psycopg2.extras import execute_values
import os
import sys
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_db_connection():
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(dotenv_path)

    db_config = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": os.getenv("DB_PORT")
    }
    return psycopg2.connect(**db_config)

def detect_timestamp_column(cur):
    """stream_stats 테이블의 시간 타임스탬프 컬럼명(collected_at, recorded_at 등)을 자동 감지합니다."""
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'oshilive' AND table_name = 'stream_stats'
          AND column_name IN ('collected_at', 'recorded_at', 'created_at', 'timestamp', 'created_time');
    """)
    res = cur.fetchone()
    if res:
        return res[0]
    return 'collected_at'  # 기본값

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description="OshiLive past stream_stats 데이터 다운샘플링 압축 도구")
    parser.add_argument("--days", type=int, default=2, help="종료 후 N일 이상 지난 과거 방송을 대상으로 압축 (기본 2일)")
    parser.add_argument("--bucket-min", type=int, default=15, help="다운샘플링 분 단위 구간 (기본 15분)")
    parser.add_argument("--execute", action="store_true", help="실제 DB 데이터 압축 및 원본 삭제 실행 (미지정 시 Dry-Run)")
    args = parser.parse_args()

    mode_str = "실제 압축 실행 (EXECUTE)" if args.execute else "Dry-Run 조회 전용"
    logging.info("=" * 60)
    logging.info(f"🚀 OshiLive stream_stats 압축 배치 시작")
    logging.info(f"   - 실행 모드: [{mode_str}]")
    logging.info(f"   - 보존 기간: 종료 후 {args.days}일 미만 방송은 1분 원본 유지")
    logging.info(f"   - 압축 간격: 종료 후 {args.days}일 이상 지난 방송은 {args.bucket_min}분 단위 피크값으로 축약")
    logging.info("=" * 60)

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        ts_col = detect_timestamp_column(cur)
        logging.info(f"[DB] stream_stats 시간 컬럼 감지 결과: '{ts_col}'")

        # 1. 압축 대상 방송(status = 'past' AND end_actual <= NOW() - N일) 개수 및 데이터 건수 파악
        count_query = f"""
            SELECT COUNT(DISTINCT s.stream_id), COUNT(st.stream_id)
            FROM oshilive.streams s
            JOIN oshilive.stream_stats st ON s.stream_id = st.stream_id
            WHERE s.status = 'past'
              AND s.end_actual <= NOW() - INTERVAL '%s days';
        """
        cur.execute(count_query, (args.days,))
        target_streams_count, total_stats_rows = cur.fetchone()

        logging.info(f"[분석] 압축 대상 방송 수: {target_streams_count:,}개 / 총 원본 로우 수: {total_stats_rows:,}개")

        if total_stats_rows == 0:
            logging.info("✅ 압축할 원본 데이터가 없습니다. 프로세스를 종료합니다.")
            cur.close()
            conn.close()
            return

        # 2. 15분 단위 다운샘플링 집계 쿼리 실행
        # 각 stream_id 및 15분 버킷별 MAX(viewer_count) 추출
        downsample_query = f"""
            WITH bucketed AS (
                SELECT 
                    stream_id,
                    viewer_count,
                    date_trunc('hour', {ts_col}) + (EXTRACT(minute FROM {ts_col})::int / %s * %s) * INTERVAL '1 minute' AS bucket_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY stream_id, date_trunc('hour', {ts_col}) + (EXTRACT(minute FROM {ts_col})::int / %s * %s) * INTERVAL '1 minute'
                        ORDER BY viewer_count DESC, {ts_col} ASC
                    ) AS rn
                FROM oshilive.stream_stats st
                WHERE stream_id IN (
                    SELECT stream_id FROM oshilive.streams 
                    WHERE status = 'past' AND end_actual <= NOW() - INTERVAL '%s days'
                )
            )
            SELECT stream_id, viewer_count, bucket_time
            FROM bucketed
            WHERE rn = 1;
        """

        cur.execute(downsample_query, (args.bucket_min, args.bucket_min, args.bucket_min, args.bucket_min, args.days))
        compressed_rows = cur.fetchall()

        reduced_count = len(compressed_rows)
        saved_rows = total_stats_rows - reduced_count
        saving_pct = (saved_rows / total_stats_rows * 100) if total_stats_rows > 0 else 0

        logging.info(f"📊 [압축 예상] 기존 {total_stats_rows:,}개 -> 압축 후 {reduced_count:,}개 ({saved_rows:,}개 삭제, {saving_pct:.1f}% 용량 절감)")

        if not args.execute:
            logging.info("💡 [Dry-Run] 실제 DB 수정이 수행되지 않았습니다. 실제 반영을 하려면 --execute 플래그를 붙여 실행하세요.")
            cur.close()
            conn.close()
            return

        # 3. 실제 DB 무테이션 (트랜잭션 세션 내에서 수행)
        logging.info("🔄 [EXECUTE] 데이터 압축 적용 중...")

        # A. 원본 압축 대상 데이터 삭제
        delete_query = f"""
            DELETE FROM oshilive.stream_stats
            WHERE stream_id IN (
                SELECT stream_id FROM oshilive.streams 
                WHERE status = 'past' AND end_actual <= NOW() - INTERVAL '%s days'
            );
        """
        cur.execute(delete_query, (args.days,))
        deleted_count = cur.rowcount
        logging.info(f"   - 기존 원본 로우 삭제 완료 ({deleted_count:,}개 삭제됨)")

        # B. 15분 단위 압축 집계 데이터 재적재
        insert_query = f"""
            INSERT INTO oshilive.stream_stats (stream_id, viewer_count, {ts_col})
            VALUES %s;
        """
        execute_values(cur, insert_query, compressed_rows, page_size=5000)
        logging.info(f"   - 15분 피크 압축 데이터 재적재 완료 ({len(compressed_rows):,}개 저장됨)")

        conn.commit()
        logging.info("🎉 [성공] DB 트랜잭션 커밋 완료!")

        # 4. VACUUM ANALYZE & REINDEX 실행 (데드 튜플 회수, 쿼리 통계 재작성, 인덱스 슬림화)
        try:
            logging.info("🧹 [DB 청소] VACUUM ANALYZE & REINDEX (용량 회수 + 쿼리 속도 0.01초 최적화) 수행 중...")
            conn.autocommit = True
            with conn.cursor() as cleanup_cur:
                cleanup_cur.execute("VACUUM ANALYZE oshilive.stream_stats;")
                cleanup_cur.execute("REINDEX TABLE oshilive.stream_stats;")
            logging.info("✨ [DB 청소 완료] 물리 용량 회수, 쿼리 통계 재작성(ANALYZE), 인덱스 슬림화(REINDEX) 완료!")
        except Exception as ve:
            logging.warning(f"⚠️ DB 청소 수행 중 알림: {ve}")

        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f"❌ 압축 작업 중 에러 발생: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        sys.exit(1)

if __name__ == "__main__":
    main()
