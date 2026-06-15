import psycopg2
from psycopg2.extras import execute_values
import os
import sys
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv

def main():
    # 1. UTF-8 인코딩 설정 (Windows 콘솔 한글 깨짐 방지)
    sys.stdout.reconfigure(encoding='utf-8')

    # 2. 인자 파싱
    parser = argparse.ArgumentParser(description="OshiLive 오염된 하이라이트 데이터 복구 도구")
    parser.add_argument("--execute", action="store_true", help="실제 DB에 반영 (생략 시 dry-run 모드로 조회만 수행)")
    parser.add_argument("--days", type=int, default=None, help="최근 N일 이내의 방송만 복구 대상으로 지정 (생략 시 전체)")
    parser.add_argument("--tolerance", type=int, default=10, help="수집 누락 판정 허용 오차 (분 단위, 기본 10분)")
    args = parser.parse_args()

    # 3. 환경 변수 로드
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(dotenv_path)

    DB_CONFIG = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": os.getenv("DB_PORT")
    }

    if not DB_CONFIG["host"]:
        print("❌ 에러: .env 파일을 로드하지 못했거나 데이터베이스 설정이 없습니다.")
        sys.exit(1)

    print("=" * 60)
    print("🚀 OshiLive 오염된 데이터 복구 도구 시작")
    print(f"   - 모드: {'[실제 반영 Mode]' if args.execute else '[Dry-Run 조회 전용 Mode]'}")
    print(f"   - 대상 범위: {'전체 방송' if args.days is None else f'최근 {args.days}일 이내 방송'}")
    print(f"   - 허용 오차: {args.tolerance}분 (방송 시간 대비 수집된 채팅 시간이 {args.tolerance}분 이상 모자라면 오염으로 판단)")
    print("=" * 60)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 4. 쿼리 작성
        # start_actual과 end_actual 차이(실제 방송 시간) 대비 collected duration_sec가 허용 오차보다 작은 것을 감지
        # 외부 API 연동 지연으로 end_actual이 지나치게 길어진 경우(예: 12시간 초과)를 고려해 최대 비교 대상을 12시간(43200초)으로 캡핑합니다.
        query = f"""
        SELECT 
            sh.stream_id,
            s.title,
            s.start_actual,
            s.end_actual,
            EXTRACT(EPOCH FROM (s.end_actual - s.start_actual)) AS actual_duration_sec,
            sh.duration_sec AS collected_duration_sec
        FROM oshilive.stream_highlights sh
        JOIN oshilive.streams s ON sh.stream_id = s.stream_id
        WHERE s.status = 'past'
          AND s.end_actual IS NOT NULL
          AND s.start_actual IS NOT NULL
          -- 실제 방송이 비정상적으로 길게(12시간 초과) 감지된 경우도 12시간으로 캡하여 안전 비교
          AND sh.duration_sec < LEAST(EXTRACT(EPOCH FROM (s.end_actual - s.start_actual)), 43200) - %s
        """
        
        params = [args.tolerance * 60]

        if args.days is not None:
            query += " AND s.start_actual >= NOW() - INTERVAL '%s day'"
            params.append(args.days)

        query += " ORDER BY s.start_actual DESC;"

        cur.execute(query, tuple(params))
        polluted_streams = cur.fetchall()

        total_found = len(polluted_streams)
        print(f"🔍 분석 완료: 총 {total_found:,}개의 오염된 스트림이 감지되었습니다.")
        print("-" * 60)

        if total_found == 0:
            print("✨ 복구할 오염된 데이터가 존재하지 않습니다!")
            cur.close()
            conn.close()
            return

        # 상위 10개 예시 출력
        print("📋 오염된 스트림 예시 (최신순 10개):")
        for i, row in enumerate(polluted_streams[:10], 1):
            stream_id, title, start, end, actual, collected = row
            diff_min = (min(actual, 43200) - collected) / 60
            print(f" {i}. [{stream_id}] {title}")
            print(f"    방송시간: {actual/60:.1f}분 | 수집시간: {collected/60:.1f}분 | 누락시간: {diff_min:.1f}분 (시작: {start})")
        
        if total_found > 10:
            print(f" ...외 {total_found - 10:,}개의 스트림이 더 존재합니다.")
        
        print("-" * 60)

        # 5. 실제 반영 단계
        if args.execute:
            print(f"♻️ {total_found:,}개의 작업을 대기열(status = 0)로 복구 및 재등록 중...")
            
            # highlight_batch_tasks에 상태 0으로 복구/재등록
            requeue_values = [(row[0], 0) for row in polluted_streams]
            
            requeue_query = """
            INSERT INTO oshilive.highlight_batch_tasks (stream_id, status)
            VALUES %s
            ON CONFLICT (stream_id) 
            DO UPDATE SET 
                status = 0, 
                updated_at = CURRENT_TIMESTAMP;
            """
            
            execute_values(cur, requeue_query, requeue_values)
            conn.commit()
            print(f"✅ 복구 성공! {total_found:,}개의 작업이 성공적으로 대기열(status = 0)에 등록되었습니다.")
            print("   이제 배치 프로세스(highlight_bach.py)가 실행 중이면 자동으로 재수집 및 분석을 처리합니다.")
        else:
            print("💡 안내: --execute 옵션을 주어 실행하시면 위 스트림들이 실제로 대기열로 복구됩니다.")
            print("   예: python recover_polluted_tasks.py --execute")
            if args.days is not None:
                print(f"   예: python recover_polluted_tasks.py --execute --days {args.days}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ 데이터베이스 처리 중 오류 발생: {e}")
        sys.exit(1)

    print("=" * 60)

if __name__ == "__main__":
    main()
