import psycopg2
import os
import sys
import argparse
import pytchat
import time
from dotenv import load_dotenv

def get_db_connection():
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(dotenv_path)

    DB_CONFIG = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": os.getenv("DB_PORT")
    }
    return psycopg2.connect(**DB_CONFIG)

def check_chat_availability(stream_id):
    """pytchat을 사용해 실시간 채팅 리플레이가 활성화되어 가져올 수 있는지 확인합니다."""
    try:
        chat = pytchat.create(video_id=stream_id, interruptable=False)
        if chat and chat.is_alive():
            # 정상적으로 리플레이를 열 수 있음
            return True
        return False
    except Exception:
        # InvalidVideo, NoChatData, Unavailable 등 예외가 발생하면 수집 불가
        return False

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description="OshiLive 실패/스킵(status=9) 태스크 안전 복구 도구")
    parser.add_argument("--days", type=int, default=20, help="최근 N일 이내에 등록된 태스크 대상")
    parser.add_argument("--limit", type=int, default=100, help="한 번에 처리할 최대 태스크 개수 (부하 분산용)")
    parser.add_argument("--execute", action="store_true", help="실제 DB에 복구 쿼리 실행 (미지정 시 조회만 수행)")
    args = parser.parse_args()

    print("============================================================")
    print("🚀 OshiLive 실패/스킵 태스크 복구 도구 시작")
    print(f"   - 모드: [{'실제 복구 실행 (EXECUTE)' if args.execute else 'Dry-Run 조회 전용'}]")
    print(f"   - 대상 범위: 최근 {args.days}일 이내 실패한 방송 (5월 28일 이후 등록 기준)")
    print(f"   - 최대 복구 처리 제한: {args.limit}개")
    print("============================================================")

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # 5월 28일 이후 + status = 9 인 태스크 목록 조회
        cur.execute("""
            SELECT t.stream_id, s.title, t.created_at, t.updated_at
            FROM oshilive.highlight_batch_tasks t
            LEFT JOIN oshilive.streams s ON t.stream_id = s.stream_id
            WHERE t.created_at >= '2026-05-28 00:00:00+09' 
              AND t.created_at >= NOW() - %s * INTERVAL '1 day'
              AND t.status = 9
            ORDER BY t.created_at DESC;
        """, (args.days,))
        
        failed_tasks = cur.fetchall()
        print(f"🔎 대상 기간 내 실패/스킵(status=9) 태스크 총 {len(failed_tasks)}개 감지.")

        if not failed_tasks:
            print("복구 대상 태스크가 없습니다.")
            return

        recoverable_ids = []
        checked_count = 0
        
        print("\n⏳ 각 스트림의 유튜브 채팅 리플레이 유효성 체크 시작...")
        for stream_id, title, created_at, updated_at in failed_tasks:
            if len(recoverable_ids) >= args.limit:
                print(f"⚠️ 설정한 처리 제한 개수({args.limit}개)에 도달하여 수집성 검사를 중단합니다.")
                break
                
            checked_count += 1
            title_str = title[:30] if title else "N/A"
            print(f"[{checked_count}/{len(failed_tasks)}] ID: {stream_id} | {title_str} ... ", end="", flush=True)
            
            # 리플레이 챗이 활성화되어 있는지 확인
            is_alive = check_chat_availability(stream_id)
            if is_alive:
                print("✅ [복구 가능] 채팅 리플레이 준비 완료")
                recoverable_ids.append(stream_id)
            else:
                print("❌ [복구 불가] 리플레이 생성 중이거나 영구 무효 영상")
            
            # 유튜브 IP 차단 방지를 위해 약간의 딜레이
            time.sleep(0.5)

        print("\n------------------------------------------------------------")
        print(f"📊 검사 완료: 총 {checked_count}개 검사 중 {len(recoverable_ids)}개 복구 가능")
        print("------------------------------------------------------------")

        if not recoverable_ids:
            print("복구할 수 있는 태스크가 없습니다.")
            return

        if args.execute:
            print("💾 DB 복구 실행 중...")
            cur.execute("""
                UPDATE oshilive.highlight_batch_tasks
                SET status = 0, retry_count = 0, updated_at = CURRENT_TIMESTAMP
                WHERE stream_id IN %s;
            """, (tuple(recoverable_ids),))
            conn.commit()
            print(f"🎉 성공적으로 {len(recoverable_ids)}개의 태스크가 대기 상태(status=0)로 복구되었습니다.")
        else:
            print("💡 안내: --execute 옵션을 주어 실행하시면 위 스트림들이 실제로 대기열(status=0)로 복구됩니다.")
            print("   예: python recover_failed_tasks.py --execute --days 20 --limit 50")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        if conn:
            conn.rollback()

if __name__ == "__main__":
    main()
