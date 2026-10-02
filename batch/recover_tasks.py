import gc
from core.logger import get_logger
from core.db import get_db_cursor

logger = get_logger("TaskRecovery")

# Status Constants (smallint)
STATUS_PENDING = 0
STATUS_COMPLETED = 1
STATUS_IN_PROGRESS = 2
STATUS_FAILED = 3

def recover_polluted_tasks(timeout_minutes: int = 30):
    """
    IN_PROGRESS(2) 상태로 지정된 시간(기본 30분) 이상 방치된 오염 태스크를 PENDING(0)으로 복구
    3회 이상 시도된 경우 FAILED(3)로 상태 변경하여 무한 루프 방지
    """
    logger.info("=== 오염된 배치 태스크 상태 복구 작업 시작 ===")

    # 1. 3회 이상 실패한 오염 태스크 FAILED(3) 처리
    fail_sql = """
    UPDATE oshilive.highlight_batch_tasks
    SET status = %s,
        updated_at = CURRENT_TIMESTAMP
    WHERE status = %s
      AND updated_at < NOW() - (INTERVAL '1 minute' * %s)
      AND retry_count >= 3;
    """

    # 2. 복구 가능한 오염 태스크 PENDING(0)으로 재설정 (retry_count 1 증가)
    recover_sql = """
    UPDATE oshilive.highlight_batch_tasks
    SET status = %s,
        retry_count = retry_count + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE status = %s
      AND updated_at < NOW() - (INTERVAL '1 minute' * %s)
      AND retry_count < 3;
    """

    try:
        with get_db_cursor() as cur:
            cur.execute(fail_sql, (STATUS_FAILED, STATUS_IN_PROGRESS, timeout_minutes))
            failed_count = cur.rowcount

            cur.execute(recover_sql, (STATUS_PENDING, STATUS_IN_PROGRESS, timeout_minutes))
            recovered_count = cur.rowcount

        logger.info(f"✅ 오염 태스크 복구 완료: 복구됨={recovered_count}건, 최종 실패 처리됨={failed_count}건")
    except Exception as e:
        logger.error(f"❌ 오염 태스크 복구 중 에러 발생: {e}")
        raise
    finally:
        gc.collect()
        logger.info("=== 오염된 배치 태스크 상태 복구 작업 종료 ===")

if __name__ == "__main__":
    recover_polluted_tasks(30)
