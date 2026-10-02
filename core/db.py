import threading
import sys
from contextlib import contextmanager
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from core.config import Config
from core.logger import get_logger

logger = get_logger("DB")

_connection_pool = None
_pool_lock = threading.Lock()

def init_db_pool(minconn: int = 1, maxconn: int = 5):
    """
    1GB RAM 메모리 방어를 위한 경량 ThreadedConnectionPool 초기화
    """
    global _connection_pool
    with _pool_lock:
        if _connection_pool is None or _connection_pool.closed:
            try:
                _connection_pool = ThreadedConnectionPool(
                    minconn=minconn,
                    maxconn=maxconn,
                    **Config.get_db_dict()
                )
                logger.info(f"DB 커넥션 풀 초기화 완료 (minconn={minconn}, maxconn={maxconn})")
            except Exception as e:
                logger.error(f"DB 커넥션 풀 생성 실패: {e}")
                sys.exit(1)

def close_db_pool():
    global _connection_pool
    with _pool_lock:
        if _connection_pool and not _connection_pool.closed:
            _connection_pool.closeall()
            logger.info("DB 커넥션 풀 정상 종료")

@contextmanager
def get_db_connection():
    """
    단일 커넥션을 가져오고 완료 후 풀로 자동 반환하는 Context Manager (Thread-safe)
    """
    global _connection_pool
    if _connection_pool is None or _connection_pool.closed:
        init_db_pool()

    conn = None
    with _pool_lock:
        conn = _connection_pool.getconn()

    try:
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        logger.error(f"DB 트랜잭션 에러 발생 (Rollback): {e}")
        raise
    finally:
        if conn and _connection_pool and not _connection_pool.closed:
            with _pool_lock:
                try:
                    _connection_pool.putconn(conn)
                except Exception:
                    pass

@contextmanager
def get_db_cursor(cursor_factory=None):
    """
    커넥션과 커서를 한 번에 관리하는 Context Manager
    cursor_factory=RealDictCursor 지정 시 Dict 형태 반환
    """
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=cursor_factory) if cursor_factory else conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
