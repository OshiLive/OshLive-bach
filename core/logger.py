import io
import logging
import sys

def get_logger(name: str = "OshiLiveBatch") -> logging.Logger:
    """
    stdout 전용 경량 스트림 로거 반환
    logging.FileHandler를 완전 제거하여 Crontab 로그 재지향 중복으로 인한 Disk I/O 낭비를 방지
    TextIOWrapper(utf-8)를 통해 윈도우 환경(CP949) 이모지 출력 시 UnicodeEncodeError 방지
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # sys.stdout이 buffer 속성을 가진 경우 UTF-8 스트림 래핑
        if hasattr(sys.stdout, 'buffer'):
            stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        else:
            stream = sys.stdout

        handler = logging.StreamHandler(stream)
        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # 상위 로거 중복 전파 방지
        logger.propagate = False
        
    return logger

