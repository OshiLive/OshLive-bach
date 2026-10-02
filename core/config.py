import os
from dotenv import load_dotenv

# .env 파일 경로 자동 탐색 및 로드
load_dotenv()

class Config:
    # Database Settings
    DB_HOST: str = os.getenv("DB_HOST", "127.0.0.1")
    DB_NAME: str = os.getenv("DB_NAME", "oshilive")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASS: str = os.getenv("DB_PASS", "")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))

    # Holodex API Key
    HOLODEX_API_KEY: str = os.getenv("API_KEY", "")

    # PostgreSQL DSN
    @classmethod
    def get_dsn(cls) -> str:
        return (
            f"host={cls.DB_HOST} dbname={cls.DB_NAME} "
            f"user={cls.DB_USER} password={cls.DB_PASS} "
            f"port={cls.DB_PORT} options='-c client_encoding=utf8'"
        )

    # DSN Dictionary for psycopg2
    @classmethod
    def get_db_dict(cls) -> dict:
        return {
            "host": cls.DB_HOST,
            "database": cls.DB_NAME,
            "user": cls.DB_USER,
            "password": cls.DB_PASS,
            "port": cls.DB_PORT,
            "options": "-c client_encoding=utf8"
        }
