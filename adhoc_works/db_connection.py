import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self, env_file='.env'):
        """PostgreSQL 데이터베이스 연결 클래스"""
        load_dotenv(env_file)

        self.host = os.getenv('PG_HOST')
        self.port = os.getenv('PG_PORT')
        self.database = os.getenv('PG_DATABASE')
        self.user = os.getenv('PG_USER')
        self.password = os.getenv('PG_PASSWORD')

        print(f"DB Config - Host: {self.host}, Port: {self.port}, Database: {self.database}, User: {self.user}")

        self.engine = None
        self._validate_config()

    def _validate_config(self):
        """환경변수 검증"""
        required_vars = ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]

        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")

    def connect(self):
        """데이터베이스 연결 생성"""
        try:
            # Azure PostgreSQL은 SSL이 필요할 수 있음
            connection_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode=require"
            self.engine = create_engine(connection_url)

            # 연결 테스트
            from sqlalchemy import text
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info(f"PostgreSQL 연결 성공: {self.host}:{self.port}/{self.database}")
            return self.engine

        except SQLAlchemyError as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            raise

    def get_engine(self):
        """엔진 객체 반환"""
        if self.engine is None:
            self.connect()
        return self.engine

    def close(self):
        """연결 종료"""
        if self.engine:
            self.engine.dispose()
            logger.info("데이터베이스 연결 종료")