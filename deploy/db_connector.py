import os
import psycopg2
import pymongo
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

class DatabaseConnector:
    """Database connection manager for PostgreSQL and MongoDB"""

    @staticmethod
    def get_postgres_connection():
        """Create PostgreSQL database connection"""
        try:
            conn = psycopg2.connect(
                host=os.getenv('PG_HOST'),
                port=os.getenv('PG_PORT'),
                database=os.getenv('PG_DATABASE'),
                user=os.getenv('PG_USER'),
                password=os.getenv('PG_PASSWORD')
            )
            print(f"✅ PostgreSQL 연결 성공: {os.getenv('PG_HOST')}")
            return conn
        except Exception as e:
            print(f"❌ PostgreSQL 연결 실패: {e}")
            return None

    @staticmethod
    def get_mongodb_client() -> Optional[pymongo.MongoClient]:
        """Create MongoDB client connection"""
        CONNECTION_STRING = os.getenv('COSMOS_CONNECTION_STRING') or os.getenv('MONGODB_CONNECTION_STRING')

        if not CONNECTION_STRING:
            print("❌ MongoDB 연결 문자열을 찾을 수 없습니다.")
            print("   .env 파일에 COSMOS_CONNECTION_STRING 또는 MONGODB_CONNECTION_STRING을 설정해주세요.")
            return None

        try:
            print(f"📡 MongoDB 연결 시도...")
            client = pymongo.MongoClient(CONNECTION_STRING)
            client.admin.command('ping')
            print(f"✅ MongoDB 연결 성공")
            return client
        except Exception as e:
            print(f"❌ MongoDB 연결 실패: {e}")
            return None

    @staticmethod
    def get_mongodb_database(client: pymongo.MongoClient, db_name: str = "rubicon"):
        """Get MongoDB database from client"""
        return client[db_name]