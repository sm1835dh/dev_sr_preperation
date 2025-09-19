"""
Database connection and management module
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import sqlite3
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from configs.config import Config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manage PostgreSQL database connections and operations"""

    def __init__(self):
        self.config = Config()
        self.connection = None

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                dbname=self.config.DB_NAME,
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD,
                host=self.config.DB_HOST,
                port=self.config.DB_PORT
            )
            logger.info("Successfully connected to PostgreSQL")
            return self.connection
        except psycopg2.OperationalError as e:
            logger.warning(f"Could not connect to PostgreSQL: {e}")
            return None

    def create_database(self):
        """Create the database if it doesn't exist"""
        try:
            # Connect to default database
            conn = psycopg2.connect(
                dbname='postgres',
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD,
                host=self.config.DB_HOST,
                port=self.config.DB_PORT
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if database exists
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.config.DB_NAME,)
            )
            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {self.config.DB_NAME}")
                logger.info(f"Created database: {self.config.DB_NAME}")
            else:
                logger.info(f"Database {self.config.DB_NAME} already exists")

            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Execute a query and return results"""
        if not self.connection:
            self.connect()

        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchall()
            return []

    def execute_many(self, query: str, data: List[tuple]):
        """Execute multiple inserts"""
        if not self.connection:
            self.connect()

        with self.connection.cursor() as cursor:
            cursor.executemany(query, data)
            self.connection.commit()

    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
        """
        results = self.execute_query(query)
        return [r['table_name'] for r in results]

    def get_columns(self, table_name: str) -> List[Dict]:
        """Get column information for a table"""
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """
        return self.execute_query(query, (table_name,))

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


class SQLiteReader:
    """Read SQLite databases from BIRD dataset"""

    @staticmethod
    def read_schema(db_path: Path) -> Dict[str, List[Dict]]:
        """Read schema information from SQLite database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get all tables
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [row[0] for row in cursor.fetchall()]

        schema = {}
        for table in tables:
            # Get column info
            cursor.execute(f"PRAGMA table_info({table})")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[1],
                    'type': row[2],
                    'nullable': not row[3],
                    'default': row[4],
                    'primary_key': bool(row[5])
                })
            schema[table] = columns

        conn.close()
        return schema

    @staticmethod
    def read_data(db_path: Path, table: str, limit: Optional[int] = None) -> List[tuple]:
        """Read data from a table"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        query = f"SELECT * FROM {table}"
        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        data = cursor.fetchall()

        conn.close()
        return data