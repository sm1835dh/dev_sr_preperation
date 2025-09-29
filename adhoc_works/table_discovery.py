from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

class TableDiscovery:
    def __init__(self, engine):
        """테이블 발견 클래스"""
        self.engine = engine

    def find_tables_by_prefix(self, prefix='table_', schema='public'):
        """지정된 접두사로 시작하는 테이블 목록 조회"""
        try:
            query = text("""
                SELECT
                    table_name,
                    table_type,
                    table_schema
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_name LIKE :pattern
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)

            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    'schema': schema,
                    'pattern': f"{prefix}%"
                })
                tables = result.fetchall()

            logger.info(f"'{prefix}' 접두사를 가진 테이블 {len(tables)}개 발견")
            return [dict(row._mapping) for row in tables]

        except Exception as e:
            logger.error(f"테이블 조회 중 오류 발생: {e}")
            raise

    def get_all_tables(self, schema='public'):
        """스키마의 모든 테이블 조회"""
        try:
            query = text("""
                SELECT
                    table_name,
                    table_type,
                    table_schema
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)

            with self.engine.connect() as conn:
                result = conn.execute(query, {'schema': schema})
                tables = result.fetchall()

            logger.info(f"스키마 '{schema}'에서 테이블 {len(tables)}개 발견")
            return [dict(row._mapping) for row in tables]

        except Exception as e:
            logger.error(f"전체 테이블 조회 중 오류 발생: {e}")
            raise