from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

class ColumnExtractor:
    def __init__(self, engine):
        """컬럼 정보 추출 클래스"""
        self.engine = engine

    def get_table_columns(self, table_name, schema='public'):
        """테이블의 컬럼 정보 추출"""
        try:
            query = text("""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    COALESCE(col_desc.description, '') as comment,
                    c.ordinal_position
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT
                        n.nspname as schema_name,
                        t.relname as table_name,
                        a.attname as column_name,
                        d.description
                    FROM pg_class t
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN pg_attribute a ON a.attrelid = t.oid
                    LEFT JOIN pg_description d ON d.objoid = t.oid AND d.objsubid = a.attnum
                    WHERE a.attnum > 0
                      AND NOT a.attisdropped
                ) col_desc ON col_desc.schema_name = c.table_schema
                                AND col_desc.table_name = c.table_name
                                AND col_desc.column_name = c.column_name
                WHERE c.table_schema = :schema
                  AND c.table_name = :table_name
                ORDER BY c.ordinal_position
            """)

            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    'schema': schema,
                    'table_name': table_name
                })
                columns = result.fetchall()

            logger.info(f"테이블 '{table_name}'에서 컬럼 {len(columns)}개 추출")
            return [dict(row._mapping) for row in columns]

        except Exception as e:
            logger.error(f"테이블 '{table_name}' 컬럼 정보 추출 중 오류: {e}")
            raise

    def format_column_type(self, column_info):
        """컬럼 타입을 포맷팅"""
        data_type = column_info['data_type']

        if data_type == 'character varying':
            if column_info['character_maximum_length']:
                return f"varchar({column_info['character_maximum_length']})"
            else:
                return "varchar"
        elif data_type == 'character':
            if column_info['character_maximum_length']:
                return f"char({column_info['character_maximum_length']})"
            else:
                return "char"
        elif data_type in ['numeric', 'decimal']:
            precision = column_info['numeric_precision']
            scale = column_info['numeric_scale']
            if precision and scale:
                return f"{data_type}({precision},{scale})"
            elif precision:
                return f"{data_type}({precision})"
            else:
                return data_type
        else:
            return data_type