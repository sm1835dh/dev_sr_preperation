from sqlalchemy import text
import logging
import json

logger = logging.getLogger(__name__)

class DataSampler:
    def __init__(self, engine):
        """데이터 샘플링 클래스"""
        self.engine = engine

    def get_recent_data_samples(self, table_name, columns, schema='public', limit=3):
        """테이블에서 최근 데이터 샘플 추출"""
        try:
            # 컬럼명들을 콤마로 구분된 문자열로 변환
            column_names = [col['column_name'] for col in columns]
            columns_str = ', '.join([f'"{col}"' for col in column_names])

            # 테이블에 데이터가 있는지 먼저 확인
            count_query = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')

            with self.engine.connect() as conn:
                count_result = conn.execute(count_query)
                row_count = count_result.scalar()

                if row_count == 0:
                    logger.info(f"테이블 '{table_name}'에 데이터가 없습니다.")
                    return []

                # 최근 데이터 추출 (ROWID 또는 첫 번째 컬럼 기준으로 정렬)
                sample_query = text(f'''
                    SELECT {columns_str}
                    FROM "{schema}"."{table_name}"
                    ORDER BY "{column_names[0]}" DESC
                    LIMIT :limit
                ''')

                result = conn.execute(sample_query, {'limit': limit})
                samples = result.fetchall()

                logger.info(f"테이블 '{table_name}'에서 {len(samples)}개 샘플 추출")
                return [dict(row._mapping) for row in samples]

        except Exception as e:
            logger.error(f"테이블 '{table_name}' 데이터 샘플링 중 오류: {e}")
            # 오류가 발생해도 빈 리스트 반환
            return []

    def format_sample_value(self, value):
        """샘플 값을 문자열로 포맷팅"""
        if value is None:
            return "NULL"
        elif isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        elif isinstance(value, str):
            # 긴 문자열은 일부만 표시
            if len(value) > 100:
                return f"{value[:97]}..."
            return value
        else:
            return str(value)

    def get_table_data_summary(self, table_name, columns, schema='public'):
        """테이블 데이터 요약 정보"""
        try:
            with self.engine.connect() as conn:
                # 테이블 총 행 수
                count_query = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                total_rows = conn.execute(count_query).scalar()

                # 각 컬럼의 NULL 개수 확인
                column_stats = {}
                for col in columns:
                    col_name = col['column_name']
                    null_count_query = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}" WHERE "{col_name}" IS NULL')
                    null_count = conn.execute(null_count_query).scalar()
                    column_stats[col_name] = {
                        'null_count': null_count,
                        'not_null_count': total_rows - null_count
                    }

                return {
                    'total_rows': total_rows,
                    'column_stats': column_stats
                }

        except Exception as e:
            logger.error(f"테이블 '{table_name}' 데이터 요약 중 오류: {e}")
            return {
                'total_rows': 0,
                'column_stats': {}
            }