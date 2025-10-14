import pandas as pd
import numpy as np
import json
from typing import Optional, Dict, Any
from db_connector import DatabaseConnector

class SchemaAnalyzer:
    """PostgreSQL schema and data statistics analyzer"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.db_connector = DatabaseConnector()

    def get_table_schema(self) -> Optional[pd.DataFrame]:
        """테이블 스키마 정보 조회 (코멘트 포함)"""
        conn = self.db_connector.get_postgres_connection()
        if not conn:
            return None

        try:
            query = """
            SELECT
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                pgd.description as column_comment
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_statio_all_tables as st
                ON c.table_schema = st.schemaname
                AND c.table_name = st.relname
            LEFT JOIN pg_catalog.pg_description pgd
                ON pgd.objoid = st.relid
                AND pgd.objsubid = c.ordinal_position
            WHERE c.table_schema = 'public'
            AND c.table_name = %s
            ORDER BY c.ordinal_position;
            """

            df_schema = pd.read_sql_query(query, conn, params=(self.table_name,))
            print(f"✅ 테이블 '{self.table_name}' 스키마 조회 성공")
            print(f"   - 컬럼 수: {len(df_schema)}")

            comment_count = df_schema['column_comment'].notna().sum()
            print(f"   - 코멘트가 있는 컬럼: {comment_count}개")

            return df_schema

        except Exception as e:
            print(f"❌ 테이블 스키마 조회 실패: {e}")
            return None

        finally:
            conn.close()

    def get_column_statistics(self, sample_size: int = 10000) -> Optional[pd.DataFrame]:
        """컬럼별 상세 통계 정보 수집"""
        conn = self.db_connector.get_postgres_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            total_rows = cursor.fetchone()[0]

            if total_rows == 0:
                print(f"⚠️ 테이블 '{self.table_name}'에 데이터가 없습니다.")
                return None

            actual_sample_size = min(sample_size, total_rows)
            query = f"SELECT * FROM {self.table_name} LIMIT {actual_sample_size}"
            df = pd.read_sql_query(query, conn)

            column_stats = []

            for col in df.columns:
                stats = self._calculate_column_stats(df, col)
                column_stats.append(stats)

            print(f"✅ 컬럼별 통계 분석 완료 (샘플 크기: {len(df)}행)")
            return pd.DataFrame(column_stats)

        except Exception as e:
            print(f"❌ 컬럼별 통계 분석 실패: {e}")
            return None

        finally:
            if 'cursor' in locals():
                cursor.close()
            conn.close()

    def _calculate_column_stats(self, df: pd.DataFrame, col: str) -> Dict[str, Any]:
        """개별 컬럼의 통계 계산"""
        stats = {
            'column_name': col,
            'data_type': str(df[col].dtype),
            'non_null_count': int(df[col].notna().sum()),
            'null_count': int(df[col].isna().sum()),
            'null_ratio': f"{df[col].isna().mean() * 100:.2f}%",
            'min': None,
            'max': None,
            'mean': None,
            'median': None,
            'std': None,
            'values': None
        }

        # unique_count 계산
        try:
            if df[col].dtype == 'object':
                unique_count = df[col].astype(str).nunique()
            else:
                unique_count = df[col].nunique()
            stats['unique_count'] = int(unique_count)
            stats['unique_ratio'] = f"{unique_count / len(df) * 100:.2f}%"
        except Exception as e:
            stats['unique_count'] = None
            stats['unique_ratio'] = None
            print(f"  ⚠️ 컬럼 '{col}' unique_count 계산 실패: {e}")

        # product_specification 컬럼 특별 처리
        if col == 'product_specification':
            stats = self._handle_json_column(df, col, stats)
        # 수치형 데이터 통계
        elif pd.api.types.is_numeric_dtype(df[col]):
            stats = self._handle_numeric_column(df, col, stats)
        # 문자열 및 객체 데이터 통계
        elif df[col].dtype == 'object':
            stats = self._handle_string_column(df, col, stats)
        # 날짜형 데이터 통계
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            stats = self._handle_datetime_column(df, col, stats)

        return stats

    def _handle_json_column(self, df: pd.DataFrame, col: str, stats: Dict) -> Dict:
        """JSON 컬럼 처리"""
        try:
            all_keys = set()
            non_null_values = df[col].dropna()

            for value in non_null_values:
                try:
                    if isinstance(value, str):
                        json_data = json.loads(value)
                    else:
                        json_data = value

                    if isinstance(json_data, dict):
                        all_keys.update(json_data.keys())
                    elif isinstance(json_data, list):
                        for item in json_data:
                            if isinstance(item, dict):
                                all_keys.update(item.keys())
                except:
                    continue

            stats['values'] = sorted(list(all_keys))

            if len(non_null_values) > 0:
                str_values = non_null_values.astype(str)
                str_lengths = str_values.str.len()

                stats.update({
                    'min_length': int(str_lengths.min()) if len(str_lengths) > 0 else None,
                    'max_length': int(str_lengths.max()) if len(str_lengths) > 0 else None,
                    'avg_length': float(str_lengths.mean()) if len(str_lengths) > 0 else None
                })
                stats['most_common'] = {"total_unique_keys": len(all_keys)}
            else:
                stats.update({
                    'min_length': None,
                    'max_length': None,
                    'avg_length': None,
                    'most_common': {}
                })

        except Exception as e:
            print(f"  ⚠️ 컬럼 '{col}' JSON 처리 실패: {e}")
            stats['values'] = None

        return stats

    def _handle_numeric_column(self, df: pd.DataFrame, col: str, stats: Dict) -> Dict:
        """수치형 컬럼 처리"""
        non_null_values = df[col].dropna()
        if len(non_null_values) > 0:
            try:
                stats['min'] = float(non_null_values.min())
                stats['max'] = float(non_null_values.max())
                stats['mean'] = float(non_null_values.mean())
                stats['median'] = float(non_null_values.median())
                if len(non_null_values) > 1:
                    stats['std'] = float(non_null_values.std())
            except:
                pass

            try:
                unique_values = non_null_values.unique()
                if len(unique_values) <= 1000:
                    stats['values'] = sorted(unique_values.tolist())
                else:
                    top_100_values = non_null_values.nlargest(100).tolist()
                    stats['values'] = top_100_values
            except:
                stats['values'] = None

        return stats

    def _handle_string_column(self, df: pd.DataFrame, col: str, stats: Dict) -> Dict:
        """문자열 컬럼 처리"""
        non_null_values = df[col].dropna()
        if len(non_null_values) > 0:
            try:
                str_values = non_null_values.astype(str)
                str_lengths = str_values.str.len()

                stats.update({
                    'min_length': int(str_lengths.min()) if len(str_lengths) > 0 else None,
                    'max_length': int(str_lengths.max()) if len(str_lengths) > 0 else None,
                    'avg_length': float(str_lengths.mean()) if len(str_lengths) > 0 else None
                })

                try:
                    value_counts = df[col].astype(str).value_counts().head(100)
                    stats['most_common'] = value_counts.to_dict()
                except:
                    stats['most_common'] = {}

                try:
                    unique_values = df[col].unique()
                    unique_values = [v for v in unique_values if pd.notna(v)]

                    if len(unique_values) <= 3000:
                        stats['values'] = sorted(unique_values, key=str)
                    else:
                        top_values = df[col].value_counts().head(300).index.tolist()
                        stats['values'] = top_values
                except:
                    stats['values'] = None

            except Exception as e:
                print(f"  ⚠️ 컬럼 '{col}' 문자열 통계 계산 실패: {e}")

        return stats

    def _handle_datetime_column(self, df: pd.DataFrame, col: str, stats: Dict) -> Dict:
        """날짜형 컬럼 처리"""
        non_null_values = df[col].dropna()
        if len(non_null_values) > 0:
            stats.update({
                'min_date': str(non_null_values.min()),
                'max_date': str(non_null_values.max()),
                'date_range': str(non_null_values.max() - non_null_values.min())
            })

            try:
                unique_dates = non_null_values.unique()
                if len(unique_dates) <= 300:
                    stats['values'] = pd.Series(unique_dates).sort_values().dt.strftime('%Y-%m-%d').tolist()
                else:
                    recent_100_dates = non_null_values.nlargest(100).dt.strftime('%Y-%m-%d').tolist()
                    stats['values'] = recent_100_dates
            except:
                stats['values'] = None

        return stats

    def get_table_data(self, limit: Optional[int] = None, offset: int = 0) -> Optional[pd.DataFrame]:
        """Get actual table data from PostgreSQL"""
        conn = self.db_connector.get_postgres_connection()
        if not conn:
            return None

        try:
            if limit:
                query = f"SELECT * FROM {self.table_name} LIMIT {limit} OFFSET {offset}"
            else:
                query = f"SELECT * FROM {self.table_name}"

            df = pd.read_sql_query(query, conn)
            print(f"✅ Retrieved {len(df)} rows from table '{self.table_name}'")
            return df

        except Exception as e:
            print(f"❌ Failed to retrieve table data: {e}")
            return None

        finally:
            conn.close()

    def get_table_row_count(self) -> Optional[int]:
        """Get total row count of the table"""
        conn = self.db_connector.get_postgres_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count

        except Exception as e:
            print(f"❌ Failed to get row count: {e}")
            return None

        finally:
            conn.close()