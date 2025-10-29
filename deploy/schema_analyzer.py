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

    def extract_jsonb_fields(self, jsonb_column: str = 'product_specification',
                            sample_size: int = 10000) -> Dict[str, Any]:
        """
        Extract all fields from JSONB column and return their paths and statistics

        Returns:
            Dict with structure: {
                'field_path': {
                    'sql_path': 'product_specification->\'KT_SPEC\'->>\'무게 (g)\'',
                    'parent_keys': ['KT_SPEC'],
                    'field_name': '무게 (g)',
                    'sample_values': [...],
                    'non_null_count': 100,
                    'null_count': 50
                }
            }
        """
        conn = self.db_connector.get_postgres_connection()
        if not conn:
            return {}

        try:
            # Get sample data
            query = f"SELECT {jsonb_column} FROM {self.table_name} WHERE {jsonb_column} IS NOT NULL LIMIT {sample_size}"
            df = pd.read_sql_query(query, conn)

            if df.empty:
                print(f"⚠️ No data found in column '{jsonb_column}'")
                return {}

            # Extract all field paths
            field_paths = {}

            for value in df[jsonb_column].dropna():
                try:
                    if isinstance(value, str):
                        json_data = json.loads(value)
                    else:
                        json_data = value

                    if isinstance(json_data, dict):
                        self._extract_nested_fields(json_data, [], field_paths, jsonb_column)

                except Exception as e:
                    continue

            # Collect statistics for each field
            result = {}
            total_rows = len(df)

            for field_path, field_info in field_paths.items():
                sql_path = field_info['sql_path']

                # Count non-null values using SQL
                try:
                    count_query = f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT({sql_path}) as non_null_count
                    FROM {self.table_name}
                    LIMIT {sample_size}
                    """
                    count_df = pd.read_sql_query(count_query, conn)
                    non_null_count = int(count_df.iloc[0]['non_null_count'])
                    total = int(count_df.iloc[0]['total'])
                    null_count = total - non_null_count

                    # Get sample values
                    sample_query = f"""
                    SELECT DISTINCT {sql_path} as value
                    FROM {self.table_name}
                    WHERE {sql_path} IS NOT NULL
                    LIMIT 100
                    """
                    sample_df = pd.read_sql_query(sample_query, conn)
                    sample_values = sample_df['value'].tolist() if not sample_df.empty else []

                    result[field_path] = {
                        'sql_path': sql_path,
                        'parent_keys': field_info['parent_keys'],
                        'field_name': field_info['field_name'],
                        'sample_values': sample_values,
                        'non_null_count': non_null_count,
                        'null_count': null_count,
                        'null_ratio': f"{(null_count / total * 100):.2f}%" if total > 0 else "N/A"
                    }

                except Exception as e:
                    print(f"  ⚠️ Failed to get statistics for {field_path}: {e}")
                    continue

            print(f"✅ Extracted {len(result)} JSONB fields from '{jsonb_column}'")
            return result

        except Exception as e:
            print(f"❌ JSONB field extraction failed: {e}")
            import traceback
            traceback.print_exc()
            return {}

        finally:
            conn.close()

    def _extract_nested_fields(self, data: Dict, parent_keys: list,
                               field_paths: Dict, jsonb_column: str):
        """
        Recursively extract nested fields from JSONB data
        """
        for key, value in data.items():
            current_path = parent_keys + [key]

            if isinstance(value, dict):
                # Recursively process nested dicts
                self._extract_nested_fields(value, current_path, field_paths, jsonb_column)
            else:
                # Leaf node - create SQL path
                sql_path = self._build_sql_path(jsonb_column, current_path)
                field_path_key = "->".join(current_path)

                if field_path_key not in field_paths:
                    field_paths[field_path_key] = {
                        'sql_path': sql_path,
                        'parent_keys': parent_keys,
                        'field_name': key
                    }

    def _build_sql_path(self, jsonb_column: str, keys: list) -> str:
        """
        Build PostgreSQL JSONB path syntax
        Example: product_specification->'KT_SPEC'->>'무게 (g)'

        Handles special characters in key names by escaping single quotes
        """
        if not keys:
            return jsonb_column

        # Use -> for intermediate keys and ->> for the final key
        path = jsonb_column
        for i, key in enumerate(keys):
            # Escape single quotes in key name by doubling them (PostgreSQL standard)
            escaped_key = key.replace("'", "''")

            if i == len(keys) - 1:
                # Last key: use ->> to get text value
                path += f"->>'{escaped_key}'"
            else:
                # Intermediate keys: use -> to keep JSON type
                path += f"->'{escaped_key}'"

        return path

    def get_jsonb_field_statistics(self, jsonb_fields: Dict[str, Any],
                                   jsonb_column: str = 'product_specification') -> pd.DataFrame:
        """
        Convert JSONB field information to DataFrame format compatible with column statistics
        """
        stats_list = []

        for field_path, field_info in jsonb_fields.items():
            # Determine data type based on sample values
            sample_values = field_info.get('sample_values', [])
            data_type = 'object'

            if sample_values:
                # Try to infer type by checking if majority of values are numeric
                numeric_count = 0
                total_count = 0

                for val in sample_values[:50]:  # Check first 50 values
                    if val is None or val == '':
                        continue

                    total_count += 1

                    # Check if value is numeric
                    try:
                        if isinstance(val, (int, float)):
                            numeric_count += 1
                        elif isinstance(val, str):
                            # Try to convert to float
                            # Remove common numeric formatting characters
                            cleaned = val.strip().replace(',', '')
                            float(cleaned)
                            numeric_count += 1
                    except (ValueError, AttributeError):
                        pass

                # If more than 80% of values are numeric, consider it numeric type
                if total_count > 0 and (numeric_count / total_count) >= 0.8:
                    data_type = 'numeric'

            stats = {
                'column_name': field_info['sql_path'],  # Use SQL path as column name
                'original_field_path': field_path,
                'jsonb_column': jsonb_column,
                'field_name': field_info['field_name'],
                'parent_keys': field_info['parent_keys'],
                'data_type': data_type,
                'non_null_count': field_info.get('non_null_count', 0),
                'null_count': field_info.get('null_count', 0),
                'null_ratio': field_info.get('null_ratio', 'N/A'),
                'unique_count': len(sample_values),
                'unique_ratio': 'N/A',
                'values': sample_values[:100],  # Limit to 100 values
                'is_jsonb_field': True
            }

            # Add numeric statistics if applicable
            if data_type == 'numeric' and sample_values:
                try:
                    numeric_values = []
                    for v in sample_values:
                        if v is None or v == '':
                            continue
                        try:
                            if isinstance(v, str):
                                cleaned = v.strip().replace(',', '')
                                numeric_values.append(float(cleaned))
                            else:
                                numeric_values.append(float(v))
                        except:
                            pass

                    if numeric_values:
                        stats.update({
                            'min': min(numeric_values),
                            'max': max(numeric_values),
                            'mean': sum(numeric_values) / len(numeric_values),
                            'median': sorted(numeric_values)[len(numeric_values) // 2]
                        })
                except Exception as e:
                    print(f"  ⚠️ Failed to calculate numeric statistics for {field_path}: {e}")

            stats_list.append(stats)

        print(f"✅ Generated statistics for {len(stats_list)} JSONB fields")
        return pd.DataFrame(stats_list)