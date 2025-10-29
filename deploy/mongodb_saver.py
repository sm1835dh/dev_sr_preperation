import os
import json
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime
from db_connector import DatabaseConnector

class MongoDBSaver:
    """MongoDB 데이터 저장 관리"""

    def __init__(self, db_name: str = "rubicon"):
        self.db_name = db_name
        self.db_connector = DatabaseConnector()

    def save_metadata_to_collection(
        self,
        df_column_stats: pd.DataFrame,
        df_metadata: Optional[pd.DataFrame],
        df_schema: Optional[pd.DataFrame],
        table_name: str,
        selected_columns: List[str],
        collection_name: str,
        output_dir: str = './metadata'
    ) -> bool:
        """선택된 컬럼들의 메타데이터를 MongoDB에 저장"""

        client = self.db_connector.get_mongodb_client()
        if not client:
            return False

        try:
            db = self.db_connector.get_mongodb_database(client, self.db_name)
            collection = db[collection_name]

            print(f"📂 데이터베이스: {self.db_name}")
            print(f"📁 컬렉션: {collection_name}")
            print(f"🎯 선택된 컬럼 수: {len(selected_columns)}")

            documents = []
            inserted_count = 0

            for column_name in selected_columns:
                document = self._create_document(
                    column_name,
                    df_column_stats,
                    df_metadata,
                    df_schema,
                    table_name
                )

                if document:
                    documents.append(document)

            # MongoDB에 삽입
            if documents:
                print(f"\n📝 {len(documents)}개 문서를 MongoDB에 저장 중...")

                for doc in documents:
                    try:
                        result = collection.replace_one(
                            {"_id": doc["_id"]},
                            doc,
                            upsert=True
                        )

                        if result.upserted_id:
                            print(f"  ✅ 삽입: {doc['column']}")
                        else:
                            print(f"  🔄 업데이트: {doc['column']}")

                        inserted_count += 1

                    except Exception as e:
                        print(f"  ❌ 실패: {doc['column']} - {str(e)}")

                print(f"\n✅ 총 {inserted_count}개 문서 저장 완료")

                # 저장된 데이터 확인
                total_count = collection.count_documents({})
                print(f"📊 컬렉션 '{collection_name}'의 전체 문서 수: {total_count}")

                # 백업 파일 생성
                if output_dir:
                    self._create_backup(documents, collection_name, output_dir)

            else:
                print("⚠️ 저장할 문서가 없습니다.")
                return False

            return True

        except Exception as e:
            print(f"❌ 예상치 못한 오류: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if client:
                client.close()
                print("🔌 MongoDB 연결 종료")

    def _create_document(
        self,
        column_name: str,
        df_column_stats: pd.DataFrame,
        df_metadata: Optional[pd.DataFrame],
        df_schema: Optional[pd.DataFrame],
        table_name: str
    ) -> Optional[Dict[str, Any]]:
        """MongoDB 문서 생성"""

        # df_column_stats에서 해당 컬럼 정보 찾기
        column_stats = df_column_stats[df_column_stats['column_name'] == column_name]

        if column_stats.empty:
            print(f"  ⚠️ 컬럼 '{column_name}'을(를) 통계 데이터에서 찾을 수 없습니다.")
            return None

        row = column_stats.iloc[0]

        # 설명 파싱
        short_desc, long_desc, data_desc = self._parse_descriptions(
            column_name, df_metadata, row
        )

        # values 필드 처리
        values_list = self._process_values(row)

        # column_type 결정
        column_type = self._determine_column_type(column_name, row, df_schema)

        # Check if this is a JSONB field
        is_jsonb_field = row.get('is_jsonb_field', False)

        # Generate comment with SQL examples
        comment = self._generate_comment(row, column_name, table_name, is_jsonb_field)

        # MongoDB 문서 생성
        document = {
            "_id": f"{table_name}_{column_name}",
            "table": table_name.replace("kt_merged_", "").replace("_20251001", ""),
            "column": column_name,
            "column_type": column_type,
            "comment": comment,
            "short_description": short_desc,
            "long_description": long_desc,
            "data_description": data_desc,
            "values": values_list,
            "sql_use": "Y",
            "synonyms": [],
            "statistics": {
                "null_ratio": row.get('null_ratio', 'N/A'),
                "unique_count": int(row.get('unique_count')) if pd.notna(row.get('unique_count')) else None,
                "unique_ratio": row.get('unique_ratio', 'N/A')
            },
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        # Add JSONB-specific metadata
        if is_jsonb_field:
            document["is_jsonb_field"] = True
            document["jsonb_column"] = row.get('jsonb_column', '')
            document["field_name"] = row.get('field_name', '')
            document["original_field_path"] = row.get('original_field_path', '')

        # 추가 통계 정보
        self._add_statistics(document, row)

        return document

    def _parse_descriptions(
        self,
        column_name: str,
        df_metadata: Optional[pd.DataFrame],
        row: pd.Series
    ) -> tuple:
        """설명 파싱"""
        short_desc = ""
        long_desc = ""
        data_desc = ""

        if df_metadata is not None:
            metadata_matches = df_metadata[df_metadata['column_name'] == column_name]
            if not metadata_matches.empty:
                metadata_row = metadata_matches.iloc[0]

                if pd.notna(metadata_row.get('description')):
                    description_text = metadata_row['description']
                    lines = description_text.split('\n')

                    try:
                        # 인덱스 기반 파싱
                        if len(lines) > 1:
                            short_desc = lines[1].strip()
                            if short_desc.startswith('1.'):
                                short_desc = short_desc[2:].strip()

                        if len(lines) > 4:
                            long_desc = lines[4].strip()
                            if long_desc.startswith('2.'):
                                long_desc = long_desc[2:].strip()

                        if len(lines) > 7:
                            data_desc = lines[7].strip()
                            if data_desc.startswith('3.'):
                                data_desc = data_desc[2:].strip()

                    except Exception as e:
                        print(f"  ⚠️ 설명 파싱 실패 ({column_name}): {e}")
                        # 백업 방법
                        for line in lines:
                            line_strip = line.strip()
                            if line_strip.startswith('1.') and not short_desc:
                                short_desc = line_strip[2:].strip()
                            elif line_strip.startswith('2.') and not long_desc:
                                long_desc = line_strip[2:].strip()
                            elif line_strip.startswith('3.') and not data_desc:
                                data_desc = line_strip[2:].strip()

        # 기본값 설정
        if not short_desc:
            short_desc = f"{column_name} 정보"
        if not long_desc:
            long_desc = f"{column_name} 컬럼에 저장되는 데이터입니다."
        if not data_desc:
            data_desc = f"데이터 타입: {row.get('data_type', 'unknown')}, NULL 비율: {row.get('null_ratio', 'N/A')}"

        return short_desc, long_desc, data_desc

    def _process_values(self, row: pd.Series) -> List:
        """values 필드 처리"""
        values_list = []
        values_field = row.get('values')

        if values_field is not None:
            if isinstance(values_field, list):
                values_list = values_field[:]
                values_list = [str(v) if not isinstance(v, (str, int, float)) else v for v in values_list]
            elif isinstance(values_field, (np.ndarray, pd.Series)):
                values_list = values_field.tolist()[:] if len(values_field) > 0 else []
                values_list = [str(v) if not isinstance(v, (str, int, float)) else v for v in values_list]

        # most_common 대체
        if not values_list:
            most_common = row.get('most_common')
            if most_common is not None and isinstance(most_common, dict) and len(most_common) > 0:
                values_list = list(most_common.keys())[:]

        return values_list

    def _determine_column_type(
        self,
        column_name: str,
        row: pd.Series,
        df_schema: Optional[pd.DataFrame]
    ) -> str:
        """컬럼 타입 결정"""
        column_type = row.get('data_type', 'unknown')

        if df_schema is not None:
            schema_match = df_schema[df_schema['column_name'] == column_name]
            if not schema_match.empty:
                schema_info = schema_match.iloc[0]
                data_type = schema_info.get('data_type', '')
                max_length = schema_info.get('character_maximum_length')
                numeric_precision = schema_info.get('numeric_precision')
                numeric_scale = schema_info.get('numeric_scale')

                if pd.notna(max_length):
                    column_type = f"{data_type}({int(max_length)})"
                elif pd.notna(numeric_precision) and pd.notna(numeric_scale):
                    column_type = f"{data_type}({int(numeric_precision)},{int(numeric_scale)})"
                elif pd.notna(numeric_precision):
                    column_type = f"{data_type}({int(numeric_precision)})"
                else:
                    column_type = data_type

        return column_type

    def _add_statistics(self, document: Dict, row: pd.Series):
        """추가 통계 정보 추가"""
        if pd.notna(row.get('min')):
            document["statistics"].update({
                "min": float(row.get('min')) if pd.notna(row.get('min')) else None,
                "max": float(row.get('max')) if pd.notna(row.get('max')) else None,
                "mean": float(row.get('mean')) if pd.notna(row.get('mean')) else None,
                "median": float(row.get('median')) if pd.notna(row.get('median')) else None,
                "std": float(row.get('std')) if pd.notna(row.get('std')) else None
            })

        if pd.notna(row.get('min_length')):
            document["statistics"].update({
                "min_length": int(row.get('min_length')) if pd.notna(row.get('min_length')) else None,
                "max_length": int(row.get('max_length')) if pd.notna(row.get('max_length')) else None,
                "avg_length": float(row.get('avg_length')) if pd.notna(row.get('avg_length')) else None
            })

    def _generate_comment(self, row: pd.Series, column_name: str,
                         table_name: str, is_jsonb_field: bool) -> str:
        """
        Generate comment with SQL query examples for Text2SQL support
        """
        # Get existing comment from PostgreSQL
        existing_comment = row.get('column_comment', '')
        if pd.notna(existing_comment) and existing_comment:
            base_comment = existing_comment
        else:
            base_comment = ""

        # Add SQL examples for JSONB fields
        if is_jsonb_field:
            sql_path = column_name  # This is already the SQL path like "product_specification->'KT_SPEC'->>'무게 (g)'"
            field_name = row.get('field_name', '')
            data_type = row.get('data_type', 'object')

            # Generate sample values text
            sample_values = row.get('values', [])
            sample_text = ""
            if sample_values:
                sample_preview = sample_values[:3]
                sample_text = f" (예시 값: {', '.join(str(v) for v in sample_preview)})"

            # Check if this is a numeric field
            is_numeric = data_type == 'numeric'

            # Build SQL example comment based on data type
            if is_numeric:
                # For numeric fields, provide basic and range query examples
                sql_examples = f"""
조회 방법:
  - 기본 조회: SELECT {sql_path} FROM {table_name}
  - 범위 검색 (사이): SELECT * FROM {table_name} WHERE ({sql_path})::numeric BETWEEN 최소값 AND 최대값
{sample_text}
"""
            else:
                # For text fields, provide basic and exact match examples
                sql_examples = f"""
조회 방법:
  - 기본 조회: SELECT {sql_path} FROM {table_name}
  - 정확한 값 검색: SELECT * FROM {table_name} WHERE {sql_path} = '특정값'
{sample_text}
"""
            # Combine with existing comment
            if base_comment:
                return f"{base_comment}\n\n{sql_examples.strip()}"
            else:
                return sql_examples.strip()

        # For regular columns, return existing comment or empty
        return base_comment if base_comment else ""

    def _create_backup(self, documents: List[Dict], collection_name: str, output_dir: str):
        """백업 파일 생성"""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(output_dir, f'{collection_name}_mongodb_backup_{timestamp}.json')

        # datetime 객체를 문자열로 변환
        backup_docs = []
        for doc in documents:
            backup_doc = doc.copy()
            backup_doc['created_at'] = str(doc['created_at'])
            backup_doc['updated_at'] = str(doc['updated_at'])
            backup_docs.append(backup_doc)

        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(backup_docs, f, ensure_ascii=False, indent=2, default=str)

        print(f"💾 백업 파일 저장: {backup_path}")