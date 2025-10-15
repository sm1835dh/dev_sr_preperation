# 1. 필요한 라이브러리 임포트
import os
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import json
from dotenv import load_dotenv
from openai import AzureOpenAI
from typing import Dict, List, Any
import warnings
warnings.filterwarnings('ignore')

# .env 파일 로드
load_dotenv()

# 2. PostgreSQL 연결 설정
def get_db_connection():
    """PostgreSQL 데이터베이스 연결"""
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

# 3. Azure OpenAI 클라이언트 설정
def get_openai_client():
    """Azure OpenAI 클라이언트 생성"""
    try:
        # 환경 변수 확인
        endpoint = os.getenv('ENDPOINT_URL')
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        api_version = os.getenv('AZURE_API_VERSION', '2024-02-01')  # 기본값 제공
        
        if not endpoint:
            print("❌ ENDPOINT_URL 환경 변수가 설정되지 않았습니다.")
            return None
        
        if not api_key:
            print("❌ AZURE_OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
            return None
        
        # API 버전이 없으면 기본값 사용
        if not api_version:
            api_version = '2024-02-01'
            print(f"⚠️ AZURE_API_VERSION이 설정되지 않아 기본값 사용: {api_version}")
        
        print(f"📋 Azure OpenAI 설정:")
        print(f"  - Endpoint: {endpoint[:50]}...")
        print(f"  - API Version: {api_version}")
        print(f"  - Deployment: {os.getenv('DEPLOYMENT_NAME')}")
        
        # Initialize without proxies parameter for compatibility
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version
        )
        print(f"✅ Azure OpenAI 클라이언트 생성 성공")
        return client
    except Exception as e:
        print(f"❌ Azure OpenAI 클라이언트 생성 실패: {e}")
        return None

# 4. 테이블 스키마 정보 조회
def get_table_schema(table_name='test'):
    """테이블 스키마 정보 조회 (코멘트 포함)"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        # PostgreSQL에서 컬럼 정보와 코멘트를 함께 조회
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
        
        df_schema = pd.read_sql_query(query, conn, params=(table_name,))
        print(f"✅ 테이블 '{table_name}' 스키마 조회 성공")
        print(f"   - 컬럼 수: {len(df_schema)}")
        
        # 코멘트가 있는 컬럼 수 확인
        comment_count = df_schema['column_comment'].notna().sum()
        print(f"   - 코멘트가 있는 컬럼: {comment_count}개")
        
        return df_schema
    
    except Exception as e:
        print(f"❌ 테이블 스키마 조회 실패: {e}")
        return None
    
    finally:
        conn.close()

# 스키마 조회
df_schema = get_table_schema(table_name="kt_merged_product_20251001")
if df_schema is not None:
    print("\n테이블 스키마 정보:")
    display(df_schema.head(10))

# 6. 컬럼별 상세 통계 분석
import json

def get_column_statistics(table_name='test', sample_size=10000):
    """컬럼별 상세 통계 정보 수집"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        # 먼저 테이블에 데이터가 있는지 확인
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]

        if total_rows == 0:
            print(f"⚠️ 테이블 '{table_name}'에 데이터가 없습니다.")
            return None

        # 샘플 크기 조정 (전체 행 수보다 크면 전체 행 수로 조정)
        actual_sample_size = min(sample_size, total_rows)

        # 샘플 데이터 로드
        query = f"SELECT * FROM {table_name} LIMIT {actual_sample_size}"
        df = pd.read_sql_query(query, conn)

        column_stats = []

        for col in df.columns:
            stats = {
                'column_name': col,
                'data_type': str(df[col].dtype),
                'non_null_count': int(df[col].notna().sum()),
                'null_count': int(df[col].isna().sum()),
                'null_ratio': f"{df[col].isna().mean() * 100:.2f}%",
                # 기본적으로 모든 통계 값을 None으로 초기화
                'min': None,
                'max': None,
                'mean': None,
                'median': None,
                'std': None,
                'values': None  # 새로 추가된 컬럼
            }
            
            # 스키마 정보에서 코멘트 추가
            if df_schema is not None and 'column_comment' in df_schema.columns:
                schema_row = df_schema[df_schema['column_name'] == col]
                if not schema_row.empty:
                    stats['column_comment'] = schema_row.iloc[0]['column_comment']
                else:
                    stats['column_comment'] = None
            else:
                stats['column_comment'] = None

            # unique_count 계산 시 에러 처리
            try:
                # JSON/JSONB 컬럼이나 복잡한 객체가 포함된 경우를 처리
                if df[col].dtype == 'object':
                    # 문자열로 변환하여 고유값 계산
                    unique_count = df[col].astype(str).nunique()
                else:
                    unique_count = df[col].nunique()

                stats['unique_count'] = int(unique_count)
                stats['unique_ratio'] = f"{unique_count / len(df) * 100:.2f}%"
            except Exception as e:
                # 에러 발생 시 기본값 설정
                stats['unique_count'] = None
                stats['unique_ratio'] = None
                print(f"  ⚠️ 컬럼 '{col}' unique_count 계산 실패: {e}")

            # product_specification 컬럼 특별 처리
            if col == 'product_specification':
                try:
                    # JSON key 추출
                    all_keys = set()
                    non_null_values = df[col].dropna()
                    
                    for value in non_null_values:
                        try:
                            # JSON 문자열을 파싱
                            if isinstance(value, str):
                                json_data = json.loads(value)
                            else:
                                json_data = value
                            
                            # key 추출
                            if isinstance(json_data, dict):
                                all_keys.update(json_data.keys())
                            elif isinstance(json_data, list):
                                for item in json_data:
                                    if isinstance(item, dict):
                                        all_keys.update(item.keys())
                        except:
                            continue
                    
                    # key 리스트를 values에 저장
                    stats['values'] = sorted(list(all_keys))
                    
                    # 기본 통계도 추가
                    if len(non_null_values) > 0:
                        str_values = non_null_values.astype(str)
                        str_lengths = str_values.str.len()
                        
                        stats.update({
                            'min_length': int(str_lengths.min()) if len(str_lengths) > 0 else None,
                            'max_length': int(str_lengths.max()) if len(str_lengths) > 0 else None,
                            'avg_length': float(str_lengths.mean()) if len(str_lengths) > 0 else None
                        })
                        
                        # most_common은 key 개수로 계산
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
                    stats.update({
                        'min_length': None,
                        'max_length': None,
                        'avg_length': None,
                        'most_common': {}
                    })
                    
            # 수치형 데이터 통계 (숫자형 컬럼에만 적용)
            elif pd.api.types.is_numeric_dtype(df[col]):
                # null이 아닌 값만 추출
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    try:
                        stats['min'] = float(non_null_values.min())
                    except:
                        stats['min'] = None

                    try:
                        stats['max'] = float(non_null_values.max())
                    except:
                        stats['max'] = None

                    try:
                        stats['mean'] = float(non_null_values.mean())
                    except:
                        stats['mean'] = None

                    try:
                        stats['median'] = float(non_null_values.median())
                    except:
                        stats['median'] = None

                    try:
                        # std는 샘플이 2개 이상일 때만 계산 가능
                        if len(non_null_values) > 1:
                            stats['std'] = float(non_null_values.std())
                        else:
                            stats['std'] = None
                    except:
                        stats['std'] = None
                    
                    # 수치형 데이터: 모든 distinct 값 추출 (고유값이 많으면 상위 15개)
                    try:
                        unique_values = non_null_values.unique()
                        if len(unique_values) <= 1000:  # distinct 값이 1000개 이하면 모두 포함
                            stats['values'] = sorted(unique_values.tolist())
                        else:  # 1000개 초과면 상위 100개만
                            top_100_values = non_null_values.nlargest(100).tolist()
                            stats['values'] = top_100_values
                    except:
                        stats['values'] = None
                else:
                    # non_null_values가 없으면 모든 통계 값은 이미 None으로 설정됨
                    pass

            # 문자열 및 객체 데이터 통계 (nominal 컬럼)
            elif df[col].dtype == 'object':
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    try:
                        # 문자열로 변환하여 길이 계산
                        str_values = non_null_values.astype(str)
                        str_lengths = str_values.str.len()

                        stats.update({
                            'min_length': int(str_lengths.min()) if len(str_lengths) > 0 else None,
                            'max_length': int(str_lengths.max()) if len(str_lengths) > 0 else None,
                            'avg_length': float(str_lengths.mean()) if len(str_lengths) > 0 else None
                        })

                        # most_common 계산 시 에러 처리 (3개에서 100개로 증가)
                        try:
                            # 복잡한 객체는 문자열로 변환하여 카운트
                            value_counts = df[col].astype(str).value_counts().head(100)
                            stats['most_common'] = value_counts.to_dict()
                        except:
                            stats['most_common'] = {}
                        
                        # nominal 데이터: 모든 distinct 값 추출
                        try:
                            unique_values = df[col].unique()
                            # NULL 값 제외
                            unique_values = [v for v in unique_values if pd.notna(v)]
                            
                            # 고유값이 너무 많지 않으면 모두 포함
                            if len(unique_values) <= 3000:  # distinct 값이 3000개 이하면 모두 포함
                                stats['values'] = sorted(unique_values, key=str)
                            else:  # 3000개 초과면 가장 빈번한 300개만
                                top_values = df[col].value_counts().head(300).index.tolist()
                                stats['values'] = top_values
                        except:
                            stats['values'] = None

                    except Exception as e:
                        print(f"  ⚠️ 컬럼 '{col}' 문자열 통계 계산 실패: {e}")
                        stats.update({
                            'min_length': None,
                            'max_length': None,
                            'avg_length': None,
                            'most_common': {}
                        })
                else:
                    stats.update({
                        'min_length': None,
                        'max_length': None,
                        'avg_length': None,
                        'most_common': {}
                    })

            # 날짜형 데이터 통계
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    stats.update({
                        'min_date': str(non_null_values.min()),
                        'max_date': str(non_null_values.max()),
                        'date_range': str(non_null_values.max() - non_null_values.min())
                    })
                    
                    # 날짜형 데이터: 모든 distinct 날짜 추출 (고유값이 많으면 최근 100개)
                    try:
                        unique_dates = non_null_values.unique()
                        if len(unique_dates) <= 300:  # distinct 날짜가 300개 이하면 모두 포함
                            stats['values'] = pd.Series(unique_dates).sort_values().dt.strftime('%Y-%m-%d').tolist()
                        else:  # 100개 초과면 최근 100개만
                            recent_100_dates = non_null_values.nlargest(100).dt.strftime('%Y-%m-%d').tolist()
                            stats['values'] = recent_100_dates
                    except:
                        stats['values'] = None
                else:
                    stats.update({
                        'min_date': None,
                        'max_date': None,
                        'date_range': None
                    })

            column_stats.append(stats)

        print(f"✅ 컬럼별 통계 분석 완료 (샘플 크기: {len(df)}행)")
        return pd.DataFrame(column_stats)

    except Exception as e:
        print(f"❌ 컬럼별 통계 분석 실패: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()

# 컬럼 통계 수집
df_column_stats = get_column_statistics(table_name="kt_merged_product_20251001")
if df_column_stats is not None:
    display(df_column_stats)


# 7. Azure OpenAI를 활용한 컬럼 설명 생성
def generate_column_description(column_info, table_context='test'):
    """Azure OpenAI를 사용하여 컬럼 설명 생성"""
    
    if not openai_client:
        print("⚠️ OpenAI 클라이언트가 초기화되지 않았습니다.")
        return None
    
    # 데이터 타입 확인
    data_type = column_info.get('data_type', '')
    is_numeric = any(dtype in data_type.lower() for dtype in ['int', 'float', 'numeric', 'decimal', 'double'])
    is_string = 'object' in data_type.lower() or 'varchar' in data_type.lower() or 'text' in data_type.lower()
    is_date = 'date' in data_type.lower() or 'time' in data_type.lower()
    
    # 기본 정보
    base_info = f"""
    테이블명: {table_context}
    컬럼 정보:
    - 컬럼명: {column_info.get('column_name')}
    - 데이터 타입: {column_info.get('data_type')}
    - NULL 비율: {column_info.get('null_ratio', 'N/A')}
    - 고유값 개수: {column_info.get('unique_count', 'N/A')}"""
    
    # 기존 코멘트가 있으면 추가
    column_comment = column_info.get('column_comment')
    if column_comment and column_comment != 'None' and pd.notna(column_comment):
        base_info += f"\n    - 기존 설명: {column_comment}"
    
    # 데이터 타입별 추가 정보
    type_specific_info = ""
    
    if is_numeric:
        # 숫자형 데이터인 경우
        min_val = column_info.get('min')
        max_val = column_info.get('max')
        mean_val = column_info.get('mean')
        median_val = column_info.get('median')
        std_val = column_info.get('std')
        
        if min_val is not None or max_val is not None or mean_val is not None:
            type_specific_info += "\n    - 통계 정보:"
            if min_val is not None:
                type_specific_info += f"\n      - 최소값: {min_val}"
            if max_val is not None:
                type_specific_info += f"\n      - 최대값: {max_val}"
            if mean_val is not None:
                type_specific_info += f"\n      - 평균: {mean_val:.2f}"
            if median_val is not None:
                type_specific_info += f"\n      - 중앙값: {median_val:.2f}"
            if std_val is not None:
                type_specific_info += f"\n      - 표준편차: {std_val:.2f}"
    
    elif is_string:
        # 문자열 데이터인 경우
        min_length = column_info.get('min_length')
        max_length = column_info.get('max_length')
        avg_length = column_info.get('avg_length')
        most_common = column_info.get('most_common', {})
        
        if min_length is not None or max_length is not None or avg_length is not None:
            type_specific_info += "\n    - 문자열 길이 정보:"
            if min_length is not None:
                type_specific_info += f"\n      - 최소 길이: {min_length}"
            if max_length is not None:
                type_specific_info += f"\n      - 최대 길이: {max_length}"
            if avg_length is not None:
                type_specific_info += f"\n      - 평균 길이: {avg_length:.1f}"
        
        if most_common and len(most_common) > 0:
            type_specific_info += "\n    - 가장 빈번한 값:"
            for value, count in list(most_common.items())[:]:
                # 긴 문자열은 잘라서 표시
                display_value = value if len(str(value)) <= 100 else str(value)[:100] + "..."
                type_specific_info += f"\n      - '{display_value}': {count}개"
    
    elif is_date:
        # 날짜형 데이터인 경우
        min_date = column_info.get('min_date')
        max_date = column_info.get('max_date')
        date_range = column_info.get('date_range')
        
        if min_date or max_date or date_range:
            type_specific_info += "\n    - 날짜 범위:"
            if min_date:
                type_specific_info += f"\n      - 최소 날짜: {min_date}"
            if max_date:
                type_specific_info += f"\n      - 최대 날짜: {max_date}"
            if date_range:
                type_specific_info += f"\n      - 날짜 범위: {date_range}"
    
    # 프롬프트 구성 - 기존 코멘트가 있는 경우와 없는 경우를 구분
    if column_comment and column_comment != 'None' and pd.notna(column_comment):
        prompt = f"""{base_info}{type_specific_info}
    
    기존 설명을 참고하여 다음 형식으로 개선된 설명을 생성해주세요:
    1. 짧은 설명 (한 줄, 20자 이내) - 기존 설명을 참고하여 더 명확하게
    2. 상세 설명 (2-3줄, 비즈니스 의미 포함) - 기존 설명을 확장하여
    3. 데이터 특성 (NULL 허용 여부, 값 범위 등)
    
    기존 설명이 충분히 명확하다면 그대로 사용하되, 통계 정보를 반영하여 보완해주세요.
    """
    else:
        prompt = f"""{base_info}{type_specific_info}
    
    다음 형식으로 응답해주세요:
    1. 짧은 설명 (한 줄, 20자 이내)
    2. 상세 설명 (2-3줄, 비즈니스 의미 포함)
    3. 데이터 특성 (NULL 허용 여부, 값 범위 등)
    """
    
    try:
        print(f"\n📝 API 요청 정보:")
        print(f"  - 모델: {os.getenv('DEPLOYMENT_NAME')}")
        print(f"  - 엔드포인트: {os.getenv('ENDPOINT_URL')[:50]}...")
        
        # 프롬프트 일부 출력 (디버깅용)
        print(f"  - 프롬프트 길이: {len(prompt)}자")
        print(f"  - 프롬프트 처음 200자:\n{prompt[:200]}...")
        
        response = openai_client.chat.completions.create(
            model=os.getenv('DEPLOYMENT_NAME'),
            messages=[
                {"role": "system", "content": "당신은 데이터베이스 전문가입니다. 컬럼의 비즈니스 의미를 명확하게 설명해주세요. 기존 코멘트가 있다면 이를 참고하여 개선된 설명을 제공하세요."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=3000,
            temperature=1,
            # temperature 제거 (gpt-5-01 모델에서 지원 안 할 수 있음)
        )
        
        # 전체 응답 객체 확인
        print(f"\n📋 응답 객체 타입: {type(response)}")
        print(f"  - choices 개수: {len(response.choices) if response.choices else 0}")
        
        # 응답 확인
        if response and response.choices and len(response.choices) > 0:
            choice = response.choices[0]
            print(f"  - choice 객체: {choice}")
            print(f"  - finish_reason: {choice.finish_reason}")
            print(f"  - message 타입: {type(choice.message)}")
            
            # content 속성 확인
            if hasattr(choice.message, 'content'):
                content = choice.message.content
                print(f"  - content 타입: {type(content)}")
                print(f"  - content 값: '{content}'")
                
                if content:
                    print(f"✅ API 응답 수신 (길이: {len(content)}자)")
                    return content
                else:
                    print("⚠️ API 응답 content가 비어있습니다.")
                    # 빈 응답일 경우 기본 값 반환
                    if column_comment:
                        return f"1. {column_comment}\n2. {column_comment} 정보를 저장하는 컬럼입니다.\n3. NULL 허용, 문자열 타입"
                    else:
                        return f"1. {column_info.get('column_name')} 정보\n2. {column_info.get('column_name')} 관련 데이터를 저장합니다.\n3. {column_info.get('null_ratio')} NULL 비율"
            else:
                print("⚠️ message에 content 속성이 없습니다.")
                print(f"  - message 속성들: {dir(choice.message)}")
                return None
        else:
            print("⚠️ API 응답 형식이 예상과 다릅니다.")
            print(f"   응답 객체: {response}")
            return None
    
    except Exception as e:
        print(f"❌ OpenAI 설명 생성 실패: {e}")
        print(f"   에러 타입: {type(e).__name__}")
        if hasattr(e, 'response'):
            print(f"   응답 상태: {getattr(e.response, 'status_code', 'N/A')}")
            print(f"   응답 내용: {getattr(e.response, 'text', 'N/A')}")
        
        # 에러 발생 시 기본 설명 반환
        if column_comment:
            return f"1. {column_comment}\n2. {column_comment} 정보를 저장하는 컬럼입니다.\n3. NULL 허용 여부 확인 필요"
        else:
            return f"1. {column_info.get('column_name')} 컬럼\n2. 상세 설명 생성 실패\n3. 데이터 타입: {column_info.get('data_type')}"

# 테스트: 첫 번째 컬럼에 대한 설명 생성
if df_column_stats is not None and len(df_column_stats) > 0:
    print("=" * 60)
    print("테스트: 첫 번째 컬럼 설명 생성")
    print("=" * 60)
    
    test_column = df_column_stats.iloc[0].to_dict()
    print(f"\n테스트 컬럼 정보:")
    print(f"  - 컬럼명: {test_column.get('column_name')}")
    print(f"  - 데이터 타입: {test_column.get('data_type')}")
    print(f"  - 기존 코멘트: {test_column.get('column_comment')}")
    
    description = generate_column_description(test_column, table_context="kt_merged_product_20251001")
    
    print("\n" + "=" * 60)
    if description:
        print(f"생성된 설명:")
        print("-" * 50)
        print(description)
    else:
        print("⚠️ 설명 생성 실패 (None 반환)")
        
    # 추가 디버깅: 직접 API 호출 테스트
    print("\n" + "=" * 60)
    print("직접 API 테스트")
    print("=" * 60)
    
    if openai_client:
        try:
            print("간단한 메시지로 테스트 중...")
            test_response = openai_client.chat.completions.create(
                model=os.getenv('DEPLOYMENT_NAME'),
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "Say 'Hello World' in Korean."}
                ],
                max_completion_tokens=50
            )
            
            print(f"테스트 응답 타입: {type(test_response)}")
            if test_response.choices:
                print(f"Choices 개수: {len(test_response.choices)}")
                print(f"첫 번째 choice: {test_response.choices[0]}")
                print(f"Message: {test_response.choices[0].message}")
                print(f"Content: '{test_response.choices[0].message.content}'")
            else:
                print("No choices in response")
                
        except Exception as test_e:
            print(f"테스트 실패: {test_e}")

# 8. 모든 컬럼에 대한 메타데이터 생성 (배치 처리)
def generate_all_column_metadata(df_stats, batch_size=5):
    """모든 컬럼에 대한 메타데이터 생성"""
    
    if df_stats is None or len(df_stats) == 0:
        print("❌ 컬럼 통계 정보가 없습니다.")
        return None
    
    metadata_list = []
    total_columns = len(df_stats)
    
    print(f"총 {total_columns}개 컬럼에 대한 메타데이터 생성 시작...")
    
    for i in range(0, total_columns, batch_size):
        batch_end = min(i + batch_size, total_columns)
        print(f"\n배치 처리: {i+1}-{batch_end}/{total_columns}")
        
        for idx in range(i, batch_end):
            column_info = df_stats.iloc[idx].to_dict()
            column_name = column_info['column_name']
            
            print(f"  - {column_name} 처리 중...")
            
            # OpenAI 설명 생성
            description = generate_column_description(column_info, table_context="kt_merged_product_20251001")
            
            # 메타데이터 구성
            metadata = {
                'column_name': column_name,
                'data_type': column_info.get('data_type'),
                'null_ratio': column_info.get('null_ratio'),
                'unique_count': column_info.get('unique_count'),
                'description': description,
                'generated_at': datetime.now().isoformat()
            }
            
            # 수치형 데이터 추가 정보
            if 'min' in column_info:
                metadata.update({
                    'min': column_info.get('min'),
                    'max': column_info.get('max'),
                    'mean': column_info.get('mean'),
                    'median': column_info.get('median')
                })
            
            metadata_list.append(metadata)
            
            # API 호출 제한 방지를 위한 대기
            import time
            time.sleep(0.5)
    
    print(f"\n✅ 총 {len(metadata_list)}개 컬럼 메타데이터 생성 완료")
    return pd.DataFrame(metadata_list)

# 9. 테이블 전체 설명 생성
def generate_table_description(table_name='test', table_stats=None, column_stats=None):
    """테이블 전체에 대한 종합적인 설명 생성"""
    
    if not openai_client:
        return None
    
    # 주요 컬럼 정보 추출
    key_columns = []
    if column_stats is not None and len(column_stats) > 0:
        key_columns = column_stats.head(10)['column_name'].tolist()
    
    prompt = f"""
    다음 데이터베이스 테이블에 대한 종합적인 설명을 영어로 작성해주세요. 
    테이블은 삼성닷컴의 제품 정보를 담고 있습니다. 
    
    테이블명: {table_name}
    
    테이블 통계:
    - 전체 행 수: {table_stats.get('total_rows', 'N/A') if table_stats else 'N/A'}
    - 테이블 크기: {table_stats.get('table_size', 'N/A') if table_stats else 'N/A'}
    - 전체 컬럼 수: {len(column_stats) if column_stats is not None else 'N/A'}
    
    주요 컬럼: {', '.join(key_columns)}
    
    다음 내용을 포함하여 설명해주세요:
    1. 테이블의 주요 목적과 역할 (2-3줄)
    2. 저장되는 데이터의 비즈니스 의미
    3. 다른 테이블과의 잠재적 연관 관계
    4. 데이터 활용 사례 (예: 리포트, 분석, API 등)
    """
    
    try:
        print("--------")
        response = openai_client.chat.completions.create(
            model=os.getenv('DEPLOYMENT_NAME'),
            messages=[
                {"role": "system", "content": "당신은 데이터 아키텍트입니다. 테이블의 비즈니스 목적과 활용 방안을 명확하게 설명해주세요."},
                {"role": "user", "content": prompt}
            ],
            # temperature 파라미터 제거 (기본값 1 사용)
            max_completion_tokens=500
        )
        
        return response.choices[0].message.content
    
    except Exception as e:
        print(f"❌ 테이블 설명 생성 실패: {e}")
        return None

# 10-3. 메타데이터 저장 (MongoDB - 선택된 컬럼만)
def save_metadata_to_mongodb(df_column_stats, df_metadata, table_name, selected_columns, collection_name, output_dir='./metadata'):
    """선택된 컬럼들의 메타데이터를 MongoDB에 저장"""
    
    import os
    import pymongo
    from datetime import datetime
    from dotenv import load_dotenv
    
    load_dotenv('.env')
    
    # MongoDB 연결 문자열 가져오기
    CONNECTION_STRING = os.getenv('COSMOS_CONNECTION_STRING') or os.getenv('MONGODB_CONNECTION_STRING')
    
    if not CONNECTION_STRING:
        print("❌ MongoDB 연결 문자열을 찾을 수 없습니다.")
        print("   .env 파일에 COSMOS_CONNECTION_STRING 또는 MONGODB_CONNECTION_STRING을 설정해주세요.")
        return False
    
    try:
        # MongoDB 클라이언트 생성
        print(f"\n📡 MongoDB 연결 시도...")
        client = pymongo.MongoClient(CONNECTION_STRING)
        
        # 연결 테스트
        client.admin.command('ping')
        print(f"✅ MongoDB 연결 성공")
        
        # 데이터베이스 선택 (연결 문자열에서 추출하거나 기본값 사용)
        db_name = "rubicon"  # 기본 데이터베이스 이름
        
        # 연결 문자열에서 데이터베이스 이름 추출 시도
        if '/' in CONNECTION_STRING and '?' in CONNECTION_STRING:
            try:
                db_name_from_conn = CONNECTION_STRING.split('/')[-1].split('?')[0]
                if db_name_from_conn:
                    db_name = db_name_from_conn
            except:
                pass
        
        db = client[db_name]
        collection = db[collection_name]
        
        print(f"📂 데이터베이스: {db_name}")
        print(f"📁 컬렉션: {collection_name}")
        print(f"🎯 선택된 컬럼 수: {len(selected_columns)}")
        
        # 선택된 컬럼들의 메타데이터 준비
        documents = []
        inserted_count = 0
        
        for column_name in selected_columns:
            # df_column_stats에서 해당 컬럼 정보 찾기
            column_stats = df_column_stats[df_column_stats['column_name'] == column_name]
            
            if column_stats.empty:
                print(f"  ⚠️ 컬럼 '{column_name}'을(를) 통계 데이터에서 찾을 수 없습니다.")
                continue
            
            row = column_stats.iloc[0]
            
            # df_metadata에서 생성된 설명 가져오기
            metadata_row = None
            if df_metadata is not None:
                metadata_matches = df_metadata[df_metadata['column_name'] == column_name]
                if not metadata_matches.empty:
                    metadata_row = metadata_matches.iloc[0]
            
            # 설명 파싱 (인덱스 기반 파싱으로 수정)
            short_desc = ""
            long_desc = ""
            data_desc = ""
            
            if metadata_row is not None and pd.notna(metadata_row.get('description')):
                description_text = metadata_row['description']
                lines = description_text.split('\n')
                
                # 인덱스 기반으로 파싱 (빈 줄 포함하여 정확한 위치)
                try:
                    # short_description: 인덱스 1 (0부터 시작, 두 번째 줄)
                    if len(lines) > 1:
                        short_desc = lines[1].strip()
                        # "1. " 제거
                        if short_desc.startswith('1.'):
                            short_desc = short_desc[2:].strip()
                    
                    # long_description: 인덱스 4 (다섯 번째 줄)
                    if len(lines) > 4:
                        long_desc = lines[4].strip()
                        # "2. " 제거
                        if long_desc.startswith('2.'):
                            long_desc = long_desc[2:].strip()
                    
                    # data_description: 인덱스 7 (여덟 번째 줄)
                    if len(lines) > 7:
                        data_desc = lines[7].strip()
                        # "3. " 제거
                        if data_desc.startswith('3.'):
                            data_desc = data_desc[2:].strip()
                        
                except Exception as e:
                    print(f"  ⚠️ 설명 파싱 실패 ({column_name}): {e}")
                    # 파싱 실패 시 전체 줄을 순회하며 번호로 찾기 (백업 방법)
                    for line in lines:
                        line_strip = line.strip()
                        if line_strip.startswith('1.') and not short_desc:
                            short_desc = line_strip[2:].strip()
                        elif line_strip.startswith('2.') and not long_desc:
                            long_desc = line_strip[2:].strip()
                        elif line_strip.startswith('3.') and not data_desc:
                            data_desc = line_strip[2:].strip()
            
            # 기본값 설정 (설명이 없는 경우)
            if not short_desc:
                short_desc = f"{column_name} 정보"
            if not long_desc:
                long_desc = f"{column_name} 컬럼에 저장되는 데이터입니다."
            if not data_desc:
                data_desc = f"데이터 타입: {row.get('data_type', 'unknown')}, NULL 비율: {row.get('null_ratio', 'N/A')}"
            
            # values 필드 처리
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
            
            # column_type 결정
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
            
            # MongoDB 문서 생성
            document = {
                "_id": f"{table_name}_{column_name}",  # 고유 ID
                "table": table_name.replace("kt_merged_", "").replace("_20251001", ""),
                "column": column_name,
                "column_type": column_type,
                "comment": row.get('column_comment', '') if pd.notna(row.get('column_comment')) else "",
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
            
            # 추가 통계 정보
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
            
            documents.append(document)
        
        # MongoDB에 삽입 (upsert: 있으면 업데이트, 없으면 삽입)
        if documents:
            print(f"\n📝 {len(documents)}개 문서를 MongoDB에 저장 중...")
            
            for doc in documents:
                try:
                    # upsert 방식으로 저장
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
            
            # 샘플 문서 출력
            sample_doc = collection.find_one({"table": table_name.replace("kt_merged_", "").replace("_20251001", "")})
            if sample_doc:
                print(f"\n📋 샘플 문서:")
                # _id와 날짜 필드는 문자열로 변환하여 출력
                sample_doc['_id'] = str(sample_doc['_id'])
                sample_doc['created_at'] = str(sample_doc.get('created_at', ''))
                sample_doc['updated_at'] = str(sample_doc.get('updated_at', ''))
                print(json.dumps(sample_doc, ensure_ascii=False, indent=2))
        else:
            print("⚠️ 저장할 문서가 없습니다.")
            return False
        
        # 선택적: JSON 백업 파일도 생성
        if output_dir:
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
        
        return True
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print("❌ MongoDB 연결 시간 초과")
        print("   연결 문자열과 네트워크 설정을 확인해주세요.")
        return False
        
    except pymongo.errors.OperationFailure as e:
        print(f"❌ MongoDB 작업 실패: {str(e)}")
        print("   권한 설정을 확인해주세요.")
        return False
        
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if 'client' in locals():
            client.close()
            print("🔌 MongoDB 연결 종료")

# 선택된 컬럼 목록 정의
selected_columns = [
    'display_category_major',
    'display_category_middle',
    'display_category_minor',
    'product_category_major',
    'product_category_middle',
    'product_category_minor',
    'model_name',
    'model_code',
    'product_id',
    'product_name',
    'product_color',
    'release_date',
    'is_ai_subscription_eligible',
    'is_smart_subscription_eligible',
    'is_galaxy_club_eligible',
    'is_installment_payment_available',
    'product_detail_url',
    'unique_selling_point',
    'review_count',
    'review_rating_score',
    'standard_price',
    'member_price',
    'benefit_price',
    'review_text_collection',
    # 'display_classification_name',
    'product_specification',
    'web_coupon_discount_amount',
    'stock_quantity',
    'bundle_component_model_code',
    'site_code',
    'final_price',
    'is_bispokle_goods',
    'is_bundle_product',
    'category_rank_recommend',
    'category_rank_quantity',
    'category_rank_rating',
    'total_sale_amount',
    'total_sale_quantity',
    'event_info',
    'coupon_info',
    'promotion_info'
]

# MongoDB에 메타데이터 저장
if df_column_stats is not None:
    print("=" * 60)
    print("MongoDB에 선택된 컬럼 메타데이터 저장")
    print("=" * 60)
    
    success = save_metadata_to_mongodb(
        df_column_stats=df_column_stats,
        df_metadata=df_metadata,
        table_name='kt_merged_product_20251001',
        selected_columns=selected_columns,
        collection_name='synonyms_20251014'  # 컬렉션 이름을 여기서 지정
    )
    
    if success:
        print("\n✅ MongoDB 저장 작업 완료")
    else:
        print("\n❌ MongoDB 저장 작업 실패")