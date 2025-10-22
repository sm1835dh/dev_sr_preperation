#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PostgreSQL 스펙 데이터 변환 파이프라인 (Spec Data Transformation Pipeline)
================================================================================

이 스크립트는 PostgreSQL 테이블의 스펙 데이터를 읽어서 dimension (width, height, depth)
정보를 파싱하고 변환하여 새로운 테이블에 저장합니다.

사용법:
------
1. 기본 실행 (대화형 모드):
   python transform_spec.py

2. 명령행 인자를 통한 실행:
   python transform_spec.py --source-table test_spec_01 --target-table test_spec_02 --truncate

3. 필터링 없이 전체 데이터 처리:
   python transform_spec.py --source-table test_spec_01 --target-table test_spec_02 --no-filter

필수 환경 변수 (.env 파일에 설정):
--------------------------------
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_database
PG_USER=your_username
PG_PASSWORD=your_password

주요 기능:
---------
1. 소스 테이블에서 데이터 로드
2. disp_nm1 필드로 데이터 필터링 (선택적)
3. 다양한 형식의 dimension 데이터 파싱:
   - W269 x D375 x H269 mm
   - 276(W) x 327(H) x 293(D) mm
   - 820 x 56 x103.5 mm(가로x높이x깊이)
   - 180 x 70 x 72 mm (단위 명시 없음)
   - 단일 값 (disp_nm2에서 타입 추론)
4. 파싱 결과를 새 테이블에 저장

출력 테이블 스키마:
-----------------
- 소스 테이블의 모든 컬럼 +
- dimension_type: 'width', 'height', 'depth' 중 하나
- parsed_value: 파싱된 수치 값
- needs_check: 검증이 필요한 데이터 플래그
- created_at: 생성 시각

================================================================================
"""

import os
import sys
import argparse
import re
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 기본 필터링 값
DEFAULT_ALLOWED_DISP_NM1 = ['규격','사양','외관 사양','기본 사양','외관','기본사양','본체치수','주요사양','일반사양']

def get_sqlalchemy_engine():
    """SQLAlchemy 엔진 생성"""
    try:
        connection_string = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
        engine = create_engine(connection_string)
        print(f"✅ SQLAlchemy 엔진 생성 성공")
        return engine
    except Exception as e:
        print(f"❌ SQLAlchemy 엔진 생성 실패: {e}")
        return None

def load_data_from_table(engine, table_name, allowed_disp_nm1):
    """
    PostgreSQL 테이블에서 데이터 로드
    
    Parameters:
    - engine: SQLAlchemy engine
    - table_name: 소스 테이블명
    - allowed_disp_nm1: 필터링할 disp_nm1 리스트
    
    Returns:
    - DataFrame
    """
    try:
        # 전체 데이터 로드
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        print(f"✅ 테이블 '{table_name}'에서 {len(df)}개 행 로드 완료")
        
        # allowed_disp_nm1로 필터링
        if allowed_disp_nm1 and len(allowed_disp_nm1) > 0:
            df_filtered = df[df['disp_nm1'].isin(allowed_disp_nm1)]
            print(f"✅ allowed_disp_nm1로 필터링: {len(df_filtered)}개 행")
        else:
            df_filtered = df
            print(f"⚠️  필터링 없이 전체 데이터 사용")
        
        return df_filtered
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        return None

def get_table_schema(engine, source_table_name):
    """
    소스 테이블의 스키마를 읽어옴
    
    Parameters:
    - engine: SQLAlchemy engine
    - source_table_name: 소스 테이블명
    
    Returns:
    - 컬럼 정보 딕셔너리 리스트
    """
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(source_table_name)
        print(f"✅ 소스 테이블 '{source_table_name}' 스키마 읽기 완료 ({len(columns)}개 컬럼)")
        return columns
    except Exception as e:
        print(f"❌ 스키마 읽기 실패: {e}")
        return None

def map_sqlalchemy_type_to_postgres(column_type):
    """
    SQLAlchemy 타입을 PostgreSQL 타입으로 변환
    """
    type_str = str(column_type)
    
    # 일반적인 타입 매핑
    if 'INTEGER' in type_str or 'BIGINT' in type_str or 'SMALLINT' in type_str:
        return 'INTEGER'
    elif 'SERIAL' in type_str or 'BIGSERIAL' in type_str:
        return 'SERIAL'
    elif 'VARCHAR' in type_str:
        # VARCHAR(길이) 추출
        return type_str.replace('VARCHAR', 'VARCHAR')
    elif 'TEXT' in type_str:
        return 'TEXT'
    elif 'BOOLEAN' in type_str or 'BOOL' in type_str:
        return 'BOOLEAN'
    elif 'TIMESTAMP' in type_str:
        return 'TIMESTAMP'
    elif 'DATE' in type_str:
        return 'DATE'
    elif 'NUMERIC' in type_str or 'DECIMAL' in type_str:
        return type_str.replace('NUMERIC', 'NUMERIC')
    elif 'FLOAT' in type_str or 'REAL' in type_str or 'DOUBLE' in type_str:
        return 'DOUBLE PRECISION'
    else:
        # 기본값
        return 'TEXT'

def create_parsed_table_from_source(engine, source_table_name, target_table_name):
    """
    소스 테이블의 스키마를 기반으로 파싱된 데이터를 저장할 테이블 생성
    dimension_type과 parsed_value 컬럼 추가
    
    Parameters:
    - engine: SQLAlchemy engine
    - source_table_name: 소스 테이블명
    - target_table_name: 생성할 테이블명
    """
    # 소스 테이블 스키마 읽기
    columns = get_table_schema(engine, source_table_name)
    if columns is None:
        return False
    
    # CREATE TABLE 쿼리 생성
    column_definitions = []
    
    for col in columns:
        col_name = col['name']
        col_type = map_sqlalchemy_type_to_postgres(col['type'])
        nullable = "NULL" if col['nullable'] else "NOT NULL"
        
        # PRIMARY KEY나 SERIAL 타입은 제거 (새 테이블에서는 id를 새로 만들 것)
        if col.get('autoincrement') or 'primary_key' in str(col).lower():
            continue
            
        column_definitions.append(f"{col_name} {col_type}")
    
    # dimension_type과 parsed_value 추가
    column_definitions.append("dimension_type TEXT")
    column_definitions.append("parsed_value NUMERIC")
    column_definitions.append("needs_check BOOLEAN")
    
    # CREATE TABLE 쿼리
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {target_table_name} (
        id SERIAL PRIMARY KEY,
        {', '.join(column_definitions)},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
        print(f"✅ 테이블 '{target_table_name}' 생성/확인 완료")
        print(f"   추가된 컬럼: dimension_type, parsed_value, needs_check")
        return True
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")
        return False

def truncate_table(engine, table_name):
    """
    테이블의 기존 데이터 삭제
    
    Parameters:
    - engine: SQLAlchemy engine
    - table_name: 테이블명
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
            conn.commit()
        print(f"✅ 테이블 '{table_name}'의 기존 데이터 삭제 완료")
        return True
    except Exception as e:
        print(f"❌ 데이터 삭제 실패: {e}")
        return False

def save_parsed_data_to_table(engine, df_parsed, df_needs_check, source_table_name, target_table_name, truncate_before_insert=False):
    """
    파싱된 데이터를 PostgreSQL 테이블에 저장
    소스 테이블의 모든 컬럼 + dimension_type, parsed_value, needs_check 저장
    
    Parameters:
    - engine: SQLAlchemy engine
    - df_parsed: 파싱 성공한 확실한 데이터
    - df_needs_check: 파싱 성공했지만 체크가 필요한 데이터
    - source_table_name: 소스 테이블명
    - target_table_name: 대상 테이블명
    - truncate_before_insert: True이면 기존 데이터 삭제
    
    Returns:
    - 성공 여부
    """
    try:
        # 테이블 생성
        if not create_parsed_table_from_source(engine, source_table_name, target_table_name):
            return False
        
        # 기존 데이터 삭제 옵션
        if truncate_before_insert:
            if not truncate_table(engine, target_table_name):
                return False
        
        # 두 DataFrame 합치기
        df_all = pd.DataFrame()
        
        if len(df_parsed) > 0:
            df_parsed_copy = df_parsed.copy()
            df_parsed_copy['needs_check'] = False
            df_all = pd.concat([df_all, df_parsed_copy], ignore_index=True)
        
        if len(df_needs_check) > 0:
            df_needs_check_copy = df_needs_check.copy()
            df_needs_check_copy['needs_check'] = True
            df_all = pd.concat([df_all, df_needs_check_copy], ignore_index=True)
        
        if len(df_all) == 0:
            print("⚠️  저장할 데이터가 없습니다.")
            return True
        
        # dimension_type과 parsed_value가 있는지 확인
        if 'dimension_type' not in df_all.columns or 'parsed_value' not in df_all.columns:
            print("❌ dimension_type 또는 parsed_value 컬럼이 없습니다.")
            return False
        
        # 소스 테이블의 컬럼 정보 가져오기
        source_columns = get_table_schema(engine, source_table_name)
        if source_columns is None:
            return False
        
        # 소스 컬럼명 리스트
        source_column_names = [col['name'] for col in source_columns if not col.get('autoincrement')]
        
        # 저장할 DataFrame 구성: 소스의 모든 컬럼 + dimension_type, parsed_value, needs_check
        df_to_save = pd.DataFrame()
        
        # 소스 테이블의 모든 컬럼 복사
        for col_name in source_column_names:
            if col_name in df_all.columns:
                df_to_save[col_name] = df_all[col_name]
        
        # 새로운 컬럼 추가
        df_to_save['dimension_type'] = df_all['dimension_type']
        df_to_save['parsed_value'] = df_all['parsed_value']
        df_to_save['needs_check'] = df_all['needs_check']
        
        # 데이터 저장
        df_to_save.to_sql(target_table_name, engine, if_exists='append', index=False)
        print(f"✅ 테이블 '{target_table_name}'에 {len(df_to_save)}개 행 저장 완료")
        return True
        
    except Exception as e:
        print(f"❌ 데이터 저장 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================
# 파싱 함수 정의
# ============================================

def identify_dimension_type(text, disp_nm3=None):
    """
    텍스트에서 dimension 타입을 식별

    Parameters:
    -----------
    text : str
        분석할 텍스트 (disp_nm2)
    disp_nm3 : str, optional
        제품 카테고리 정보 (마우스, 키보드 구분용)
    """
    text_lower = text.lower()

    # L(Length) 키워드 - depth로 매핑
    if any(keyword in text_lower for keyword in ['길이', 'l', 'length']):
        return 'depth'
    # Depth 키워드
    elif any(keyword in text_lower for keyword in ['두께', '깊이', 'd']):
        return 'depth'
    # Width 키워드
    elif any(keyword in text_lower for keyword in ['가로', '폭', 'w']):
        return 'width'
    # Height 키워드
    elif any(keyword in text_lower for keyword in ['세로', '높이', 'h']):
        return 'height'

    return None

def parse_dimensions_advanced(row):
    """
    disp_nm2에 따라 value를 파싱하는 함수 (확장 버전)
    """
    parsed_rows = []
    value = str(row['value'])
    disp_nm2 = str(row['disp_nm2'])
    disp_nm3 = str(row.get('disp_nm3', ''))

    # 제외 조건: 각도 조정 관련 텍스트가 포함된 경우
    if any(keyword in value.lower() for keyword in ['각도 조정', '각도조정']):
        return parsed_rows, False, False
    
    # 키보드 세트의 경우 첫 번째 제품만 파싱 (키보드:, 마우스:, 리시버 등이 여러개 있는 경우)
    if '키보드' in value and ':' in value:
        # "키보드 : 440(L)*156(W)*24(H)mm마우스 : ..." 형태에서 키보드 부분만 추출
        keyboard_match = re.search(r'키보드\s*:\s*([^가-힣]*?)(?:마우스|리시버|$)', value)
        if keyboard_match:
            value = keyboard_match.group(1).strip()

    # 패턴 0: W숫자 x D숫자 x H숫자 형식 (예: "W269 x D375 x H269 mm") + L 매핑 지원
    wdh_pattern = r'([WwHhDdLl])\s*([0-9,]+(?:\.[0-9]+)?)'
    wdh_matches = re.findall(wdh_pattern, value)

    if len(wdh_matches) >= 2:  # 최소 2개 이상의 dimension이 있는 경우
        base_row = row.to_dict()
        dimension_map = {'w': 'width', 'h': 'height', 'd': 'depth', 'l': 'depth'}  # L을 depth로 매핑

        for dim_letter, num_val in wdh_matches:
            dim_type = dimension_map.get(dim_letter.lower())
            if dim_type:
                # 콤마 제거 후 숫자 파싱
                clean_num = num_val.replace(',', '')
                try:
                    parsed_num = float(clean_num)
                    new_row = base_row.copy()
                    new_row['dimension_type'] = dim_type
                    new_row['parsed_value'] = parsed_num
                    new_row['needs_check'] = False
                    parsed_rows.append(new_row)
                except ValueError:
                    continue

        if parsed_rows:
            return parsed_rows, True, False
    
    # 패턴 1: value에 숫자(W), 숫자(H), 숫자(D), 숫자(L)가 명시된 경우
    whd_pattern = r'([0-9,]+(?:\.[0-9]+)?)\s*(?:mm)?\s*\(?\s*([WwHhDdLl])\s*\)?'
    whd_matches = re.findall(whd_pattern, value)

    if len(whd_matches) >= 2:  # 최소 2개 이상의 dimension이 있는 경우
        base_row = row.to_dict()
        dimension_map = {'w': 'width', 'h': 'height', 'd': 'depth', 'l': 'depth'}  # L을 depth로 매핑

        for num_val, dim_letter in whd_matches:
            dim_type = dimension_map.get(dim_letter.lower())
            if dim_type:
                # 콤마 제거 후 숫자 파싱
                clean_num = num_val.replace(',', '')
                try:
                    parsed_num = float(clean_num)
                    new_row = base_row.copy()
                    new_row['dimension_type'] = dim_type
                    new_row['parsed_value'] = parsed_num
                    new_row['needs_check'] = False
                    parsed_rows.append(new_row)
                except ValueError:
                    continue

        if parsed_rows:
            return parsed_rows, True, False
    
    # 패턴 2: "가로x높이x깊이" 텍스트가 있는 경우 (예: "820 x 56 x103.5 mm(가로x높이x깊이)")
    if '가로' in value and '높이' in value and '깊이' in value:
        nums = re.findall(r'([0-9,]+(?:\.[0-9]+)?)', value)
        if len(nums) >= 3:
            base_row = row.to_dict()

            try:
                # 가로 (width)
                row1 = base_row.copy()
                row1['dimension_type'] = 'width'
                row1['parsed_value'] = float(nums[0].replace(',', ''))
                row1['needs_check'] = False
                parsed_rows.append(row1)

                # 높이 (height)
                row2 = base_row.copy()
                row2['dimension_type'] = 'height'
                row2['parsed_value'] = float(nums[1].replace(',', ''))
                row2['needs_check'] = False
                parsed_rows.append(row2)

                # 깊이 (depth)
                row3 = base_row.copy()
                row3['dimension_type'] = 'depth'
                row3['parsed_value'] = float(nums[2].replace(',', ''))
                row3['needs_check'] = False
                parsed_rows.append(row3)

                return parsed_rows, True, False
            except ValueError:
                pass
    
    # 패턴 3: WxHxD 형식 (x로 구분, 단위 명시 없음) (예: "180 x 70 x 72 mm", "223 x 96.5 x 94 mm")
    wxhxd_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)\s*[xX×]\s*([0-9,]+(?:\.[0-9]+)?)\s*[xX×]\s*([0-9,]+(?:\.[0-9]+)?)', value)
    if wxhxd_match:
        val1, val2, val3 = wxhxd_match.groups()
        base_row = row.to_dict()

        # 기본 가정: 가로 x 높이 x 깊이
        dimensions = [
            ('width', val1),
            ('height', val2),
            ('depth', val3)
        ]

        try:
            for dim_type, val in dimensions:
                new_row = base_row.copy()
                new_row['dimension_type'] = dim_type
                new_row['parsed_value'] = float(val.replace(',', ''))
                new_row['needs_check'] = True  # 단위가 명확하지 않음
                parsed_rows.append(new_row)

            return parsed_rows, True, True
        except ValueError:
            pass
    
    # 패턴 4: WxH 형식 (예: "500x600 mm")
    wxh_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)\s*[xX×]\s*([0-9,]+(?:\.[0-9]+)?)', value)
    if wxh_match:
        val1, val2 = wxh_match.groups()
        base_row = row.to_dict()

        # 기본 가정: 가로 x 높이
        dimensions = [
            ('width', val1),
            ('height', val2)
        ]

        try:
            for dim_type, val in dimensions:
                new_row = base_row.copy()
                new_row['dimension_type'] = dim_type
                new_row['parsed_value'] = float(val.replace(',', ''))
                new_row['needs_check'] = True  # 단위가 명확하지 않음
                parsed_rows.append(new_row)

            return parsed_rows, True, True
        except ValueError:
            pass
    
    # 패턴 5: 단일 값 (disp_nm2에서 dimension 타입 식별)
    single_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)', value)
    if single_match:
        dim_type = identify_dimension_type(disp_nm2, disp_nm3)
        if dim_type:
            try:
                clean_num = single_match.group(1).replace(',', '')
                parsed_num = float(clean_num)
                base_row = row.to_dict()
                base_row['dimension_type'] = dim_type
                base_row['parsed_value'] = parsed_num
                base_row['needs_check'] = False
                parsed_rows.append(base_row)
                return parsed_rows, True, False
            except ValueError:
                pass
    
    return parsed_rows, False, False

# ============================================
# 메인 실행 함수
# ============================================

def process_spec_data(source_table, target_table, allowed_disp_nm1=None, truncate_before_insert=True, verbose=True):
    """
    스펙 데이터 처리 파이프라인 실행

    Parameters:
    -----------
    source_table : str
        소스 테이블명
    target_table : str
        타겟 테이블명
    allowed_disp_nm1 : list, optional
        필터링할 disp_nm1 값 리스트 (None이면 필터링 안함)
    truncate_before_insert : bool
        True이면 기존 데이터 삭제 후 삽입
    verbose : bool
        상세 출력 여부

    Returns:
    --------
    bool : 성공 여부
    """
    # 1. SQLAlchemy 엔진 생성
    engine = get_sqlalchemy_engine()

    if engine is None:
        print("❌ 엔진 생성 실패. 작업을 중단합니다.")
        return False

    try:
        # 2. 데이터 로드
        print("\n" + "="*80)
        print("📥 데이터 로드 중...")
        print("="*80)
        df_filtered = load_data_from_table(engine, source_table, allowed_disp_nm1)

        if df_filtered is None or len(df_filtered) == 0:
            print("\n❌ 데이터 로드 실패 또는 데이터 없음")
            return False

        # 3. 데이터 파싱
        print("\n" + "="*80)
        print("🔄 데이터 파싱 중...")
        print("="*80)

        parsed_data = []
        parsed_data_needs_check = []
        unparsed_data = []

        for _, row in df_filtered.iterrows():
            parsed_rows, success, needs_check = parse_dimensions_advanced(row)
            if success and parsed_rows:
                if needs_check:
                    parsed_data_needs_check.extend(parsed_rows)
                else:
                    parsed_data.extend(parsed_rows)
            else:
                unparsed_data.append(row)

        df_parsed = pd.DataFrame(parsed_data)
        df_parsed_needs_check = pd.DataFrame(parsed_data_needs_check)
        df_unparsed = pd.DataFrame(unparsed_data)

        # 파싱 통계 출력
        total_parsed = len(df_parsed) + len(df_parsed_needs_check)
        print(f"✅ 파싱 성공 (확실): {len(df_parsed)}개 행")
        print(f"⚠️  파싱 성공 (체크 필요): {len(df_parsed_needs_check)}개 행")
        print(f"❌ 파싱 실패: {len(df_unparsed)}개 행")
        print(f"📈 전체 대비 파싱률: {(total_parsed / len(df_filtered) * 100):.1f}%")

        # 상세 출력 (verbose 모드)
        if verbose:
            print_parsing_results(df_parsed, df_parsed_needs_check, df_unparsed)

        # 4. PostgreSQL 테이블에 저장
        print("\n" + "="*80)
        print("💾 데이터 저장 중...")
        print("="*80)

        success = save_parsed_data_to_table(
            engine=engine,
            df_parsed=df_parsed,
            df_needs_check=df_parsed_needs_check,
            source_table_name=source_table,
            target_table_name=target_table,
            truncate_before_insert=truncate_before_insert
        )

        if success:
            print("\n" + "="*80)
            print("✅ 전체 작업 완료!")
            print("="*80)
            print(f"📊 요약:")
            print(f"  - 소스 테이블: {source_table}")
            print(f"  - 타겟 테이블: {target_table}")
            print(f"  - 저장된 데이터: {total_parsed}개 행")
            print(f"  - 기존 데이터 삭제: {'예' if truncate_before_insert else '아니오'}")
            print(f"  - 타겟 테이블 스키마: 소스 테이블 컬럼 + dimension_type, parsed_value, needs_check")
            return True
        else:
            print("\n❌ 데이터 저장 실패")
            return False

    except Exception as e:
        print(f"\n❌ 처리 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if engine:
            engine.dispose()

def print_parsing_results(df_parsed, df_parsed_needs_check, df_unparsed):
    """파싱 결과 상세 출력"""

    # 파싱 성공한 데이터 출력 (확실한 것)
    if len(df_parsed) > 0:
        print("\n✅ 파싱 성공 데이터 - 확실 (처음 20개):")
        print("-" * 80)
        display_cols = ['disp_nm1', 'disp_nm2', 'disp_nm3', 'disp_nm4', 'dimension_type', 'parsed_value', 'value']
        available_cols = [col for col in display_cols if col in df_parsed.columns]
        print(df_parsed[available_cols].head(20).to_string())
    else:
        print("\n확실하게 파싱된 데이터가 없습니다.")

    # 파싱 성공했지만 체크가 필요한 데이터 출력
    if len(df_parsed_needs_check) > 0:
        print("\n\n⚠️  파싱 성공 데이터 - 체크 필요 (단위 명시 없음, 처음 20개):")
        print("-" * 80)
        display_cols = ['disp_nm1', 'disp_nm2', 'disp_nm3', 'disp_nm4', 'dimension_type', 'parsed_value', 'value']
        available_cols = [col for col in display_cols if col in df_parsed_needs_check.columns]
        print(df_parsed_needs_check[available_cols].head(20).to_string())
    else:
        print("\n체크가 필요한 데이터가 없습니다.")

    # 파싱 실패한 데이터 출력
    if len(df_unparsed) > 0:
        print("\n\n❌ 파싱 실패 데이터 (처음 20개):")
        print("-" * 80)
        display_cols = ['disp_nm1', 'disp_nm2', 'disp_nm3', 'disp_nm4', 'value']
        available_cols = [col for col in display_cols if col in df_unparsed.columns]
        print(df_unparsed[available_cols].head(20).to_string())

        # 파싱 실패 패턴 분석
        print("\n\n❌ 파싱 실패 패턴 분석 (disp_nm2별 개수):")
        print("-" * 80)
        print(df_unparsed['disp_nm2'].value_counts().head(10))
    else:
        print("\n\n모든 데이터가 성공적으로 파싱되었습니다!")

def get_user_input():
    """사용자로부터 실행 파라미터 입력받기"""
    print("\n" + "="*80)
    print("스펙 데이터 변환 파이프라인 설정")
    print("="*80)

    # 소스 테이블
    source_table = input("\n소스 테이블명을 입력하세요 [기본값: test_spec_01]: ").strip()
    if not source_table:
        source_table = "test_spec_01"

    # 타겟 테이블
    target_table = input("타겟 테이블명을 입력하세요 [기본값: test_spec_02]: ").strip()
    if not target_table:
        target_table = "test_spec_02"

    # 필터링 옵션
    use_filter = input("\ndisp_nm1 필터링을 사용하시겠습니까? (y/n) [기본값: y]: ").strip().lower()
    if use_filter != 'n':
        print("\n기본 필터 값:")
        for i, val in enumerate(DEFAULT_ALLOWED_DISP_NM1, 1):
            print(f"  {i}. {val}")

        use_default = input("\n기본 필터 값을 사용하시겠습니까? (y/n) [기본값: y]: ").strip().lower()
        if use_default == 'n':
            custom_filter = input("필터링할 disp_nm1 값들을 쉼표로 구분하여 입력하세요: ").strip()
            allowed_disp_nm1 = [v.strip() for v in custom_filter.split(',') if v.strip()]
        else:
            allowed_disp_nm1 = DEFAULT_ALLOWED_DISP_NM1
    else:
        allowed_disp_nm1 = None

    # Truncate 옵션
    truncate = input("\n타겟 테이블의 기존 데이터를 삭제하시겠습니까? (y/n) [기본값: y]: ").strip().lower()
    truncate_before_insert = truncate != 'n'

    # 상세 출력 옵션
    verbose = input("\n파싱 결과를 상세히 출력하시겠습니까? (y/n) [기본값: y]: ").strip().lower()
    verbose = verbose != 'n'

    return source_table, target_table, allowed_disp_nm1, truncate_before_insert, verbose

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='PostgreSQL 스펙 데이터 변환 파이프라인',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
예제:
  python transform_spec.py                     # 대화형 모드
  python transform_spec.py --source-table test_spec_01 --target-table test_spec_02
  python transform_spec.py --no-filter --no-truncate
        '''
    )

    parser.add_argument('--source-table', '-s', type=str, help='소스 테이블명')
    parser.add_argument('--target-table', '-t', type=str, help='타겟 테이블명')
    parser.add_argument('--filter', nargs='+', help='필터링할 disp_nm1 값 리스트')
    parser.add_argument('--no-filter', action='store_true', help='필터링 없이 전체 데이터 처리')
    parser.add_argument('--no-truncate', action='store_true', help='타겟 테이블 기존 데이터 유지')
    parser.add_argument('--quiet', '-q', action='store_true', help='간략한 출력만 표시')

    args = parser.parse_args()

    # 명령행 인자가 제공되지 않은 경우 대화형 모드
    if not args.source_table and not args.target_table:
        print("\n🚀 PostgreSQL 스펙 데이터 변환 파이프라인")
        print("="*80)
        source_table, target_table, allowed_disp_nm1, truncate_before_insert, verbose = get_user_input()
    else:
        # 명령행 인자 사용
        source_table = args.source_table or "test_spec_01"
        target_table = args.target_table or "test_spec_02"

        if args.no_filter:
            allowed_disp_nm1 = None
        elif args.filter:
            allowed_disp_nm1 = args.filter
        else:
            allowed_disp_nm1 = DEFAULT_ALLOWED_DISP_NM1

        truncate_before_insert = not args.no_truncate
        verbose = not args.quiet

    # 설정 확인
    print("\n" + "="*80)
    print("실행 설정 확인")
    print("="*80)
    print(f"소스 테이블: {source_table}")
    print(f"타겟 테이블: {target_table}")
    print(f"필터링: {allowed_disp_nm1 if allowed_disp_nm1 else '없음'}")
    print(f"기존 데이터 삭제: {'예' if truncate_before_insert else '아니오'}")
    print(f"상세 출력: {'예' if verbose else '아니오'}")

    confirm = input("\n계속 진행하시겠습니까? (y/n): ").strip().lower()
    if confirm != 'y':
        print("작업을 취소했습니다.")
        return

    # 파이프라인 실행
    success = process_spec_data(
        source_table=source_table,
        target_table=target_table,
        allowed_disp_nm1=allowed_disp_nm1,
        truncate_before_insert=truncate_before_insert,
        verbose=verbose
    )

    # 종료 코드 반환
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()