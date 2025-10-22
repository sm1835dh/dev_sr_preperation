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
DEFAULT_ALLOWED_DISP_NM1 = ['크기', '규격','사양','외관 사양','기본 사양','외관','기본사양','본체치수','주요사양','일반사양']

# ============================================
# 화이트리스트: disp_nm1 + disp_nm2 조합별 dimension_type 매핑
# ============================================
# 구조: {(disp_nm1, disp_nm2_pattern): dimension_type}
# disp_nm2_pattern은 정확히 일치하거나 포함 여부로 확인

DIMENSION_WHITELIST = {
    # 크기 관련
    ('크기', '본체'): 'product',  # 본체 크기 (width, height, depth 모두 파싱)
    ('크기', '스탠드 포함'): 'product',  # 스탠드 포함 크기
    ('크기', '스탠드포함'): 'product',  # 스탠드포함 크기
    ('크기', '제품'): 'product',  # 제품 크기

    # 규격 관련
    ('규격', '본체'): 'product',
    ('규격', '제품'): 'product',
    ('규격', '크기'): 'product',

    # 사양 관련
    ('사양', '본체 크기'): 'product',
    ('사양', '제품 크기'): 'product',
    ('사양', '외형 크기'): 'product',

    # 외관 사양
    ('외관 사양', '본체'): 'product',
    ('외관 사양', '크기'): 'product',

    # 기본 사양
    ('기본 사양', '크기'): 'product',
    ('기본 사양', '본체'): 'product',

    # 본체치수
    ('본체치수', ''): 'product',  # disp_nm2가 비어있어도 처리
}

# 부분 매칭용 키워드 (disp_nm2에 포함되어 있으면 매칭)
DIMENSION_WHITELIST_CONTAINS = {
    '본체': 'product',
    '제품': 'product',
    '스탠드 포함': 'product',
    '스탠드포함': 'product',
}

# 제외 키워드 (disp_nm2에 이 단어가 포함되어 있으면 파싱 안 함)
DIMENSION_BLACKLIST_KEYWORDS = [
    'gross',
    'Gross',
    'GROSS',
    '패키지',
    '포장',
    '박스',
    '케이스',
    'Buckle Band'
]

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

def is_whitelisted(disp_nm1, disp_nm2):
    """
    disp_nm1과 disp_nm2 조합이 화이트리스트에 있는지 확인
    블랙리스트 키워드가 있으면 무조건 제외

    Parameters:
    -----------
    disp_nm1 : str
        첫 번째 분류명
    disp_nm2 : str
        두 번째 분류명

    Returns:
    --------
    bool : 화이트리스트에 있으면 True, 없으면 False
    """
    if not disp_nm1:
        return False

    disp_nm2 = str(disp_nm2) if disp_nm2 else ''

    # 0. 블랙리스트 체크 (최우선, 무조건 제외)
    for blacklist_keyword in DIMENSION_BLACKLIST_KEYWORDS:
        if blacklist_keyword in disp_nm2:
            return False

    # 1. 정확한 매칭 확인
    if (disp_nm1, disp_nm2) in DIMENSION_WHITELIST:
        return True

    # 2. disp_nm1만 매칭되고 disp_nm2가 비어있는 경우
    if (disp_nm1, '') in DIMENSION_WHITELIST and not disp_nm2:
        return True

    # 3. 부분 매칭 확인 (disp_nm2에 키워드 포함)
    for keyword in DIMENSION_WHITELIST_CONTAINS:
        if keyword in disp_nm2:
            return True

    return False

def analyze_unparsed_patterns(df_unparsed):
    """
    파싱되지 않은 데이터의 disp_nm1, disp_nm2 패턴 분석
    화이트리스트에 추가할 후보를 찾기 위한 함수

    Parameters:
    -----------
    df_unparsed : DataFrame
        파싱되지 않은 데이터

    Returns:
    --------
    DataFrame : (disp_nm1, disp_nm2) 조합별 개수
    """
    if len(df_unparsed) == 0:
        print("파싱되지 않은 데이터가 없습니다.")
        return pd.DataFrame()

    print("\n" + "="*80)
    print("📊 파싱되지 않은 데이터의 disp_nm1 + disp_nm2 패턴 분석")
    print("="*80)

    # disp_nm1, disp_nm2 조합별 카운트
    pattern_counts = df_unparsed.groupby(['disp_nm1', 'disp_nm2']).size().reset_index(name='count')
    pattern_counts = pattern_counts.sort_values('count', ascending=False)

    print("\n상위 20개 패턴:")
    print(pattern_counts.head(20).to_string(index=False))

    print("\n💡 화이트리스트 추가 예시:")
    print("="*80)
    for _, row in pattern_counts.head(10).iterrows():
        print(f"    ('{row['disp_nm1']}', '{row['disp_nm2']}'): 'product',  # {row['count']}개")

    return pattern_counts

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
    elif any(keyword in text_lower for keyword in ['너비', '가로', '폭', 'w']):
        return 'width'
    # Height 키워드
    elif any(keyword in text_lower for keyword in ['세로', '높이', 'h']):
        return 'height'

    return None

def parse_dimensions_advanced(row):
    """
    화이트리스트 기반 dimension 파싱 함수

    1. disp_nm1 + disp_nm2 조합이 화이트리스트에 있는지 확인
    2. 화이트리스트에 있으면 value를 파싱
    3. 화이트리스트에 없으면 파싱하지 않음 (정확성 우선)
    """
    parsed_rows = []
    value = str(row['value'])
    disp_nm1 = str(row.get('disp_nm1', ''))
    disp_nm2 = str(row.get('disp_nm2', ''))
    disp_nm3 = str(row.get('disp_nm3', ''))

    # ============================================
    # 화이트리스트 체크 (최우선)
    # ============================================
    if not is_whitelisted(disp_nm1, disp_nm2):
        # 화이트리스트에 없으면 파싱하지 않음
        return parsed_rows, False, False

    # ============================================
    # 전처리: 제외 조건 체크 및 값 추출
    # ============================================
    # 제외 조건: 각도 조정 관련 텍스트가 포함된 경우
    if any(keyword in value.lower() for keyword in ['각도 조정', '각도조정']):
        return parsed_rows, False, False

    # 복수 개의 값이 있는 경우 첫 번째 값만 추출
    # 예: "TOP/BOTTOM : 1460.0(L) x 24.6(W) x 17.7(H), LEFT/RIGHT : 837.4(L) x 24.6(W) x 17.7(H) mm"
    # → "TOP/BOTTOM : 1460.0(L) x 24.6(W) x 17.7(H)"
    if ':' in value and ',' in value:
        # 콜론과 콤마가 있으면 첫 번째 그룹만 추출
        first_part = value.split(',')[0].strip()
        # "TOP/BOTTOM : 값" 형태에서 값 부분만 추출
        if ':' in first_part:
            value = first_part.split(':', 1)[1].strip()
        else:
            value = first_part

    # 키보드 세트의 경우 첫 번째 제품만 파싱
    if '키보드' in value and ':' in value:
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
    
    # 패턴 2: 한글 키워드로 순서 명시 (우선순위 높음)
    # value 또는 disp_nm2에서 키워드 확인
    # 예: disp_nm2="본체 크기 (너비x두께, mm)", value="7.0 x 2.6"

    combined_text = value + ' ' + disp_nm2  # 두 필드를 합쳐서 키워드 검색

    # 숫자 추출
    nums = re.findall(r'([0-9,]+(?:\.[0-9]+)?)', value)
    base_row = row.to_dict()

    # 키워드 순서 파싱: disp_nm2에서 키워드 순서대로 추출
    # 예: "가로x세로x두께" → ['가로', '세로', '두께']
    keyword_pattern = r'(가로|세로|너비|폭|높이|두께|깊이|길이)'
    keyword_order = re.findall(keyword_pattern, disp_nm2)

    # 키워드가 2개 이상 있고, 숫자도 충분히 있으면 순서대로 매핑
    if len(keyword_order) >= 2 and len(nums) >= len(keyword_order):
        # 한글 키워드 → dimension_type 매핑 (기본값)
        keyword_map = {
            '가로': 'width',
            '너비': 'width',
            '폭': 'width',
            '세로': 'height',   # 기본: 세로=height
            '높이': 'height',
            '두께': 'depth',
            '깊이': 'depth',
            '길이': 'depth',
        }

        # 예외 처리: 특정 조합에서 세로의 의미가 달라짐
        # "가로x높이x세로" 패턴 → 세로를 depth로 해석
        if '높이' in keyword_order and '세로' in keyword_order:
            # 높이와 세로가 함께 있으면, 세로=depth
            keyword_map['세로'] = 'depth'

        try:
            for i, keyword in enumerate(keyword_order):
                if i < len(nums):
                    dim_type = keyword_map.get(keyword)
                    if dim_type:
                        parsed_rows.append({
                            **base_row,
                            'dimension_type': dim_type,
                            'parsed_value': float(nums[i].replace(',', '')),
                            'needs_check': False
                        })

            if parsed_rows:
                return parsed_rows, True, False
        except ValueError:
            pass

    # 키워드 순서 파싱 실패 시, 기존 로직 사용

    # 2-1. 3개 값: 가로x높이x깊이 (명시적)
    if '가로' in combined_text and '높이' in combined_text and '깊이' in combined_text and len(nums) >= 3:
        try:
            parsed_rows.append({**base_row, 'dimension_type': 'width', 'parsed_value': float(nums[0].replace(',', '')), 'needs_check': False})
            parsed_rows.append({**base_row, 'dimension_type': 'height', 'parsed_value': float(nums[1].replace(',', '')), 'needs_check': False})
            parsed_rows.append({**base_row, 'dimension_type': 'depth', 'parsed_value': float(nums[2].replace(',', '')), 'needs_check': False})
            return parsed_rows, True, False
        except ValueError:
            pass

    # 2-2. 2개 값: 너비x두께, 가로x두께, 폭x두께, 가로x깊이 등
    if ('너비' in combined_text or '가로' in combined_text or '폭' in combined_text) and ('두께' in combined_text or '깊이' in combined_text):
        # 높이 키워드가 없어야 함 (우선순위 구분)
        if '높이' not in combined_text and len(nums) >= 2:
            try:
                parsed_rows.append({**base_row, 'dimension_type': 'width', 'parsed_value': float(nums[0].replace(',', '')), 'needs_check': False})
                # 두께/깊이는 depth
                parsed_rows.append({**base_row, 'dimension_type': 'depth', 'parsed_value': float(nums[1].replace(',', '')), 'needs_check': False})
                return parsed_rows, True, False
            except ValueError:
                pass

    # 2-3. 2개 값: 너비x높이, 가로x높이
    if ('너비' in combined_text or '가로' in combined_text or '폭' in combined_text) and '높이' in combined_text and len(nums) >= 2:
        try:
            parsed_rows.append({**base_row, 'dimension_type': 'width', 'parsed_value': float(nums[0].replace(',', '')), 'needs_check': False})
            parsed_rows.append({**base_row, 'dimension_type': 'height', 'parsed_value': float(nums[1].replace(',', '')), 'needs_check': False})
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

        # 파싱 실패 패턴 분석 (화이트리스트에 추가할 후보 찾기)
        analyze_unparsed_patterns(df_unparsed)
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