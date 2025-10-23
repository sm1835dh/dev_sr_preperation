#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PostgreSQL 스펙 데이터 변환 파이프라인 - 검증 규칙 기반 (Validation Rule Based)
================================================================================

이 스크립트는 검증 규칙 테이블(staging)을 기반으로 PostgreSQL 테이블의 스펙 데이터에서
dimension (width, height, depth) 정보를 파싱하고 변환합니다.

테이블 구조:
-----------
1. kt_spec_validation_table_20251021_staging: 검증 규칙 테이블
   - is_target=true인 레코드만 처리
   - is_completed: 파싱 완료 여부

2. kt_spec_validation_table_20251021: 소스 데이터 테이블
   - 실제 스펙 데이터 포함

3. kt_spec_validation_table_20251021_mod: 파싱 결과 테이블
   - target_disp_nm2: 사용자 정의 명칭
   - dimension_type: ['depth', 'height', 'width'] 형식의 리스트
   - is_target, is_completed: 모두 true로 설정

사용법:
------
1. 기본 실행:
   python transform_spec.py

2. mod 테이블 데이터 유지하며 실행:
   python transform_spec.py --no-truncate

필수 환경 변수 (.env 파일에 설정):
--------------------------------
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_database
PG_USER=your_username
PG_PASSWORD=your_password

주요 기능:
---------
1. staging 테이블에서 is_target=true인 검증 규칙 로드
2. 소스 테이블에서 매칭되는 데이터 조회
3. 다양한 형식의 dimension 데이터 파싱
4. mod 테이블에 파싱 결과 저장
5. staging 테이블의 is_completed 업데이트

================================================================================
"""

import os
import sys
import argparse
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 테이블 이름 정의
STAGING_TABLE = 'kt_spec_validation_table_20251021_staging'
SOURCE_TABLE = 'kt_spec_validation_table_20251021'
MOD_TABLE = 'kt_spec_validation_table_20251021_mod'

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

def load_validation_rules(engine):
    """
    staging 테이블에서 is_target=true인 validation 규칙 로드

    Parameters:
    - engine: SQLAlchemy engine

    Returns:
    - DataFrame with validation rules
    """
    try:
        query = f"""
        SELECT disp_lv1, disp_lv2, disp_lv3, disp_nm1, disp_nm2,
               target_disp_nm2, dimension_type, is_target, is_completed
        FROM {STAGING_TABLE}
        WHERE is_target = true AND (is_completed = false OR is_completed IS NULL)
        """
        df = pd.read_sql(query, engine)
        print(f"✅ 검증 규칙 {len(df)}개 로드 완료 (is_target=true, is_completed=false)")
        return df
    except Exception as e:
        print(f"❌ 검증 규칙 로드 실패: {e}")
        return None

def load_data_with_validation_rules(engine, validation_rules_df):
    """
    validation 규칙에 매칭되는 데이터를 소스 테이블에서 로드

    Parameters:
    - engine: SQLAlchemy engine
    - validation_rules_df: 검증 규칙 DataFrame

    Returns:
    - DataFrame with matched data and validation rules
    """
    try:
        all_data = []

        for _, rule in validation_rules_df.iterrows():
            # NULL 값 처리
            conditions = []
            params = {}

            for idx, col in enumerate(['disp_lv1', 'disp_lv2', 'disp_lv3', 'disp_nm1', 'disp_nm2']):
                if pd.notna(rule[col]):
                    conditions.append(f"{col} = :param_{idx}")
                    params[f'param_{idx}'] = rule[col]
                else:
                    conditions.append(f"{col} IS NULL")

            where_clause = " AND ".join(conditions)
            query = text(f"SELECT * FROM {SOURCE_TABLE} WHERE {where_clause}")

            df_part = pd.read_sql(query, engine, params=params)

            # validation 규칙 정보 추가
            df_part['target_disp_nm2'] = rule['target_disp_nm2']
            df_part['validation_rule_id'] = f"{rule['disp_lv1']}|{rule['disp_lv2']}|{rule['disp_lv3']}|{rule['disp_nm1']}|{rule['disp_nm2']}"

            all_data.append(df_part)

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            print(f"✅ 검증 규칙에 매칭되는 {len(df_combined)}개 데이터 로드 완료")
            return df_combined
        else:
            print("⚠️ 매칭되는 데이터가 없습니다.")
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        import traceback
        traceback.print_exc()
        return None



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


# ============================================
# 파싱 함수 정의
# ============================================

def update_staging_table(engine, validation_rules_df, parsed_results, dimension_summaries):
    """
    staging 테이블의 is_completed 값과 dimension_type 업데이트

    Parameters:
    - engine: SQLAlchemy engine
    - validation_rules_df: 처리한 검증 규칙
    - parsed_results: 파싱 결과 딕셔너리 {validation_rule_id: success}
    - dimension_summaries: {validation_rule_id: ['depth', 'width', ...]}
    """
    try:
        with engine.begin() as conn:
            for _, rule in validation_rules_df.iterrows():
                rule_id = f"{rule['disp_lv1']}|{rule['disp_lv2']}|{rule['disp_lv3']}|{rule['disp_nm1']}|{rule['disp_nm2']}"

                if rule_id in parsed_results and parsed_results[rule_id]:
                    # dimension_type 리스트 가져오기
                    dimension_list = dimension_summaries.get(rule_id, [])
                    dimension_str = str(dimension_list) if dimension_list else None

                    # 파싱 성공한 경우 is_completed와 dimension_type 업데이트
                    conditions = []
                    params = {}

                    for idx, col in enumerate(['disp_lv1', 'disp_lv2', 'disp_lv3', 'disp_nm1', 'disp_nm2']):
                        if pd.notna(rule[col]):
                            conditions.append(f"{col} = :param_{idx}")
                            params[f'param_{idx}'] = rule[col]
                        else:
                            conditions.append(f"{col} IS NULL")

                    where_clause = " AND ".join(conditions)

                    if dimension_str:
                        params['dimension_type'] = dimension_str
                        update_query = text(f"""
                            UPDATE {STAGING_TABLE}
                            SET is_completed = true,
                                dimension_type = :dimension_type
                            WHERE {where_clause}
                        """)
                    else:
                        update_query = text(f"""
                            UPDATE {STAGING_TABLE}
                            SET is_completed = true
                            WHERE {where_clause}
                        """)

                    conn.execute(update_query, params)

        print(f"✅ Staging 테이블 업데이트 완료 ({len([v for v in parsed_results.values() if v])}개 규칙 완료)")
        return True

    except Exception as e:
        print(f"❌ Staging 테이블 업데이트 실패: {e}")
        return False

def save_to_mod_table(engine, df_parsed):
    """
    파싱 결과를 mod 테이블에 저장

    Parameters:
    - engine: SQLAlchemy engine
    - df_parsed: 파싱된 데이터 DataFrame
    """
    try:
        if len(df_parsed) == 0:
            print("⚠️ 저장할 파싱 데이터가 없습니다.")
            return True

        # 파싱된 데이터를 직접 저장 (dimension_type별로 row 생성)
        rows_to_insert = []

        for _, row in df_parsed.iterrows():
            row_dict = row.to_dict()

            # mod 테이블에 저장할 데이터 준비
            insert_data = {
                'mdl_code': row_dict.get('mdl_code'),
                'goods_nm': row_dict.get('goods_nm'),
                'disp_lv1': row_dict.get('disp_lv1'),
                'disp_lv2': row_dict.get('disp_lv2'),
                'disp_lv3': row_dict.get('disp_lv3'),
                'category_lv1': row_dict.get('category_lv1'),
                'category_lv2': row_dict.get('category_lv2'),
                'category_lv3': row_dict.get('category_lv3'),
                'disp_nm1': row_dict.get('disp_nm1'),
                'disp_nm2': row_dict.get('disp_nm2'),
                'value': row_dict.get('value'),
                'is_numeric': row_dict.get('is_numeric'),
                'symbols': row_dict.get('symbols'),
                'new_value': row_dict.get('new_value'),
                'target_disp_nm2': row_dict.get('target_disp_nm2'),
                'dimension_type': row_dict.get('dimension_type'),  # 개별 dimension type (width, height, depth)
                'parsed_value': row_dict.get('parsed_value'),
                'needs_check': row_dict.get('needs_check', False)
            }
            rows_to_insert.append(insert_data)

        # DataFrame으로 변환하여 한번에 저장
        df_to_save = pd.DataFrame(rows_to_insert)

        # 테이블에 저장
        df_to_save.to_sql(MOD_TABLE, engine, if_exists='append', index=False)

        # validation_rule_id별로 dimension_type 집계 (사용자 확인용)
        rule_summary = df_parsed.groupby('validation_rule_id')['dimension_type'].apply(
            lambda x: sorted(list(set(x)))
        )

        print(f"✅ Mod 테이블에 {len(df_to_save)}개 dimension 값 저장 완료")
        print(f"📊 처리된 규칙별 dimension 타입:")
        for rule_id, dimensions in rule_summary.items():
            print(f"   - {dimensions}")

        return True

    except Exception as e:
        print(f"❌ Mod 테이블 저장 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def identify_dimension_type(text):
    """
    텍스트에서 dimension 타입을 식별

    Parameters:
    -----------
    text : str
        분석할 텍스트 (disp_nm2)
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
    dimension 파싱 함수

    validation_rule에 따라 데이터를 파싱
    (화이트리스트 체크 제거 - staging 테이블의 is_target으로 대체)
    """
    parsed_rows = []
    value = str(row['value'])
    disp_nm2 = str(row.get('disp_nm2', ''))

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
        dim_type = identify_dimension_type(disp_nm2)
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

def process_spec_data_with_validation(engine, truncate_before_insert=True, verbose=True):
    """
    검증 규칙 기반 스펙 데이터 처리 파이프라인 실행

    Parameters:
    -----------
    engine : SQLAlchemy engine
        데이터베이스 연결
    truncate_before_insert : bool
        True이면 mod 테이블의 기존 데이터 삭제 후 삽입
    verbose : bool
        상세 출력 여부

    Returns:
    --------
    bool : 성공 여부
    """
    try:
        # 1. validation 규칙 로드
        print("\n" + "="*80)
        print("📥 검증 규칙 로드 중...")
        print("="*80)
        validation_rules = load_validation_rules(engine)

        if validation_rules is None or len(validation_rules) == 0:
            print("\n⚠️ 처리할 검증 규칙이 없습니다 (is_target=true, is_completed=false)")
            return True

        # 2. 검증 규칙에 매칭되는 데이터 로드
        print("\n" + "="*80)
        print("📥 소스 데이터 로드 중...")
        print("="*80)
        df_filtered = load_data_with_validation_rules(engine, validation_rules)

        if df_filtered is None or len(df_filtered) == 0:
            print("\n❌ 매칭되는 데이터가 없습니다")
            return False

        # 3. 데이터 파싱
        print("\n" + "="*80)
        print("🔄 데이터 파싱 중...")
        print("="*80)

        parsed_data = []
        parsed_results = {}  # {validation_rule_id: success}
        unparsed_data = []

        for _, row in df_filtered.iterrows():
            parsed_rows, success, needs_check = parse_dimensions_advanced(row)
            rule_id = row['validation_rule_id']

            if success and parsed_rows:
                # validation_rule_id와 target_disp_nm2 추가
                for parsed_row in parsed_rows:
                    parsed_row['validation_rule_id'] = rule_id
                    parsed_row['target_disp_nm2'] = row['target_disp_nm2']

                parsed_data.extend(parsed_rows)
                parsed_results[rule_id] = True
            else:
                unparsed_data.append(row)
                if rule_id not in parsed_results:
                    parsed_results[rule_id] = False

        df_parsed = pd.DataFrame(parsed_data)
        df_unparsed = pd.DataFrame(unparsed_data)

        # 파싱 통계 출력
        successful_rules = len([v for v in parsed_results.values() if v])
        total_rules = len(validation_rules)
        print(f"✅ 파싱 성공: {len(df_parsed)}개 dimension 값")
        print(f"✅ 성공한 검증 규칙: {successful_rules}/{total_rules}개")
        print(f"❌ 파싱 실패: {len(df_unparsed)}개 행")
        print(f"📈 전체 대비 파싱률: {(len(df_parsed) / len(df_filtered) * 100 if len(df_filtered) > 0 else 0):.1f}%")

        # 상세 출력 (verbose 모드)
        if verbose and len(df_parsed) > 0:
            print("\n✅ 파싱 성공 데이터 샘플 (처음 20개):")
            print("-" * 80)
            display_cols = ['mdl_code', 'goods_nm', 'disp_nm1', 'disp_nm2', 'target_disp_nm2', 'dimension_type', 'parsed_value', 'value']
            available_cols = [col for col in display_cols if col in df_parsed.columns]
            print(df_parsed[available_cols].head(20).to_string())

        # 4. Mod 테이블에 저장
        print("\n" + "="*80)
        print("💾 Mod 테이블에 저장 중...")
        print("="*80)

        if truncate_before_insert:
            truncate_table(engine, MOD_TABLE)

        success_mod = save_to_mod_table(engine, df_parsed)

        # 5. Staging 테이블 업데이트
        print("\n" + "="*80)
        print("💾 Staging 테이블 업데이트 중...")
        print("="*80)

        # validation_rule_id별로 dimension_type 집계
        dimension_summaries = {}
        if len(df_parsed) > 0:
            dimension_summaries = df_parsed.groupby('validation_rule_id')['dimension_type'].apply(
                lambda x: sorted(list(set(x)))
            ).to_dict()

        success_staging = update_staging_table(engine, validation_rules, parsed_results, dimension_summaries)

        success = success_mod and success_staging

        if success:
            print("\n" + "="*80)
            print("✅ 전체 작업 완료!")
            print("="*80)
            print(f"📊 요약:")
            print(f"  - 처리된 검증 규칙: {successful_rules}/{total_rules}개")
            print(f"  - 파싱된 dimension 값: {len(df_parsed)}개")
            print(f"  - Staging 테이블 업데이트: 완료")
            print(f"  - Mod 테이블 저장: 완료")
            return True
        else:
            print("\n❌ 데이터 저장 실패")
            return False

    except Exception as e:
        print(f"\n❌ 처리 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False



def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='PostgreSQL 스펙 데이터 변환 파이프라인 (검증 규칙 기반)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
예제:
  python transform_spec.py                     # 기본 실행
  python transform_spec.py --no-truncate       # mod 테이블 데이터 유지
  python transform_spec.py --quiet             # 간략한 출력
        '''
    )

    parser.add_argument('--no-truncate', action='store_true', help='mod 테이블 기존 데이터 유지')
    parser.add_argument('--quiet', '-q', action='store_true', help='간략한 출력만 표시')

    args = parser.parse_args()

    truncate_before_insert = not args.no_truncate
    verbose = not args.quiet

    print("\n🚀 PostgreSQL 스펙 데이터 변환 파이프라인 (검증 규칙 기반)")
    print("="*80)

    # 설정 확인
    print("\n" + "="*80)
    print("실행 설정 확인")
    print("="*80)
    print(f"Staging 테이블: {STAGING_TABLE}")
    print(f"소스 테이블: {SOURCE_TABLE}")
    print(f"Mod 테이블: {MOD_TABLE}")
    print(f"기존 데이터 삭제: {'예' if truncate_before_insert else '아니오'}")
    print(f"상세 출력: {'예' if verbose else '아니오'}")

    confirm = input("\n계속 진행하시겠습니까? (y/n): ").strip().lower()
    if confirm != 'y':
        print("작업을 취소했습니다.")
        return

    # 엔진 생성
    engine = get_sqlalchemy_engine()
    if engine is None:
        print("❌ 데이터베이스 연결 실패")
        sys.exit(1)

    try:
        # 파이프라인 실행
        success = process_spec_data_with_validation(
            engine=engine,
            truncate_before_insert=truncate_before_insert,
            verbose=verbose
        )

        # 종료 코드 반환
        sys.exit(0 if success else 1)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()