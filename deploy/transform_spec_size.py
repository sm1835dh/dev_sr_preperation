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
   python transform_spec_size.py

2. mod 테이블 데이터 유지하며 실행:
   python transform_spec_size.py --no-truncate

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
from datetime import datetime
import time

# .env 파일 로드
load_dotenv()

# 테이블 이름 정의
STAGING_TABLE = 'kt_spec_validation_table_v03_20251023_staging'
SOURCE_TABLE = 'kt_spec_validation_table_v03_20251023'
MOD_TABLE = 'kt_spec_validation_table_v03_20251023_result'

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
    staging 테이블의 is_completed 값, dimension_type, from_disp_nm2 업데이트

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

                    # from_disp_nm1, from_disp_nm2 정보 준비 (디버깅용)
                    from_disp_nm1 = str(rule['disp_nm1']) if pd.notna(rule['disp_nm1']) else None
                    from_disp_nm2 = str(rule['disp_nm2']) if pd.notna(rule['disp_nm2']) else None

                    # 파싱 성공한 경우 is_completed, dimension_type, from_disp_nm1, from_disp_nm2 업데이트
                    conditions = []
                    params = {}

                    for idx, col in enumerate(['disp_lv1', 'disp_lv2', 'disp_lv3', 'disp_nm1', 'disp_nm2']):
                        if pd.notna(rule[col]):
                            conditions.append(f"{col} = :param_{idx}")
                            params[f'param_{idx}'] = rule[col]
                        else:
                            conditions.append(f"{col} IS NULL")

                    where_clause = " AND ".join(conditions)

                    # 모든 정보 포함한 업데이트 (PostgreSQL 문법)
                    params['from_disp_nm1'] = from_disp_nm1
                    params['from_disp_nm2'] = from_disp_nm2

                    if dimension_str:
                        params['dimension_type'] = dimension_str
                        update_query = text(f"""
                            UPDATE {STAGING_TABLE}
                            SET is_completed = true,
                                dimension_type = :dimension_type,
                                from_disp_nm1 = :from_disp_nm1,
                                from_disp_nm2 = :from_disp_nm2
                            WHERE {where_clause}
                        """)
                    else:
                        update_query = text(f"""
                            UPDATE {STAGING_TABLE}
                            SET is_completed = true,
                                from_disp_nm1 = :from_disp_nm1,
                                from_disp_nm2 = :from_disp_nm2
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
    파싱 결과를 mod 테이블에 저장 (중복 체크 포함)

    Parameters:
    - engine: SQLAlchemy engine
    - df_parsed: 파싱된 데이터 DataFrame

    Returns:
    - tuple: (success, duplicate_count)
    """
    try:
        if len(df_parsed) == 0:
            print("⚠️ 저장할 파싱 데이터가 없습니다.")
            return True, 0

        # 중복 체크를 위한 키 컬럼들
        duplicate_check_cols = [
            'mdl_code', 'goods_nm', 'category_lv1', 'category_lv2',
            'disp_nm1', 'disp_nm2', 'value', 'target_disp_nm2',
            'dimension_type', 'parsed_value'
        ]

        # 기존 데이터 조회 (중복 체크용)
        try:
            existing_query = f"""
                SELECT mdl_code, goods_nm, category_lv1, category_lv2,
                       disp_nm1, disp_nm2, value, target_disp_nm2,
                       dimension_type, parsed_value
                FROM {MOD_TABLE}
            """
            df_existing = pd.read_sql(existing_query, engine)
        except Exception as e:
            # 테이블이 없거나 비어있는 경우
            df_existing = pd.DataFrame(columns=duplicate_check_cols)

        # 파싱된 데이터를 저장 형식으로 준비
        rows_to_insert = []
        duplicate_count = 0

        for _, row in df_parsed.iterrows():
            row_dict = row.to_dict()

            # 중복 체크용 데이터 준비
            check_data = {
                'mdl_code': row_dict.get('mdl_code'),
                'goods_nm': row_dict.get('goods_nm'),
                'category_lv1': row_dict.get('category_lv1'),
                'category_lv2': row_dict.get('category_lv2'),
                'disp_nm1': row_dict.get('disp_nm1'),
                'disp_nm2': row_dict.get('disp_nm2'),
                'value': row_dict.get('value'),
                'target_disp_nm2': row_dict.get('target_disp_nm2'),
                'dimension_type': row_dict.get('dimension_type'),
                'parsed_value': row_dict.get('parsed_value')
            }

            # 중복 체크
            is_duplicate = False
            if len(df_existing) > 0:
                # 모든 키 컬럼이 일치하는지 확인
                mask = True
                for col in duplicate_check_cols:
                    # NaN 처리를 위한 특별 로직
                    col_value = check_data.get(col)
                    if pd.isna(col_value):
                        mask = mask & df_existing[col].isna()
                    else:
                        mask = mask & (df_existing[col] == col_value)

                if mask.any():
                    is_duplicate = True
                    duplicate_count += 1

            if not is_duplicate:
                # mod 테이블에 저장할 전체 데이터 준비
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
                    'dimension_type': row_dict.get('dimension_type'),
                    'parsed_value': row_dict.get('parsed_value'),
                    'needs_check': row_dict.get('needs_check', False)
                }
                rows_to_insert.append(insert_data)

                # 메모리상의 기존 데이터에도 추가 (후속 중복 체크를 위해)
                df_existing = pd.concat([df_existing, pd.DataFrame([check_data])], ignore_index=True)

        # 새로운 데이터만 저장
        if len(rows_to_insert) > 0:
            df_to_save = pd.DataFrame(rows_to_insert)
            df_to_save.to_sql(MOD_TABLE, engine, if_exists='append', index=False)
            print(f"✅ {len(rows_to_insert)}개의 새로운 데이터를 저장했습니다.")
        else:
            print("ℹ️ 모든 데이터가 이미 존재합니다. 새로운 데이터가 없습니다.")

        if duplicate_count > 0:
            print(f"⚠️ {duplicate_count}개의 중복 데이터는 건너뛰었습니다.")

        # validation_rule_id별로 dimension_type 집계 (사용자 확인용)
        rule_summary = df_parsed.groupby('validation_rule_id')['dimension_type'].apply(
            lambda x: sorted(list(set(x)))
        )

        print(f"📊 처리된 규칙별 dimension 타입:")
        for rule_id, dimensions in rule_summary.items():
            print(f"   - {dimensions}")

        return True, duplicate_count

    except Exception as e:
        print(f"❌ Mod 테이블 저장 실패: {e}")
        import traceback
        traceback.print_exc()
        return False, 0


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
    # 예: "TOP/BOTTOM : 730.8(L), 17.7(W) x 24.6(H) mm, LEFT/RIGHT : 425.3(L), 17.7(W) x 24.6(H) mm"
    # → 첫 번째 세트만 추출

    # 복수 세트 감지: LEFT/RIGHT/TOP/BOTTOM 키워드가 2번 이상 나타나는지 확인
    direction_keywords = ['LEFT', 'RIGHT', 'TOP', 'BOTTOM']
    keyword_count = sum(1 for keyword in direction_keywords if keyword in value.upper())

    if keyword_count >= 2:
        # 복수 세트가 있는 경우
        # 정규식으로 첫 번째 세트 추출: "라벨 : 값들" 패턴에서 다음 라벨 앞까지
        # LEFT/RIGHT/TOP/BOTTOM 키워드 앞에서 분리
        first_set_match = re.search(
            r'(?:TOP|BOTTOM|LEFT|RIGHT)[^:]*:\s*([^:]+?)(?=\s*(?:,\s*)?(?:LEFT|RIGHT|TOP|BOTTOM)|$)',
            value,
            re.IGNORECASE
        )

        if first_set_match:
            # 첫 번째 세트의 값 부분만 추출
            extracted_value = first_set_match.group(1).strip()
            # 끝에 있는 불필요한 콤마 제거
            if extracted_value.endswith(','):
                extracted_value = extracted_value[:-1].strip()
            value = extracted_value
        else:
            # Fallback: 콜론이 있으면 첫 번째 콜론 다음부터 두 번째 방향 키워드까지
            if ':' in value:
                # 콜론 뒤의 내용 추출
                after_colon = value.split(':', 1)[1]
                # 두 번째 방향 키워드 찾기
                second_keyword_match = re.search(r'(LEFT|RIGHT|TOP|BOTTOM)', after_colon, re.IGNORECASE)
                if second_keyword_match:
                    value = after_colon[:second_keyword_match.start()].strip()
                    # 끝에 콤마가 있으면 제거
                    if value.endswith(','):
                        value = value[:-1].strip()
                else:
                    value = after_colon.strip()
    else:
        # 단일 세트인 경우 - 콜론이 있고 방향 키워드가 있으면 콜론 뒤의 값만 추출
        if ':' in value and any(keyword in value.upper() for keyword in direction_keywords):
            value = value.split(':', 1)[1].strip()

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
    # 전체 수행 시간 측정 시작
    start_time = time.time()

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

        # 파싱 실패 데이터 상세 출력
        if len(df_unparsed) > 0:
            print("\n" + "="*80)
            print(f"❌ 파싱 실패 데이터 상세 ({len(df_unparsed)}개 행)")
            print("="*80)

            # 화면 출력
            display_cols = ['mdl_code', 'goods_nm', 'disp_nm1', 'disp_nm2', 'value', 'validation_rule_id']
            available_cols = [col for col in display_cols if col in df_unparsed.columns]
            print(df_unparsed[available_cols].to_string(index=False))

            # CSV 파일로 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            failed_file = f"parsing_failed_{timestamp}.csv"
            df_unparsed[available_cols].to_csv(failed_file, index=False, encoding='utf-8-sig')
            print(f"\n💾 파싱 실패 데이터를 '{failed_file}' 파일로 저장했습니다.")
            print("="*80)

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

        success_mod, duplicate_count = save_to_mod_table(engine, df_parsed)

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

        # 전체 수행 시간 계산
        end_time = time.time()
        elapsed_time = end_time - start_time
        elapsed_minutes = int(elapsed_time // 60)
        elapsed_seconds = elapsed_time % 60

        if success:
            print("\n" + "="*80)
            print("✅ 전체 작업 완료!")
            print("="*80)
            print(f"📊 요약:")
            print(f"  - 처리된 검증 규칙: {successful_rules}/{total_rules}개")
            print(f"  - 파싱된 dimension 값: {len(df_parsed)}개")

            # DB에서 실제 저장된 데이터를 기준으로 (mdl_code, goods_nm)별 통계 생성
            print(f"\n📈 제품별 DB 저장 rows 통계 (mdl_code + goods_nm 조합 기준):")

            # 현재 처리된 제품들의 조합
            unique_products = df_parsed[['mdl_code', 'goods_nm']].drop_duplicates()

            try:
                # DB에서 실제 저장된 데이터 조회 - mdl_code와 goods_nm 조합으로
                actual_stats_query = f"""
                    SELECT mdl_code, goods_nm, COUNT(*) as row_count
                    FROM {MOD_TABLE}
                    GROUP BY mdl_code, goods_nm
                """
                df_actual_stats = pd.read_sql(actual_stats_query, engine)

                # 현재 처리된 제품들만 필터링
                df_actual_stats_filtered = df_actual_stats.merge(
                    unique_products,
                    on=['mdl_code', 'goods_nm'],
                    how='inner'
                )

                if len(df_actual_stats_filtered) > 0:
                    # DB 기준 통계 계산
                    df_actual_stats_filtered['product_key'] = df_actual_stats_filtered['mdl_code'] + '_' + df_actual_stats_filtered['goods_nm']
                    product_row_counts = df_actual_stats_filtered.set_index('product_key')['row_count']
                    product_stats = product_row_counts.value_counts().sort_index()

                    print(f"  DB에 실제 저장된 제품별 통계:")
                    for row_count, product_count in product_stats.items():
                        print(f"  - {row_count}개 row 저장: {product_count}개 제품")
                    print(f"  - 전체 제품 수: {len(df_actual_stats_filtered)}개")

                    # mdl_code_row_counts를 product 기준으로 재설정
                    mdl_code_row_counts = product_row_counts
                else:
                    # 파싱된 데이터 기준으로 fallback
                    df_parsed['product_key'] = df_parsed['mdl_code'] + '_' + df_parsed['goods_nm']
                    product_row_counts = df_parsed.groupby('product_key').size()
                    product_stats = product_row_counts.value_counts().sort_index()

                    print(f"  파싱된 데이터 기준 (DB 조회 실패):")
                    for row_count, product_count in product_stats.items():
                        print(f"  - {row_count}개 row 생성: {product_count}개 제품")
                    print(f"  - 전체 제품 수: {df_parsed[['mdl_code', 'goods_nm']].drop_duplicates().shape[0]}개")

                    mdl_code_row_counts = product_row_counts

            except Exception as e:
                print(f"  ⚠️ DB 통계 조회 실패: {e}")
                # 파싱된 데이터 기준으로 fallback
                df_parsed['product_key'] = df_parsed['mdl_code'] + '_' + df_parsed['goods_nm']
                product_row_counts = df_parsed.groupby('product_key').size()
                product_stats = product_row_counts.value_counts().sort_index()

                print(f"  파싱된 데이터 기준:")
                for row_count, product_count in product_stats.items():
                    print(f"  - {row_count}개 row 생성: {product_count}개 제품")
                print(f"  - 전체 제품 수: {df_parsed[['mdl_code', 'goods_nm']].drop_duplicates().shape[0]}개")

                mdl_code_row_counts = product_row_counts

            # 3개가 아닌 제품들 출력 (DB 기준 또는 파싱 데이터 기준)
            if len(df_parsed) > 0:
                # mdl_code_row_counts가 위에서 설정되었는지 확인 (이제 product_key 기준)
                if 'mdl_code_row_counts' not in locals():
                    df_parsed['product_key'] = df_parsed['mdl_code'] + '_' + df_parsed['goods_nm']
                    mdl_code_row_counts = df_parsed.groupby('product_key').size()

                # 타임스탬프를 미리 생성
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                non_three_products = mdl_code_row_counts[mdl_code_row_counts != 3]
                if len(non_three_products) > 0:
                    print(f"\n⚠️ 3개가 아닌 row를 가진 제품 목록 ({len(non_three_products)}개):")

                    # row 수별로 그룹화하여 출력
                    for row_count in sorted(non_three_products.unique()):
                        products_with_count = non_three_products[non_three_products == row_count].index.tolist()
                        print(f"\n  [{row_count}개 row 저장] - {len(products_with_count)}개 제품:")

                        # 제품별 데이터 정보 출력
                        for product_key in products_with_count[:10]:  # 처음 10개만 표시
                            # product_key에서 mdl_code 추출
                            if 'product_key' not in df_parsed.columns:
                                df_parsed['product_key'] = df_parsed['mdl_code'] + '_' + df_parsed['goods_nm']

                            # 해당 제품의 데이터 샘플 정보 가져오기
                            product_data = df_parsed[df_parsed['product_key'] == product_key]
                            if len(product_data) > 0:
                                sample_data = product_data.iloc[0]
                                mdl_code = sample_data.get('mdl_code', 'N/A')
                                goods_nm = sample_data.get('goods_nm', 'N/A')
                                disp_nm2 = sample_data.get('disp_nm2', 'N/A')
                                value = sample_data.get('value', 'N/A')

                                # dimension_types 리스트
                                dimension_types = product_data['dimension_type'].tolist()

                                print(f"    • {mdl_code}: {goods_nm[:30]}... | {disp_nm2[:20]}...")
                                print(f"      값: {value[:50]}..." if len(str(value)) > 50 else f"      값: {value}")
                                print(f"      파싱된 타입: {dimension_types}")

                        if len(products_with_count) > 10:
                            print(f"    ... 외 {len(products_with_count) - 10}개 더 있음")

                    # 3개가 아닌 row를 가진 제품 목록을 파일로 저장
                    non_standard_file = f"non_standard_products_{timestamp}.csv"

                    # 데이터 준비 - DB 실제 데이터와 파싱 데이터 병합
                    non_standard_data = []

                    if 'product_key' not in df_parsed.columns:
                        df_parsed['product_key'] = df_parsed['mdl_code'] + '_' + df_parsed['goods_nm']

                    for product_key in non_three_products.index:
                        # 파싱 데이터에서 정보 가져오기
                        product_data = df_parsed[df_parsed['product_key'] == product_key]
                        # DB 기준 row count 사용
                        row_count = non_three_products[product_key]

                        if len(product_data) > 0:
                            sample_row = product_data.iloc[0]
                            dimension_types = sorted(product_data['dimension_type'].tolist())

                            non_standard_data.append({
                                'mdl_code': sample_row.get('mdl_code', ''),
                                'goods_nm': sample_row.get('goods_nm', ''),
                                'disp_nm1': sample_row.get('disp_nm1', ''),
                                'disp_nm2': sample_row.get('disp_nm2', ''),
                                'value': sample_row.get('value', ''),
                                'target_disp_nm2': sample_row.get('target_disp_nm2', ''),
                                'row_count': row_count,
                                'dimension_types': ', '.join(dimension_types),
                                'category_lv1': sample_row.get('category_lv1', ''),
                                'category_lv2': sample_row.get('category_lv2', '')
                            })

                    # CSV로 저장
                    if len(non_standard_data) > 0:
                        df_non_standard = pd.DataFrame(non_standard_data)
                        df_non_standard = df_non_standard.sort_values(['row_count', 'mdl_code', 'goods_nm'])
                        df_non_standard.to_csv(non_standard_file, index=False, encoding='utf-8-sig')

                        print(f"\n💾 3개가 아닌 row를 가진 제품 목록을 '{non_standard_file}' 파일로 저장했습니다.")
                        print(f"   총 {len(non_three_products)}개 제품, 파일에는 상세 정보 포함")

                # 모든 제품별 통계를 파일로 저장 (3개 row 포함)
                all_product_stats_file = f"all_product_stats_{timestamp}.csv"
                all_product_data = []

                # product_key 컬럼 확인 및 생성
                if 'product_key' not in df_parsed.columns:
                    df_parsed['product_key'] = df_parsed['mdl_code'] + '_' + df_parsed['goods_nm']

                for product_key in mdl_code_row_counts.index:
                    product_data = df_parsed[df_parsed['product_key'] == product_key]
                    row_count = mdl_code_row_counts[product_key]

                    if len(product_data) > 0:
                        sample_row = product_data.iloc[0]
                        dimension_types = sorted(product_data['dimension_type'].tolist())

                        all_product_data.append({
                            'mdl_code': sample_row.get('mdl_code', ''),
                            'goods_nm': sample_row.get('goods_nm', ''),
                            'category_lv1': sample_row.get('category_lv1', ''),
                            'category_lv2': sample_row.get('category_lv2', ''),
                            'disp_nm1': sample_row.get('disp_nm1', ''),
                            'disp_nm2': sample_row.get('disp_nm2', ''),
                            'value': sample_row.get('value', ''),
                            'target_disp_nm2': sample_row.get('target_disp_nm2', ''),
                            'row_count': row_count,
                            'is_standard': 'O' if row_count == 3 else 'X',
                            'dimension_types': ', '.join(dimension_types)
                        })

                # DataFrame 생성 및 저장
                if len(all_product_data) > 0:
                    df_all_stats = pd.DataFrame(all_product_data)
                    df_all_stats = df_all_stats.sort_values(['is_standard', 'row_count', 'mdl_code', 'goods_nm'])
                    df_all_stats.to_csv(all_product_stats_file, index=False, encoding='utf-8-sig')

                    print(f"💾 전체 제품별 통계를 '{all_product_stats_file}' 파일로 저장했습니다.")
                    print(f"   총 {len(mdl_code_row_counts)}개 제품(mdl_code + goods_nm)의 상세 정보 포함")

            print(f"\n  - Staging 테이블 업데이트: 완료")
            print(f"  - Mod 테이블 저장: 완료")
            if duplicate_count > 0:
                print(f"  - 중복으로 건너뛴 데이터: {duplicate_count}개")
            print(f"  - 전체 수행 시간: {elapsed_minutes}분 {elapsed_seconds:.2f}초")
            return True
        else:
            print("\n❌ 데이터 저장 실패")
            print(f"⏱️  전체 수행 시간: {elapsed_minutes}분 {elapsed_seconds:.2f}초")
            return False

    except Exception as e:
        # 오류 발생 시에도 수행 시간 출력
        end_time = time.time()
        elapsed_time = end_time - start_time
        elapsed_minutes = int(elapsed_time // 60)
        elapsed_seconds = elapsed_time % 60

        print(f"\n❌ 처리 중 오류 발생: {e}")
        print(f"⏱️  수행 시간 (오류 발생 시점까지): {elapsed_minutes}분 {elapsed_seconds:.2f}초")
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
  python transform_spec_size.py                     # 기본 실행
  python transform_spec_size.py --no-truncate       # mod 테이블 데이터 유지
  python transform_spec_size.py --quiet             # 간략한 출력
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