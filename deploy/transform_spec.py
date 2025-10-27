#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
PostgreSQL 스펙 데이터 변환 파이프라인 - 검증 규칙 기반 (Validation Rule Based)
================================================================================

이 스크립트는 검증 규칙 테이블(staging)을 기반으로 PostgreSQL 테이블의 스펙 데이터를
goal 값에 따라 적절한 파서를 선택하여 파싱하고 변환합니다.

테이블 구조:
-----------
1. kt_spec_validation_table_v03_20251023_staging: 검증 규칙 테이블
   - goal: 파싱 목적 (예: '크기작업', '색상작업' 등)
   - is_target=true인 레코드만 처리
   - is_completed: 파싱 완료 여부

2. kt_spec_validation_table_v03_20251023: 소스 데이터 테이블
   - 실제 스펙 데이터 포함

3. kt_spec_validation_table_v03_20251023_result: 파싱 결과 테이블
   - target_disp_nm2: 사용자 정의 명칭
   - dimension_type: 파싱된 타입 (goal에 따라 다름)
   - parsed_value: 파싱된 값

사용법:
------
1. 기본 실행 (goal 필수):
   python transform_spec.py --goal 크기작업

2. mod 테이블 데이터 유지하며 실행:
   python transform_spec.py --goal 크기작업 --no-truncate

필수 환경 변수 (.env 파일에 설정):
--------------------------------
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=your_database
PG_USER=your_username
PG_PASSWORD=your_password

주요 기능:
---------
1. goal 파라미터에 따른 파서 자동 선택
2. staging 테이블에서 is_target=true이고 goal이 일치하는 검증 규칙 로드
3. 소스 테이블에서 매칭되는 데이터 조회
4. 선택된 파서로 데이터 파싱
5. result 테이블에 파싱 결과 저장
6. staging 테이블의 is_completed 업데이트

지원하는 파서 (goal 값):
--------------------
- '크기작업': 제품 크기 정보 (width, height, depth) 파싱
- (추가 예정) '색상작업', '소재작업', '기능작업' 등

================================================================================
"""

import os
import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import time

# 파서 모듈 임포트
from parsers import get_parser, list_available_parsers

# .env 파일 로드
load_dotenv()

# 테이블 이름 정의
STAGING_TABLE = 'kt_spec_validation_table_v03_20251023_staging'
SOURCE_TABLE = 'kt_spec_validation_table_v03_20251023'
# 개발 중에는 temp_result 사용, 프로덕션에서는 원래 테이블 사용
USE_TEMP_TABLE = True  # 개발 완료 후 False로 변경
MOD_TABLE = 'temp_result' if USE_TEMP_TABLE else 'kt_spec_validation_table_v03_20251023_result'

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

def load_validation_rules(engine, goal):
    """
    staging 테이블에서 is_target=true이고 goal이 일치하는 validation 규칙 로드

    Parameters:
    - engine: SQLAlchemy engine
    - goal: 파싱 목적 (예: '크기작업')

    Returns:
    - DataFrame with validation rules
    """
    try:
        query = text(f"""
        SELECT disp_lv1, disp_lv2, disp_lv3, disp_nm1, disp_nm2,
               target_disp_nm2, dimension_type, is_target, is_completed, goal
        FROM {STAGING_TABLE}
        WHERE is_target = true
          AND goal = :goal
          AND (is_completed = false OR is_completed IS NULL)
        """)
        df = pd.read_sql(query, engine, params={'goal': goal})
        print(f"✅ 검증 규칙 {len(df)}개 로드 완료 (is_target=true, goal='{goal}', is_completed=false)")
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
# 데이터베이스 관련 함수
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

        # parsed_string_value 컬럼이 있는 경우 추가
        if 'parsed_string_value' in df_parsed.columns:
            duplicate_check_cols.append('parsed_string_value')

        # parsed_symbols 컬럼이 있는 경우 추가
        if 'parsed_symbols' in df_parsed.columns:
            duplicate_check_cols.append('parsed_symbols')

        # 기존 데이터 조회 (중복 체크용)
        try:
            # 컬럼 존재 여부 확인 (parsed_string_value, parsed_symbols)
            check_columns_query = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                AND column_name IN ('parsed_string_value', 'parsed_symbols')
            """)
            col_result = pd.read_sql(check_columns_query, engine, params={'table_name': MOD_TABLE.lower()})

            existing_columns = col_result['column_name'].tolist()
            has_string_value = 'parsed_string_value' in existing_columns
            has_symbols = 'parsed_symbols' in existing_columns

            # 기본 컬럼
            select_cols = [
                'mdl_code', 'goods_nm', 'category_lv1', 'category_lv2',
                'disp_nm1', 'disp_nm2', 'value', 'target_disp_nm2',
                'dimension_type', 'parsed_value'
            ]

            # 선택적 컬럼 추가
            if has_string_value:
                select_cols.append('parsed_string_value')
            if has_symbols:
                select_cols.append('parsed_symbols')

            existing_query = f"""
                SELECT {', '.join(select_cols)}
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

            # parsed_string_value가 있는 경우 추가
            if 'parsed_string_value' in row_dict:
                check_data['parsed_string_value'] = row_dict.get('parsed_string_value')

            # parsed_symbols가 있는 경우 추가
            if 'parsed_symbols' in row_dict:
                check_data['parsed_symbols'] = row_dict.get('parsed_symbols')

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
                    'needs_check': row_dict.get('needs_check', False),
                    'goal': row_dict.get('goal')  # goal 필드 추가
                }

                # parsed_string_value가 있는 경우 추가
                if 'parsed_string_value' in row_dict:
                    insert_data['parsed_string_value'] = row_dict.get('parsed_string_value')

                # parsed_symbols가 있는 경우 추가
                if 'parsed_symbols' in row_dict:
                    insert_data['parsed_symbols'] = row_dict.get('parsed_symbols')

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
            lambda x: sorted([item for item in set(x) if item is not None]) + ([None] if None in set(x) else [])
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


def parse_data_with_parser(row, parser):
    """
    주어진 파서를 사용하여 데이터를 파싱

    Parameters:
    -----------
    row : pandas.Series
        파싱할 데이터 행
    parser : BaseParser instance
        사용할 파서 인스턴스

    Returns:
    --------
    tuple : (parsed_rows, success, needs_check)
    """
    if parser is None:
        return [], False, False

    return parser.parse(row)

# ============================================
# 메인 실행 함수
# ============================================

def process_spec_data_with_validation(engine, goal, truncate_before_insert=True, verbose=True):
    """
    검증 규칙 기반 스펙 데이터 처리 파이프라인 실행

    Parameters:
    -----------
    engine : SQLAlchemy engine
        데이터베이스 연결
    goal : str
        파싱 목적 (예: '크기작업')
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
        # 0. 파서 가져오기
        parser = get_parser(goal)
        if parser is None:
            print(f"❌ '{goal}'에 대한 파서를 찾을 수 없습니다.")
            print(f"사용 가능한 파서 목록: {list_available_parsers()}")
            return False

        print(f"✅ '{goal}' 파서 로드 완료")

        # 1. validation 규칙 로드
        print("\n" + "="*80)
        print("📥 검증 규칙 로드 중...")
        print("="*80)
        validation_rules = load_validation_rules(engine, goal)

        if validation_rules is None or len(validation_rules) == 0:
            print(f"\n⚠️ 처리할 검증 규칙이 없습니다 (is_target=true, goal='{goal}', is_completed=false)")
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
            parsed_rows, success, needs_check = parse_data_with_parser(row, parser)
            rule_id = row['validation_rule_id']

            if success and parsed_rows:
                # validation_rule_id, target_disp_nm2, goal 추가
                for parsed_row in parsed_rows:
                    parsed_row['validation_rule_id'] = rule_id
                    parsed_row['target_disp_nm2'] = row['target_disp_nm2']
                    parsed_row['goal'] = goal  # 함수 파라미터에서 직접 가져옴

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
                lambda x: sorted([item for item in set(x) if item is not None]) + ([None] if None in set(x) else [])
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
                            # None 값을 필터링하고 문자열로 변환
                            dimension_types = sorted([str(dt) for dt in product_data['dimension_type'].tolist() if dt is not None])

                            non_standard_data.append({
                                'mdl_code': sample_row.get('mdl_code', ''),
                                'goods_nm': sample_row.get('goods_nm', ''),
                                'disp_nm1': sample_row.get('disp_nm1', ''),
                                'disp_nm2': sample_row.get('disp_nm2', ''),
                                'value': sample_row.get('value', ''),
                                'target_disp_nm2': sample_row.get('target_disp_nm2', ''),
                                'row_count': row_count,
                                'dimension_types': ', '.join(dimension_types) if dimension_types else 'none',
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
                        # None 값을 필터링하고 문자열로 변환
                        dimension_types = sorted([str(dt) for dt in product_data['dimension_type'].tolist() if dt is not None])

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
                            'dimension_types': ', '.join(dimension_types) if dimension_types else 'none'
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
        epilog=f'''
예제:
  python transform_spec.py --goal 크기작업                    # 크기 파싱 실행 (기존 데이터 유지)
  python transform_spec.py --goal 크기작업 --truncate         # mod 테이블 데이터 삭제 후 실행
  python transform_spec.py --goal 크기작업 --quiet            # 간략한 출력
  python transform_spec.py --list-parsers                     # 사용 가능한 파서 목록 보기

사용 가능한 파서 (goal 값):
  {', '.join(list_available_parsers())}
        '''
    )

    parser.add_argument('--goal', type=str, help='파싱 목적 (필수)')
    parser.add_argument('--truncate', action='store_true', help='mod 테이블 기존 데이터 삭제 (기본값: 유지)')
    parser.add_argument('--quiet', '-q', action='store_true', help='간략한 출력만 표시')
    parser.add_argument('--list-parsers', action='store_true', help='사용 가능한 파서 목록 표시')

    args = parser.parse_args()

    # 파서 목록 표시 요청 처리
    if args.list_parsers:
        print("\n사용 가능한 파서 목록:")
        print("=" * 40)
        for parser_goal in list_available_parsers():
            print(f"  - {parser_goal}")
        print("=" * 40)
        return

    # goal 파라미터 필수 체크
    if not args.goal:
        print("❌ 오류: --goal 파라미터는 필수입니다.")
        print(f"사용 가능한 값: {', '.join(list_available_parsers())}")
        print("\n사용 예시:")
        print("  python transform_spec.py --goal 크기작업")
        sys.exit(1)

    truncate_before_insert = args.truncate  # 기본값은 False (데이터 유지)
    verbose = not args.quiet
    goal = args.goal

    print("\n🚀 PostgreSQL 스펙 데이터 변환 파이프라인 (검증 규칙 기반)")
    print("="*80)

    # 설정 확인
    print("\n" + "="*80)
    print("실행 설정 확인")
    print("="*80)
    print(f"파싱 목적 (goal): {goal}")
    print(f"Staging 테이블: {STAGING_TABLE}")
    print(f"소스 테이블: {SOURCE_TABLE}")
    print(f"Result 테이블: {MOD_TABLE}")
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
            goal=goal,
            truncate_before_insert=truncate_before_insert,
            verbose=verbose
        )

        # 종료 코드 반환
        sys.exit(0 if success else 1)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()