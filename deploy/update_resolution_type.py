#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
이미 저장된 해상도 데이터에 resolution_type 추가
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.append('.')
from parsers.resolution_parser import ResolutionParser

# .env 파일 로드
load_dotenv()

# 테이블 이름
RESULT_TABLE = 'kt_spec_validation_table_v03_20251023_result'

def get_sqlalchemy_engine():
    """SQLAlchemy 엔진 생성"""
    try:
        connection_string = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
        engine = create_engine(connection_string)
        print(f"✅ 데이터베이스 연결 성공")
        return engine
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        return None

def add_resolution_type_column(engine):
    """resolution_type 컬럼 추가 (없는 경우)"""
    try:
        # 컬럼 존재 확인
        check_query = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
            AND column_name = 'resolution_type'
        """)

        result = pd.read_sql(check_query, engine, params={'table_name': RESULT_TABLE.lower()})

        if result.empty:
            # 컬럼 추가
            with engine.begin() as conn:
                add_column_query = text(f"""
                    ALTER TABLE {RESULT_TABLE}
                    ADD COLUMN resolution_type TEXT
                """)
                conn.execute(add_column_query)
                print(f"✅ resolution_type 컬럼 추가 완료")
                return True
        else:
            print(f"ℹ️ resolution_type 컬럼이 이미 존재합니다")
            return True

    except Exception as e:
        print(f"❌ 컬럼 추가 실패: {e}")
        return False

def update_resolution_types(engine):
    """이미 저장된 해상도 데이터에 resolution_type 추가"""

    # 파서 인스턴스 생성
    parser = ResolutionParser()

    try:
        # 해상도 데이터 조회 (고유한 value들만)
        query = text(f"""
            SELECT DISTINCT value
            FROM {RESULT_TABLE}
            WHERE target_disp_nm2 = '화면 해상도'
            AND value IS NOT NULL
        """)

        df_values = pd.read_sql(query, engine)
        print(f"\n📊 처리할 해상도 값: {len(df_values)}개")

        # 각 value에 대해 resolution_type 추출
        update_count = 0

        for _, row in df_values.iterrows():
            value = row['value']

            # resolution_type 추출
            resolution_type = parser.extract_resolution_type(value)

            if resolution_type:
                # 해당 value를 가진 모든 row 업데이트
                with engine.begin() as conn:
                    update_query = text(f"""
                        UPDATE {RESULT_TABLE}
                        SET resolution_type = :resolution_type
                        WHERE value = :value
                        AND target_disp_nm2 = '화면 해상도'
                    """)

                    result = conn.execute(update_query, {
                        'resolution_type': resolution_type,
                        'value': value
                    })

                    update_count += result.rowcount
                    print(f"  ✓ {value:40s} → {resolution_type} ({result.rowcount}개 row 업데이트)")
            else:
                print(f"  - {value:40s} → 타입 없음")

        print(f"\n✅ 총 {update_count}개 row에 resolution_type 추가 완료")

        # 결과 확인
        verify_query = text(f"""
            SELECT resolution_type, COUNT(*) as count
            FROM {RESULT_TABLE}
            WHERE target_disp_nm2 = '화면 해상도'
            GROUP BY resolution_type
            ORDER BY count DESC
        """)

        df_verify = pd.read_sql(verify_query, engine)

        print("\n" + "="*80)
        print("Resolution Type 분포")
        print("="*80)
        print(df_verify.to_string())

        # 샘플 데이터 확인
        sample_query = text(f"""
            SELECT mdl_code, goods_nm, value, dimension_type, parsed_value, resolution_type
            FROM {RESULT_TABLE}
            WHERE target_disp_nm2 = '화면 해상도'
            AND resolution_type IS NOT NULL
            LIMIT 10
        """)

        df_sample = pd.read_sql(sample_query, engine)

        print("\n" + "="*80)
        print("업데이트된 데이터 샘플")
        print("="*80)
        print(df_sample.to_string())

    except Exception as e:
        print(f"❌ 업데이트 실패: {e}")
        import traceback
        traceback.print_exc()

def update_parser_code_for_future():
    """향후 파싱을 위한 코드 수정 안내"""
    print("\n" + "="*80)
    print("📝 향후 파싱을 위한 파서 코드 수정 안내")
    print("="*80)
    print("""
save_to_mod_table 함수에서 resolution_type을 저장하도록 수정 필요:

1. ResolutionParser의 parse 메서드가 반환하는 parsed_rows에
   이미 'resolution_type' 필드가 포함되어 있음

2. transform_spec_size.py의 save_to_mod_table 함수에서
   resolution_type 필드도 저장하도록 수정:

   - 중복 체크 컬럼에 'resolution_type' 추가
   - insert_data에 'resolution_type': row_dict.get('resolution_type') 추가
""")

def main():
    """메인 실행 함수"""

    print("="*80)
    print("해상도 타입 업데이트 시작")
    print("="*80)

    engine = get_sqlalchemy_engine()
    if not engine:
        sys.exit(1)

    try:
        # 1. resolution_type 컬럼 추가 (필요한 경우)
        if not add_resolution_type_column(engine):
            print("컬럼 추가 실패로 작업 중단")
            return

        # 2. 기존 데이터 업데이트
        update_resolution_types(engine)

        # 3. 향후 수정 안내
        update_parser_code_for_future()

    finally:
        engine.dispose()

if __name__ == "__main__":
    main()