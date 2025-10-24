#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터베이스 스키마 확인 스크립트
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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

def check_table_schema(engine):
    """테이블 스키마 확인"""
    try:
        # 컬럼 정보 확인
        schema_query = text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = :table_name
            ORDER BY ordinal_position
        """)

        df_schema = pd.read_sql(schema_query, engine, params={'table_name': RESULT_TABLE.lower()})

        print("\n" + "="*80)
        print(f"{RESULT_TABLE} 테이블 스키마")
        print("="*80)
        print(df_schema.to_string())

        # resolution_type 관련 컬럼 찾기
        print("\n" + "="*80)
        print("resolution_type 저장 가능한 컬럼 확인")
        print("="*80)

        # 텍스트 타입 컬럼 찾기
        text_columns = df_schema[df_schema['data_type'].isin(['text', 'character varying', 'varchar'])]
        print("\n텍스트 타입 컬럼:")
        print(text_columns.to_string())

        # 샘플 데이터로 현재 저장된 형태 확인
        sample_query = text(f"""
            SELECT mdl_code, goods_nm, disp_nm2, value,
                   dimension_type, parsed_value, target_disp_nm2
            FROM {RESULT_TABLE}
            WHERE target_disp_nm2 = '화면 해상도'
            LIMIT 10
        """)

        df_sample = pd.read_sql(sample_query, engine)

        print("\n" + "="*80)
        print("현재 저장된 해상도 데이터 샘플")
        print("="*80)
        print(df_sample.to_string())

    except Exception as e:
        print(f"❌ 스키마 확인 중 오류: {e}")

def main():
    """메인 실행 함수"""
    engine = get_sqlalchemy_engine()
    if engine:
        check_table_schema(engine)
        engine.dispose()

if __name__ == "__main__":
    main()