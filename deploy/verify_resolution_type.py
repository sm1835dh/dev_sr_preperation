#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
resolution_type 업데이트 검증 스크립트
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

def verify_resolution_types(engine):
    """resolution_type 업데이트 검증"""

    print("\n" + "="*80)
    print("Resolution Type 업데이트 검증")
    print("="*80)

    # 1. 전체 통계
    stats_query = text(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(CASE WHEN resolution_type IS NOT NULL THEN 1 END) as with_type,
            COUNT(CASE WHEN resolution_type IS NULL THEN 1 END) as without_type
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
    """)

    df_stats = pd.read_sql(stats_query, engine)
    print("\n📊 전체 통계:")
    print(df_stats.to_string())

    # 2. Resolution Type별 분포
    type_dist_query = text(f"""
        SELECT
            COALESCE(resolution_type, '(없음)') as resolution_type,
            COUNT(*) as count,
            COUNT(DISTINCT value) as unique_values
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
        GROUP BY resolution_type
        ORDER BY count DESC
    """)

    df_type_dist = pd.read_sql(type_dist_query, engine)
    print("\n📊 Resolution Type별 분포:")
    print(df_type_dist.to_string())

    # 3. 타입이 없는 값들 확인
    no_type_query = text(f"""
        SELECT DISTINCT value
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
        AND resolution_type IS NULL
        ORDER BY value
    """)

    df_no_type = pd.read_sql(no_type_query, engine)
    print(f"\n📊 Resolution Type이 없는 값들 ({len(df_no_type)}개):")
    for _, row in df_no_type.iterrows():
        print(f"  - {row['value']}")

    # 4. 샘플 데이터 확인
    sample_query = text(f"""
        SELECT mdl_code, goods_nm, value, dimension_type, parsed_value, resolution_type
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
        AND resolution_type IS NOT NULL
        ORDER BY resolution_type, mdl_code
        LIMIT 20
    """)

    df_sample = pd.read_sql(sample_query, engine)
    print("\n📊 샘플 데이터 (resolution_type 포함):")
    print(df_sample.to_string())

    # 5. 업데이트 성공률
    success_rate = (df_stats.iloc[0]['with_type'] / df_stats.iloc[0]['total_rows']) * 100
    print(f"\n✅ Resolution Type 업데이트 성공률: {success_rate:.1f}%")

def main():
    """메인 실행 함수"""
    engine = get_sqlalchemy_engine()
    if engine:
        verify_resolution_types(engine)
        engine.dispose()

if __name__ == "__main__":
    main()