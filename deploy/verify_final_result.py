#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
최종 해상도 데이터 검증 스크립트
dimension_type에 resolution_type이 저장되었는지 확인
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

def verify_final_result(engine):
    """최종 결과 검증"""

    print("\n" + "="*80)
    print("최종 해상도 데이터 검증")
    print("="*80)

    # 1. dimension_type 값 분포
    type_dist_query = text(f"""
        SELECT dimension_type, COUNT(*) as count
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
        GROUP BY dimension_type
        ORDER BY count DESC
    """)

    df_type_dist = pd.read_sql(type_dist_query, engine)
    print("\n📊 dimension_type 값 분포:")
    print(df_type_dist.to_string())

    # 2. resolution type 카운트
    resolution_types = df_type_dist[~df_type_dist['dimension_type'].isin(['width', 'height'])]
    print(f"\n📊 Resolution Type 통계:")
    print(f"  - width/height 외 타입: {len(resolution_types)}개")
    print(f"  - 총 row 수: {resolution_types['count'].sum()}개")

    # 3. 샘플 데이터 확인 - 각 타입별
    print("\n" + "="*80)
    print("샘플 데이터 (각 타입별)")
    print("="*80)

    # 3-1. 표준 타입이 있는 경우
    sample_with_type = text(f"""
        SELECT DISTINCT ON (value)
            mdl_code, goods_nm, value, dimension_type, parsed_value
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
        AND dimension_type NOT IN ('width', 'height')
        LIMIT 10
    """)

    df_sample_type = pd.read_sql(sample_with_type, engine)
    print("\n📌 Resolution Type이 dimension_type으로 저장된 샘플:")
    print(df_sample_type.to_string())

    # 4. 특정 제품의 전체 row 확인
    sample_product = text(f"""
        SELECT mdl_code, goods_nm, value, dimension_type, parsed_value
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
        AND value = '4K (3,840 x 2,160)'
        ORDER BY mdl_code, dimension_type
        LIMIT 9
    """)

    df_sample_product = pd.read_sql(sample_product, engine)
    print("\n📌 '4K (3,840 x 2,160)' 해상도의 파싱 결과:")
    print(df_sample_product.to_string())

    # 5. 제품별 row 수 통계
    product_stats = text(f"""
        SELECT
            COUNT(DISTINCT CONCAT(mdl_code, '_', goods_nm)) as total_products,
            COUNT(DISTINCT CASE
                WHEN dimension_type IN ('width', 'height') THEN NULL
                ELSE CONCAT(mdl_code, '_', goods_nm)
            END) as products_with_type
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
    """)

    df_product_stats = pd.read_sql(product_stats, engine)
    print(f"\n📊 제품 통계:")
    print(f"  - 전체 제품 수: {df_product_stats.iloc[0]['total_products']}개")
    print(f"  - Resolution Type이 있는 제품: {df_product_stats.iloc[0]['products_with_type']}개")

    # 6. 전체 요약
    total_query = text(f"""
        SELECT COUNT(*) as total_rows
        FROM {RESULT_TABLE}
        WHERE target_disp_nm2 = '화면 해상도'
    """)

    df_total = pd.read_sql(total_query, engine)
    total_rows = df_total.iloc[0]['total_rows']

    width_height_count = df_type_dist[df_type_dist['dimension_type'].isin(['width', 'height'])]['count'].sum()
    type_count = total_rows - width_height_count

    print("\n" + "="*80)
    print("✅ 최종 검증 결과")
    print("="*80)
    print(f"총 {total_rows}개 row:")
    print(f"  - width/height: {width_height_count}개 ({width_height_count/total_rows*100:.1f}%)")
    print(f"  - resolution type: {type_count}개 ({type_count/total_rows*100:.1f}%)")

def main():
    """메인 실행 함수"""
    engine = get_sqlalchemy_engine()
    if engine:
        verify_final_result(engine)
        engine.dispose()

if __name__ == "__main__":
    main()