#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
해상도 데이터 정리 스크립트
기존 잘못 저장된 resolution_name 데이터를 삭제하고 다시 처리
"""

import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd

# .env 파일 로드
load_dotenv()

# 데이터베이스 연결 설정
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 테이블 정의
MOD_TABLE = "kt_spec_dimension_mod_table_v01"


def cleanup_resolution_data():
    """잘못 저장된 resolution_name 데이터 정리"""
    print("="*80)
    print("해상도 데이터 정리 스크립트")
    print("="*80)

    engine = create_engine(CONNECTION_STRING)

    try:
        # 1. 현재 상태 확인
        print("\n1. 현재 데이터 상태 확인...")
        print("-"*40)

        # resolution_name으로 저장된 데이터 확인
        check_query = text(f"""
            SELECT COUNT(*) as count,
                   dimension_type,
                   CASE
                       WHEN parsed_value IS NOT NULL THEN 'parsed_value'
                       WHEN parsed_string_value IS NOT NULL THEN 'parsed_string_value'
                       ELSE 'both_null'
                   END as stored_in
            FROM {MOD_TABLE}
            WHERE goal = '해상도'
              AND dimension_type IN ('resolution_name', 'width', 'height')
            GROUP BY dimension_type, stored_in
            ORDER BY dimension_type, stored_in
        """)

        df_status = pd.read_sql(check_query, engine)
        print("\n현재 저장 상태:")
        for _, row in df_status.iterrows():
            print(f"  {row['dimension_type']:20s} → {row['stored_in']:20s}: {row['count']:,}건")

        # 2. 잘못된 데이터 삭제
        print("\n2. 잘못된 resolution_name 데이터 삭제...")
        print("-"*40)

        # parsed_string_value가 NULL인 resolution_name 데이터 삭제
        delete_query = text(f"""
            DELETE FROM {MOD_TABLE}
            WHERE goal = '해상도'
              AND dimension_type = 'resolution_name'
              AND (parsed_string_value IS NULL OR parsed_string_value = '')
        """)

        with engine.connect() as conn:
            result = conn.execute(delete_query)
            conn.commit()
            deleted_count = result.rowcount
            print(f"✅ {deleted_count}건의 잘못된 resolution_name 데이터 삭제 완료")

        # 3. staging 테이블에서 미완료 작업 다시 표시
        print("\n3. 미완료 작업 상태 업데이트...")
        print("-"*40)

        update_staging_query = text("""
            UPDATE kt_spec_validation_table_v03_20251023_staging
            SET is_completed = false
            WHERE goal = '해상도'
              AND is_target = true
        """)

        with engine.connect() as conn:
            result = conn.execute(update_staging_query)
            conn.commit()
            updated_count = result.rowcount
            print(f"✅ {updated_count}건의 staging 레코드를 미완료로 업데이트")

        # 4. 삭제 후 상태 확인
        print("\n4. 정리 후 데이터 상태...")
        print("-"*40)

        df_status_after = pd.read_sql(check_query, engine)
        print("\n정리 후 저장 상태:")
        for _, row in df_status_after.iterrows():
            print(f"  {row['dimension_type']:20s} → {row['stored_in']:20s}: {row['count']:,}건")

        print("\n" + "="*80)
        print("✅ 정리 완료!")
        print("-"*40)
        print("이제 다음 명령어로 해상도 데이터를 다시 처리하세요:")
        print("\npython transform_spec_size.py --goal 해상도")
        print("="*80)

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")

    finally:
        engine.dispose()


if __name__ == "__main__":
    cleanup_resolution_data()