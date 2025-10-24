#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터베이스 데이터 확인 스크립트
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 테이블 이름
STAGING_TABLE = 'kt_spec_validation_table_v03_20251023_staging'

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

def check_goal_values(engine):
    """goal 컬럼 값 확인"""
    try:
        # goal 컬럼이 있는지 확인
        check_column_query = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
            AND column_name = 'goal'
        """)

        result = pd.read_sql(check_column_query, engine, params={'table_name': STAGING_TABLE.lower()})

        if result.empty:
            print(f"⚠️ '{STAGING_TABLE}' 테이블에 'goal' 컬럼이 없습니다.")
            return

        print(f"✅ 'goal' 컬럼 존재 확인")

        # goal 값들 확인
        goal_query = text(f"""
            SELECT goal, COUNT(*) as count,
                   SUM(CASE WHEN is_target = true THEN 1 ELSE 0 END) as target_count,
                   SUM(CASE WHEN is_completed = true THEN 1 ELSE 0 END) as completed_count
            FROM {STAGING_TABLE}
            GROUP BY goal
            ORDER BY count DESC
        """)

        df_goals = pd.read_sql(goal_query, engine)

        print("\n" + "="*80)
        print("Goal 값별 통계")
        print("="*80)

        if df_goals.empty:
            print("데이터가 없습니다.")
        else:
            print(df_goals.to_string())

        # is_target=true인 데이터 중 goal별 통계
        target_query = text(f"""
            SELECT goal, COUNT(*) as count,
                   SUM(CASE WHEN is_completed = true THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN is_completed = false OR is_completed IS NULL THEN 1 ELSE 0 END) as pending
            FROM {STAGING_TABLE}
            WHERE is_target = true
            GROUP BY goal
            ORDER BY count DESC
        """)

        df_targets = pd.read_sql(target_query, engine)

        print("\n" + "="*80)
        print("is_target=true인 데이터의 goal별 통계")
        print("="*80)

        if df_targets.empty:
            print("is_target=true인 데이터가 없습니다.")
        else:
            print(df_targets.to_string())

        # 샘플 데이터 확인
        sample_query = text(f"""
            SELECT disp_nm1, disp_nm2, target_disp_nm2, goal, is_target, is_completed
            FROM {STAGING_TABLE}
            WHERE goal IS NOT NULL
            LIMIT 10
        """)

        df_sample = pd.read_sql(sample_query, engine)

        print("\n" + "="*80)
        print("샘플 데이터 (10개)")
        print("="*80)
        print(df_sample.to_string())

    except Exception as e:
        print(f"❌ 데이터 확인 중 오류: {e}")

def main():
    """메인 실행 함수"""
    engine = get_sqlalchemy_engine()
    if engine:
        check_goal_values(engine)
        engine.dispose()

if __name__ == "__main__":
    main()