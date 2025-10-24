#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기존 해상도 데이터 정리 및 재처리 스크립트
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 테이블 이름
RESULT_TABLE = 'kt_spec_validation_table_v03_20251023_result'
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

def cleanup_resolution_data(engine):
    """기존 해상도 데이터 삭제"""
    try:
        with engine.begin() as conn:
            # 1. result 테이블에서 해상도 데이터 삭제
            delete_result = text(f"""
                DELETE FROM {RESULT_TABLE}
                WHERE target_disp_nm2 = '화면 해상도'
            """)
            result1 = conn.execute(delete_result)
            print(f"✅ Result 테이블에서 {result1.rowcount}개 row 삭제")

            # 2. staging 테이블의 is_completed를 false로 리셋
            reset_staging = text(f"""
                UPDATE {STAGING_TABLE}
                SET is_completed = false
                WHERE goal = '해상도'
            """)
            result2 = conn.execute(reset_staging)
            print(f"✅ Staging 테이블에서 {result2.rowcount}개 row 리셋")

            # 3. resolution_type 컬럼 삭제 (있는 경우)
            try:
                drop_column = text(f"""
                    ALTER TABLE {RESULT_TABLE}
                    DROP COLUMN IF EXISTS resolution_type
                """)
                conn.execute(drop_column)
                print(f"✅ resolution_type 컬럼 삭제")
            except:
                print(f"ℹ️ resolution_type 컬럼이 없거나 삭제 실패 (무시)")

        return True

    except Exception as e:
        print(f"❌ 정리 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_cleanup(engine):
    """정리 결과 확인"""
    try:
        # 1. Result 테이블 확인
        check_result = text(f"""
            SELECT COUNT(*) as count
            FROM {RESULT_TABLE}
            WHERE target_disp_nm2 = '화면 해상도'
        """)
        result = pd.read_sql(check_result, engine)
        print(f"\n📊 Result 테이블 해상도 데이터: {result.iloc[0]['count']}개")

        # 2. Staging 테이블 확인
        check_staging = text(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_completed = true THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN is_completed = false OR is_completed IS NULL THEN 1 ELSE 0 END) as pending
            FROM {STAGING_TABLE}
            WHERE goal = '해상도'
        """)
        staging = pd.read_sql(check_staging, engine)
        print(f"📊 Staging 테이블 해상도 규칙:")
        print(f"   - 전체: {staging.iloc[0]['total']}개")
        print(f"   - 완료: {staging.iloc[0]['completed']}개")
        print(f"   - 대기: {staging.iloc[0]['pending']}개")

    except Exception as e:
        print(f"❌ 확인 실패: {e}")

def main():
    """메인 실행 함수"""

    print("="*80)
    print("해상도 데이터 정리 및 재처리 준비")
    print("="*80)

    confirm = input("\n기존 해상도 데이터를 삭제하시겠습니까? (y/n): ").strip().lower()
    if confirm != 'y':
        print("작업을 취소했습니다.")
        return

    engine = get_sqlalchemy_engine()
    if not engine:
        sys.exit(1)

    try:
        # 1. 기존 데이터 정리
        print("\n" + "="*80)
        print("기존 데이터 정리 중...")
        print("="*80)

        if cleanup_resolution_data(engine):
            print("\n✅ 정리 완료!")

            # 2. 정리 결과 확인
            verify_cleanup(engine)

            # 3. 재처리 안내
            print("\n" + "="*80)
            print("📝 재처리 방법")
            print("="*80)
            print("""
이제 다음 명령으로 해상도 데이터를 재처리할 수 있습니다:

python transform_spec_size.py --goal 해상도

파싱 결과:
- dimension_type='width' : width 값
- dimension_type='height' : height 값
- dimension_type='4K' 등 : resolution 타입 (타입이 있는 경우)
""")
        else:
            print("\n❌ 정리 실패")

    finally:
        engine.dispose()

if __name__ == "__main__":
    main()