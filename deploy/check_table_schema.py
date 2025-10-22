#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""소스 테이블의 스키마를 확인하는 스크립트"""

import os
import sys
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

load_dotenv()

def main():
    table_name = sys.argv[1] if len(sys.argv) > 1 else 'test_spec_01'

    connection_string = f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    engine = create_engine(connection_string)
    inspector = inspect(engine)

    columns = inspector.get_columns(table_name)
    print(f"\n테이블 '{table_name}'의 컬럼 목록:")
    print("=" * 80)
    for col in columns:
        print(f"  {col['name']:<30} {col['type']}")
    print("=" * 80)
    print(f"총 {len(columns)}개 컬럼\n")

    # dimension_type 컬럼이 있는지 확인
    has_dimension_type = any(col['name'] == 'dimension_type' for col in columns)
    if has_dimension_type:
        print("⚠️  주의: 이 테이블에는 이미 'dimension_type' 컬럼이 있습니다!")

    engine.dispose()

if __name__ == "__main__":
    main()
