#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
parsed_string_value 필드 테스트
numeric 타입은 parsed_value, 문자열은 parsed_string_value에 저장
"""

import sys
sys.path.append('.')

from parsers import get_parser
import pandas as pd


def test_resolution_with_string_value():
    """resolution_name이 parsed_string_value에 저장되는지 확인"""
    print("="*80)
    print("parsed_string_value 필드 테스트")
    print("="*80)

    parser = get_parser('해상도')

    test_cases = [
        '4K (3,840 x 2,160)',
        'FHD (1,920 x 1,080)',
        'QHD (2,560 x 1,440)',
        '1920 x 1080',  # resolution_type 없는 경우
        'up to 4K 60Hz'
    ]

    for test_value in test_cases:
        print(f"\n테스트: {test_value}")
        print("-"*40)

        test_row = pd.Series({'value': test_value})
        parsed_rows, success, _ = parser.parse(test_row)

        if success:
            for row in parsed_rows:
                dim_type = row['dimension_type']

                if dim_type == 'resolution_name':
                    # resolution_name은 parsed_string_value에 저장되어야 함
                    if 'parsed_string_value' in row:
                        print(f"✓ dimension_type='{dim_type}' → parsed_string_value='{row['parsed_string_value']}'")
                    else:
                        print(f"✗ dimension_type='{dim_type}' → parsed_string_value 필드 없음!")
                        if 'parsed_value' in row:
                            print(f"  (잘못됨: parsed_value='{row['parsed_value']}')")

                elif dim_type in ['width', 'height']:
                    # width/height는 parsed_value에 저장되어야 함
                    if 'parsed_value' in row:
                        print(f"✓ dimension_type='{dim_type}' → parsed_value={row['parsed_value']}")
                    else:
                        print(f"✗ dimension_type='{dim_type}' → parsed_value 필드 없음!")
        else:
            print("✗ 파싱 실패")

    print("\n" + "="*80)
    print("데이터 타입별 저장 규칙:")
    print("-"*40)
    print("• numeric (width, height) → parsed_value (float)")
    print("• string (resolution_name) → parsed_string_value (string)")
    print("="*80)


def test_comprehensive_fields():
    """전체 필드 구조 확인"""
    print("\n전체 필드 구조 테스트")
    print("="*80)

    parser = get_parser('해상도')
    test_row = pd.Series({'value': '4K (3,840 x 2,160)'})
    parsed_rows, success, _ = parser.parse(test_row)

    if success:
        print("'4K (3,840 x 2,160)' 파싱 결과:\n")

        for i, row in enumerate(parsed_rows, 1):
            print(f"Row {i}:")
            print(f"  dimension_type: '{row['dimension_type']}'")

            # parsed_value 확인
            if 'parsed_value' in row:
                print(f"  parsed_value: {row['parsed_value']}")
            else:
                print(f"  parsed_value: (없음)")

            # parsed_string_value 확인
            if 'parsed_string_value' in row:
                print(f"  parsed_string_value: '{row['parsed_string_value']}'")
            else:
                print(f"  parsed_string_value: (없음)")

            print()

    print("="*80)
    print("✅ 예상 DB 스키마:")
    print("-"*40)
    print("""
CREATE TABLE kt_spec_dimension_mod_table_v01 (
    mdl_code VARCHAR,
    dimension_type VARCHAR,
    parsed_value FLOAT,          -- numeric 값 저장
    parsed_string_value VARCHAR,  -- 문자열 값 저장
    goal VARCHAR,
    ...
);
""")
    print("="*80)


if __name__ == "__main__":
    test_resolution_with_string_value()
    test_comprehensive_fields()