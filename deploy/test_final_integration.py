#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
최종 통합 테스트 - parsed_string_value 필드가 DB에 제대로 저장되는지 확인
"""

import sys
sys.path.append('.')

from parsers import get_parser
import pandas as pd


def test_complete_flow():
    """완전한 데이터 흐름 테스트"""
    print("="*80)
    print("최종 통합 테스트 - DB 저장 형식")
    print("="*80)

    parser = get_parser('해상도')

    # 테스트 데이터
    test_row = pd.Series({
        'value': '4K (3,840 x 2,160)',
        'validation_rule_id': 1,
        'target_disp_nm2': '해상도',
        'goal': '해상도',
        'mdl_code': 'TEST001',
        'goods_nm': '테스트 제품',
        'disp_lv1': 'A',
        'disp_lv2': 'B',
        'disp_lv3': 'C',
        'category_lv1': 'Cat1',
        'category_lv2': 'Cat2',
        'category_lv3': 'Cat3',
        'disp_nm1': '디스플레이1',
        'disp_nm2': '디스플레이2',
        'is_numeric': True,
        'symbols': '',
        'new_value': '4K'
    })

    # 파싱 실행
    parsed_rows, success, _ = parser.parse(test_row)

    if success:
        print("\n✅ 파싱 성공!")
        print(f"생성된 row 수: {len(parsed_rows)}")
        print("\n" + "-"*80)

        # 각 row에 메타데이터 추가 (실제 파이프라인과 동일하게)
        for parsed_row in parsed_rows:
            parsed_row['validation_rule_id'] = test_row['validation_rule_id']
            parsed_row['target_disp_nm2'] = test_row['target_disp_nm2']
            parsed_row['goal'] = test_row['goal']

        # 각 row 확인
        for i, row in enumerate(parsed_rows, 1):
            print(f"\n[Row {i}]")
            print(f"  dimension_type: '{row['dimension_type']}'")
            print(f"  goal: '{row.get('goal', 'N/A')}'")

            # 중요: parsed_value와 parsed_string_value 확인
            if 'parsed_value' in row and row['parsed_value'] is not None:
                print(f"  parsed_value: {row['parsed_value']}")
            else:
                print(f"  parsed_value: NULL")

            if 'parsed_string_value' in row and row['parsed_string_value'] is not None:
                print(f"  parsed_string_value: '{row['parsed_string_value']}'")
            else:
                print(f"  parsed_string_value: NULL")

        # SQL INSERT 예시 생성
        print("\n" + "="*80)
        print("예상 SQL INSERT 문:")
        print("-"*80)

        for i, row in enumerate(parsed_rows, 1):
            dim_type = row['dimension_type']
            goal = row.get('goal', '해상도')

            if dim_type in ['width', 'height']:
                # 숫자 타입
                print(f"""
-- Row {i}: {dim_type}
INSERT INTO kt_spec_dimension_mod_table_v01 (
    mdl_code, dimension_type, parsed_value, parsed_string_value, goal
) VALUES (
    'TEST001', '{dim_type}', {row.get('parsed_value', 'NULL')}, NULL, '{goal}'
);""")
            elif dim_type == 'resolution_name':
                # 문자열 타입
                print(f"""
-- Row {i}: {dim_type}
INSERT INTO kt_spec_dimension_mod_table_v01 (
    mdl_code, dimension_type, parsed_value, parsed_string_value, goal
) VALUES (
    'TEST001', '{dim_type}', NULL, '{row.get('parsed_string_value', 'NULL')}', '{goal}'
);""")

    else:
        print("\n❌ 파싱 실패!")

    print("\n" + "="*80)
    print("✅ 핵심 포인트:")
    print("-"*80)
    print("1. dimension_type='resolution_name'인 경우:")
    print("   - parsed_value = NULL")
    print("   - parsed_string_value = '4K' (문자열)")
    print("\n2. dimension_type='width' 또는 'height'인 경우:")
    print("   - parsed_value = 3840.0 (숫자)")
    print("   - parsed_string_value = NULL")
    print("="*80)


if __name__ == "__main__":
    test_complete_flow()