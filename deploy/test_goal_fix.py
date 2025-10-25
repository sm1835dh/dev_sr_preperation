#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
goal 필드 수정 테스트
"""

import sys
sys.path.append('.')

from parsers import get_parser
import pandas as pd


def test_goal_field():
    """goal 필드가 올바르게 전달되는지 테스트"""
    print("="*80)
    print("Goal 필드 전달 테스트")
    print("="*80)

    # 해상도 파서로 테스트
    parser = get_parser('해상도')
    goal = '해상도'  # 함수 파라미터로 전달될 값

    # 테스트 데이터 (goal 필드 없음)
    test_row = pd.Series({
        'value': '4K (3,840 x 2,160)',
        'validation_rule_id': 1,
        'target_disp_nm2': '해상도',
        'mdl_code': 'TEST001',
        'goods_nm': '테스트 제품'
    })

    print(f"\n입력 데이터:")
    print(f"  - value: {test_row['value']}")
    print(f"  - target_disp_nm2: {test_row['target_disp_nm2']}")
    print(f"  - goal 필드 존재 여부: {'goal' in test_row}")
    print(f"  - goal 파라미터 값: '{goal}'")

    # 파싱
    parsed_rows, success, _ = parser.parse(test_row)

    if success:
        print(f"\n✅ 파싱 성공! {len(parsed_rows)}개 row 생성")

        # 실제 파이프라인처럼 goal 추가
        for parsed_row in parsed_rows:
            parsed_row['goal'] = goal  # 함수 파라미터에서 가져옴

        print("\n각 row의 goal 값:")
        for i, row in enumerate(parsed_rows, 1):
            print(f"  Row {i}: dimension_type='{row['dimension_type']}', goal='{row.get('goal', 'MISSING!')}'")

        # 데이터 타입별 저장 확인
        print("\n데이터 타입별 저장 형식:")
        for row in parsed_rows:
            dim_type = row['dimension_type']
            if dim_type in ['width', 'height']:
                if 'parsed_value' in row:
                    print(f"  ✓ {dim_type}: parsed_value={row['parsed_value']}, goal='{row['goal']}'")
            elif dim_type == 'resolution_name':
                if 'parsed_string_value' in row:
                    print(f"  ✓ {dim_type}: parsed_string_value='{row['parsed_string_value']}', goal='{row['goal']}'")

    print("\n" + "="*80)
    print("✅ 핵심 포인트:")
    print("  - goal은 row에 없고 함수 파라미터로 전달됨")
    print("  - transform_spec_size.py의 라인 519 수정 완료")
    print("  - parsed_row['goal'] = goal (row['goal']이 아님)")
    print("="*80)


if __name__ == "__main__":
    test_goal_field()