#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
통합 테스트 - 파서 등록 및 실행 확인
"""

import sys
sys.path.append('.')

from parsers import get_parser
import pandas as pd


def test_parser_registration():
    """파서 등록 상태 확인"""
    print("="*80)
    print("파서 등록 및 통합 테스트")
    print("="*80)

    # 1. 크기작업 파서 확인
    print("\n1. 크기작업 파서 테스트:")
    print("-"*40)

    size_parser = get_parser('크기작업')
    if size_parser:
        print(f"✓ 크기작업 파서 등록됨: {size_parser.__class__.__name__}")
        test_row = pd.Series({
            'value': '가로 100mm x 세로 200mm',
            'target_disp_nm2': '크기'
        })
        parsed_rows, success, _ = size_parser.parse(test_row)
        if success:
            print(f"  ✓ 테스트 파싱 성공: {len(parsed_rows)}개 row 생성")
        else:
            print("  ✗ 테스트 파싱 실패")
    else:
        print("✗ 크기작업 파서를 찾을 수 없음")

    # 2. 해상도 파서 확인
    print("\n2. 해상도 파서 테스트:")
    print("-"*40)

    resolution_parser = get_parser('해상도')
    if resolution_parser:
        print(f"✓ 해상도 파서 등록됨: {resolution_parser.__class__.__name__}")

        # 테스트 케이스들
        test_cases = [
            ('4K (3,840 x 2,160)', 3),  # width, height, resolution_name
            ('1920 x 1080', 2),          # width, height만
            ('FHD (1,920 x 1,080)', 3),  # width, height, resolution_name
        ]

        for value, expected_rows in test_cases:
            test_row = pd.Series({'value': value})
            parsed_rows, success, _ = resolution_parser.parse(test_row)

            if success and len(parsed_rows) == expected_rows:
                print(f"  ✓ '{value}' → {len(parsed_rows)}개 row")

                # resolution_name 확인
                resolution_name_row = next(
                    (r for r in parsed_rows if r['dimension_type'] == 'resolution_name'),
                    None
                )
                if resolution_name_row:
                    print(f"    - resolution_name: '{resolution_name_row['parsed_value']}'")
            else:
                print(f"  ✗ '{value}' 파싱 실패 또는 예상과 다른 row 수")
    else:
        print("✗ 해상도 파서를 찾을 수 없음")

    # 3. 미등록 파서 테스트
    print("\n3. 미등록 파서 테스트:")
    print("-"*40)

    unknown_parser = get_parser('미등록작업')
    if unknown_parser is None:
        print("✓ 미등록 파서에 대해 None 반환 (정상)")
    else:
        print("✗ 미등록 파서에 대해 예상치 못한 값 반환")

    # 4. dimension_type='resolution_name' 형식 확인
    print("\n4. resolution_name 저장 형식 확인:")
    print("-"*40)

    test_row = pd.Series({'value': '4K (3,840 x 2,160)'})
    resolution_parser = get_parser('해상도')
    parsed_rows, success, _ = resolution_parser.parse(test_row)

    if success:
        for row in parsed_rows:
            if row['dimension_type'] == 'resolution_name':
                if isinstance(row['parsed_value'], str):
                    print(f"✓ dimension_type='resolution_name', parsed_value='{row['parsed_value']}' (문자열)")
                else:
                    print(f"✗ parsed_value가 문자열이 아님: {type(row['parsed_value'])}")
            elif row['dimension_type'] in ['width', 'height']:
                if isinstance(row['parsed_value'], (int, float)):
                    print(f"✓ dimension_type='{row['dimension_type']}', parsed_value={row['parsed_value']} (숫자)")
                else:
                    print(f"✗ {row['dimension_type']}의 parsed_value가 숫자가 아님")

    print("\n" + "="*80)
    print("통합 테스트 완료")
    print("="*80)


if __name__ == "__main__":
    test_parser_registration()