#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
용량 파서 테스트 스크립트
"""
import pandas as pd
from parsers.capacity_parser import CapacityParser

def test_parser():
    """용량 파서 테스트"""
    parser = CapacityParser()

    # 테스트 케이스 (입력값 → 기대 출력)
    test_cases = [
        # (value, 기대되는 dimension_type, 기대되는 parsed_value)
        ("정격용량 (7.4 V) 5,000 mAh, 37Wh  /    환산용량 (3.7 V) 10,000 mAh", 'mAh', 5000),
        ("상의 5~9 벌 + 하의 5 벌", "multiple", [('상의', 9), ('하의', 5)]),
        ("6인용", '인용', 6),
        ("06인용", '인용', 6),
        ("23.5", None, 23.5),
        ("30 mAh", 'mAh', 30),
        ("4400", None, 4400),
        ("512 GB", 'GB', 512),
        ("1TB", 'TB', 1),
        ("25 kg", 'kg', 25),
        ("62.6 ㎡", '제곱미터', 62.6),
        ("4 켤레", '켤레', 4),
        ("420 ℓ", '리터', 420),
        ("1.7 L", '리터', 1.7),
        ("50 L", '리터', 50),
        ("250매", '매', 250),
    ]

    print("=" * 80)
    print("용량 파서 테스트 시작")
    print("=" * 80)

    passed = 0
    failed = 0

    for i, test_case in enumerate(test_cases, 1):
        if len(test_case) == 3:
            value, expected_type, expected_value = test_case
        else:
            continue

        # 테스트용 row 생성
        row = pd.Series({
            'value': value,
            'disp_nm2': '용량',
            'mdl_code': 'TEST001',
            'goods_nm': '테스트 제품',
            'disp_lv1': 'TEST',
            'disp_lv2': 'TEST',
            'disp_lv3': 'TEST',
            'category_lv1': 'TEST',
            'category_lv2': 'TEST',
            'category_lv3': 'TEST',
            'disp_nm1': '스펙',
            'is_numeric': True,
            'symbols': '',
            'new_value': value
        })

        # 파싱 실행
        parsed_rows, success, needs_check = parser.parse(row)

        # 결과 검증
        print(f"\n테스트 #{i}: {value[:60]}...")
        print(f"입력값: {value}")

        if expected_type == "multiple":
            # 복수 값 케이스
            if success and len(parsed_rows) == len(expected_value):
                all_match = True
                for j, (exp_type, exp_val) in enumerate(expected_value):
                    actual_type = parsed_rows[j]['dimension_type']
                    actual_val = parsed_rows[j]['parsed_value']

                    if actual_type != exp_type or abs(actual_val - exp_val) > 0.01:
                        all_match = False
                        print(f"  항목 {j+1}: 기대값 ({exp_type}, {exp_val}) != 실제값 ({actual_type}, {actual_val})")

                if all_match:
                    print(f"✅ PASS")
                    for j, row in enumerate(parsed_rows):
                        print(f"  항목 {j+1}: dimension_type={row['dimension_type']}, parsed_value={row['parsed_value']}")
                    passed += 1
                else:
                    print(f"❌ FAIL - 값이 일치하지 않음")
                    failed += 1
            else:
                print(f"❌ FAIL - 기대 개수: {len(expected_value)}, 실제 개수: {len(parsed_rows) if success else 0}")
                if parsed_rows:
                    for j, row in enumerate(parsed_rows):
                        print(f"  항목 {j+1}: dimension_type={row['dimension_type']}, parsed_value={row['parsed_value']}")
                failed += 1
        else:
            # 단일 값 케이스
            if success and len(parsed_rows) == 1:
                actual_type = parsed_rows[0]['dimension_type']
                actual_value = parsed_rows[0]['parsed_value']

                # 타입 체크 (None도 허용)
                type_match = (expected_type is None and actual_type is None) or (expected_type == actual_type)
                # 값 체크 (소수점 오차 허용)
                value_match = abs(actual_value - expected_value) < 0.01

                if type_match and value_match:
                    print(f"✅ PASS - dimension_type={actual_type}, parsed_value={actual_value}")
                    passed += 1
                else:
                    print(f"❌ FAIL")
                    print(f"  기대값: dimension_type={expected_type}, parsed_value={expected_value}")
                    print(f"  실제값: dimension_type={actual_type}, parsed_value={actual_value}")
                    failed += 1
            else:
                print(f"❌ FAIL - 파싱 실패 또는 예상치 못한 결과 개수: {len(parsed_rows) if success else 0}")
                if parsed_rows:
                    for j, row in enumerate(parsed_rows):
                        print(f"  항목 {j+1}: dimension_type={row['dimension_type']}, parsed_value={row['parsed_value']}")
                failed += 1

    print("\n" + "=" * 80)
    print(f"테스트 완료: 총 {len(test_cases)}개 중 {passed}개 통과, {failed}개 실패")
    print("=" * 80)

    return passed, failed

if __name__ == "__main__":
    passed, failed = test_parser()
    exit(0 if failed == 0 else 1)
