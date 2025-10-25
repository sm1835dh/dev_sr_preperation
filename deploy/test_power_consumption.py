#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
소비전력 파서 테스트 스크립트
"""
import pandas as pd
from parsers.power_consumption_parser import PowerConsumptionParser

def test_parser():
    """소비전력 파서 테스트"""
    parser = PowerConsumptionParser()

    # 테스트 케이스 (입력값 → 기대 출력)
    test_cases = [
        # (value, 기대되는 dimension_type, 기대되는 parsed_value)
        ("2050", None, 2050),
        ("(23.5.1일 부 개정 기준) 15.9 kWh/월", None, 15.9),
        ("(세트) 70W (스테이션) 1200W", "multiple", [(
'세트', 70), ('스테이션', 1200)]),
        ("0.10 kW", None, 0.10),
        ("0.90 / 4.55 / 6.30 kW", None, 6.30),  # 최대값
        ("103.0 W", None, 103.0),
        ("1100 W", None, 1100),
        ("12.0 kWh/월", None, 12.0),
        ("1450W", None, 1450),
        ("2400 W", None, 2400),
        ("15.4 (21.10.1일 부 소비전력량 개정 기준) kWh/월", None, 15.4),
        ("15.9 kWh/월", None, 15.9),
        ("2.30 kW", None, 2.30),
        ("600 W (인쇄 시), 13 W (대기 시), 0.8 W (절전 모드), 1.921 kWh (TEC)", "multiple", [('인쇄', 600), ('대기', 13)]),  # 절전 skip
        ("700W(동작시) / 50W(대기시) / 1.21W(Sleep 모드)", "multiple", [('동작', 700), ('대기', 50)]),  # Sleep skip
        ("820 watts (40ppm, printing), 41.5 watts (ready), 0.8 watts (sleep), 0.2 watts (auto-off), 0.2 watts (manual-off)", "multiple", [('인쇄', 820), ('대기', 41.5)]),  # sleep, off skip
        ("가열 세탁 시 2200W", '가열세탁', 2200),
        ("가열세탁 시 2100 W / 건조 1700 W", "multiple", [('가열세탁', 2100), ('건조', 1700)]),
        ("가열세탁 시 2200 W/건조 2400 W", "multiple", [('가열세탁', 2200), ('건조', 2400)]),
        ("표준세탁 시 800 W/가열세탁 시 1400 W", "multiple", [('표준세탁', 800), ('가열세탁', 1400)]),
        ("가열세탁 시 2200W", '가열세탁', 2200),
        ("냉수세탁 시 : 120 W/가열세탁 시 : 1800 W", "multiple", [('냉수세탁', 120), ('가열세탁', 1800)]),
        ("냉수 : 0.75A / 온수 : 2,600W", "multiple", [('냉수', 0.75), ('온수', 2600)]),  # 둘 다 파싱 (A도 포함)
        ("냉수: 1.2 A, 온수: 2600 W", "multiple", [('냉수', 1.2), ('온수', 2600)]),  # 둘 다 파싱 (A도 포함)
        ("삶음 2500 W", '삶음', 2500),
        ("OFF : 0.11 W, Ready : 3.02 W, Sleep : 1.96W", '대기', 3.02),  # OFF, Sleep skip, Ready만 파싱
        # 새로운 케이스들
        ("0.10 Watts (Off), 4.0 Watts (Ready), 2 Watts (Sleep),14 Watts (Printing)", "multiple", [('대기', 4.0), ('인쇄', 14.0)]),  # Off, Sleep skip
        ("500 W (평균) / 14 W (대기 시) / 1.2W (절전 모드)", "multiple", [('평균', 500), ('대기', 14)]),  # 절전 skip
        ("400 W (평균 작동 모드) / 50 W (대기 모드) / *1.6 W (절전 모드) *(Wi-Fi Direct 작동시: 2.4W)", "multiple", [('평균', 400), ('대기', 50)]),  # 절전 skip
        ("10W (사용중), 0.7W(슬립모드)", '사용중', 10),
        # Skip cases - should return False
        ("슬립모드 0.83 W, 오프모드 0.05 W", None, None),
    ]

    print("=" * 80)
    print("소비전력 파서 테스트 시작")
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
            'disp_nm2': '소비전력',
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

        if expected_value is None:
            # Skip 케이스
            if not success:
                print(f"✅ PASS - 올바르게 skip 처리됨")
                passed += 1
            else:
                print(f"❌ FAIL - skip 되어야 하는데 파싱됨: {parsed_rows}")
                failed += 1
        elif expected_type == "multiple":
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
