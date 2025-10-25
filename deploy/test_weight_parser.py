#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
무게 파서 테스트
"""
import pandas as pd
from parsers.weight_parser import WeightParser

def test_weight_parser():
    """무게 파서 테스트"""

    # 테스트 데이터
    test_cases = [
        ("3.3", 3.3),
        ("508", 508),
        ("5.6 g", 5.6),
        ("4100 g", 4100),
        ("34.7 kg", 34.7),
        ("4.9 ㎏", 4.9),
        ("5Kg", 5),
        ("7.3Kg", 7.3),
        ("3.7kg", 3.7),
        ("860g", 860),
        ("9.79 (kg)", 9.79),
        ("2.67 Kg (기본 배터리 기준)", 2.67),
        ("70 g (수트 케이스 + 플립수트 카드 4 매)", 70),
        ("스트랩: 87 g /베젤링 스티커: 10 g(1개 기준)", 87),
        ("케이스 : 47 g / 키링 : 43 g", 47),
        ("48,200 g", 48200),
        ("7.6 kg (포장 무게)/ 6.4 kg (제품 무게)", 7.6),
        ("1,610 g", 1610),
        ("약 5kg", 5),
        ("실 중량 : 96kg / 총 중량 : 116kg", 96),
        ("3.5kg (포장상태 :5.5Kg)", 3.5),
        ("2,000ml", 2000),
        ("Set Only : 19.3 kg, Set with Supplies : 22.3 kg", 19.3),
        ("Net: 9.8 kg (21.6 lb) / Gross: 11.9kg ( 26.2 lb)", 9.8),
        ("Approx. 0.25g", 0.25),
        ("2 TB : 125 g", 125),
        ("약 45.0g", 45),
        ("1.54 kg (3.4 lb)", 1.54),
        ("82.7 g※ 배터리 포함 중량이며, 중량은 제조 환경에 따라 달라질 수 있음", 82.7),
        ("키보드 800 g, 리시버 1.7 g※ 무게는 제조 환경에 따라 달라질 수 있음", 800),
        ("키보드 462 g, 마우스 49 g, 리시버 1.8 g<br>* 무게는 제조 환경에 따라 달라질 수 있음", 462),
        ("마우스 70g, 리시버 1.75g", 70),
        ("100 g※ 무게는 제조 환경에 따라 달라질 수 있음", 100),
        ("0.87 (무게는 제조 환경에 따라 달라질 수 있음) cm", 0.87),
        ("9.2 / 10.6 kg", 9.2),
        ("12.4 / 13.8 kg", 12.4),
    ]

    # 파서 초기화
    parser = WeightParser()

    # 테스트 실행
    print("=" * 80)
    print("무게 파서 테스트")
    print("=" * 80)

    success_count = 0
    fail_count = 0

    for input_value, expected_value in test_cases:
        # 테스트 row 생성
        row_data = {
            'mdl_code': 'TEST001',
            'goods_nm': '테스트 제품',
            'disp_nm1': '무게',
            'disp_nm2': '무게',
            'value': input_value,
            'target_disp_nm2': '무게'
        }
        row = pd.Series(row_data)

        # 파싱 실행
        parsed_rows, success, needs_check = parser.parse(row)

        if success and parsed_rows:
            parsed_value = parsed_rows[0]['parsed_value']
            if parsed_value == expected_value:
                print(f"✅ PASS: '{input_value}' → {parsed_value}")
                success_count += 1
            else:
                print(f"❌ FAIL: '{input_value}' → {parsed_value} (예상: {expected_value})")
                fail_count += 1
        else:
            print(f"❌ FAIL: '{input_value}' → 파싱 실패 (예상: {expected_value})")
            fail_count += 1

    # 결과 요약
    print("=" * 80)
    print(f"테스트 결과: 성공 {success_count}/{len(test_cases)}, 실패 {fail_count}/{len(test_cases)}")
    print(f"성공률: {success_count/len(test_cases)*100:.1f}%")
    print("=" * 80)

    # 추가 테스트: extract_weight_value 메서드 직접 테스트
    print("\n특수 케이스 상세 테스트:")
    print("-" * 40)

    special_cases = [
        "키보드 800 g, 리시버 1.7 g",
        "Set Only : 19.3 kg, Set with Supplies : 22.3 kg",
        "Net: 9.8 kg (21.6 lb) / Gross: 11.9kg",
        "실 중량 : 96kg / 총 중량 : 116kg",
    ]

    for test_value in special_cases:
        result = parser.extract_weight_value(test_value)
        print(f"입력: '{test_value}'")
        print(f"결과: {result}")
        print()

if __name__ == "__main__":
    test_weight_parser()