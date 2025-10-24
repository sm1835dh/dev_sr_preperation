#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
파서 모듈 테스트 스크립트
"""

import pandas as pd
from parsers import get_parser, list_available_parsers

def test_parser():
    """파서 모듈 테스트"""

    print("=" * 60)
    print("파서 모듈 테스트")
    print("=" * 60)

    # 1. 사용 가능한 파서 목록 확인
    print("\n1. 사용 가능한 파서 목록:")
    parsers = list_available_parsers()
    for p in parsers:
        print(f"   - {p}")

    # 2. 크기작업 파서 테스트
    print("\n2. '크기작업' 파서 테스트:")
    parser = get_parser('크기작업')
    if parser:
        print(f"   ✓ 파서 로드 성공: {parser.__class__.__name__}")
        print(f"   ✓ Goal: {parser.get_goal()}")

        # 테스트 데이터 생성
        test_data = pd.Series({
            'mdl_code': 'TEST001',
            'goods_nm': '테스트 상품',
            'disp_nm2': '본체 크기 (가로x세로x두께)',
            'value': '100 x 200 x 50',
            'target_disp_nm2': '크기'
        })

        print("\n   테스트 데이터:")
        print(f"     - disp_nm2: {test_data['disp_nm2']}")
        print(f"     - value: {test_data['value']}")

        # 파싱 테스트
        parsed_rows, success, needs_check = parser.parse(test_data)

        print(f"\n   파싱 결과:")
        print(f"     - 성공 여부: {success}")
        print(f"     - 추가 확인 필요: {needs_check}")
        print(f"     - 파싱된 데이터 수: {len(parsed_rows)}")

        if parsed_rows:
            for i, row in enumerate(parsed_rows, 1):
                print(f"\n     [{i}] dimension_type: {row['dimension_type']}, parsed_value: {row['parsed_value']}")
    else:
        print("   ✗ '크기작업' 파서를 찾을 수 없습니다.")

    # 3. 존재하지 않는 파서 테스트
    print("\n3. 존재하지 않는 파서 테스트:")
    parser = get_parser('색상작업')
    if parser is None:
        print("   ✓ 예상대로 None 반환")
    else:
        print("   ✗ 예상과 달리 파서가 반환됨")

    print("\n" + "=" * 60)
    print("테스트 완료")
    print("=" * 60)

if __name__ == "__main__":
    test_parser()