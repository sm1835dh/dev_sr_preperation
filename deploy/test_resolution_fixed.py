#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 해상도 파서 테스트
resolution_type을 dimension_type으로 저장
"""

import pandas as pd
import sys
sys.path.append('.')

from parsers.resolution_parser import ResolutionParser


def test_resolution_fixed():
    """수정된 해상도 파서 테스트"""

    # 테스트 샘플
    test_samples = [
        "4K (3,840 x 2,160)",     # width, height, 4K
        "FHD (1,920 x 1,080)",    # width, height, FHD
        "1920 x 1080",            # width, height만
        "up to 4K 60Hz",          # width, height, 4K
        "QHD (2,560 x 1,440)",    # width, height, QHD
    ]

    parser = ResolutionParser()

    print("="*80)
    print("수정된 해상도 파서 테스트 (resolution_type → dimension_type)")
    print("="*80)

    for i, sample in enumerate(test_samples, 1):
        print(f"\n[{i}] 테스트: {sample}")
        print("-"*40)

        test_row = pd.Series({
            'mdl_code': f'TEST{i:03d}',
            'goods_nm': '테스트 제품',
            'value': sample,
            'target_disp_nm2': '해상도'
        })

        parsed_rows, success, needs_check = parser.parse(test_row)

        if success and parsed_rows:
            print(f"✓ 성공: {len(parsed_rows)}개 row 생성")
            for j, row in enumerate(parsed_rows, 1):
                dim_type = row['dimension_type']
                parsed_val = row['parsed_value']

                if parsed_val is not None:
                    print(f"  [{j}] dimension_type: {dim_type:10s} | parsed_value: {parsed_val:.0f}")
                else:
                    print(f"  [{j}] dimension_type: {dim_type:10s} | parsed_value: None (타입 정보)")
        else:
            print("✗ 실패")

    # 상세 분석
    print("\n" + "="*80)
    print("파싱 결과 분석")
    print("="*80)

    sample = "4K (3,840 x 2,160)"
    test_row = pd.Series({'value': sample})
    parsed_rows, success, _ = parser.parse(test_row)

    if success:
        print(f"\n'{sample}' 파싱 결과:")
        dimension_types = [row['dimension_type'] for row in parsed_rows]
        print(f"  dimension_type 값들: {dimension_types}")

        width_row = next((r for r in parsed_rows if r['dimension_type'] == 'width'), None)
        height_row = next((r for r in parsed_rows if r['dimension_type'] == 'height'), None)
        type_row = next((r for r in parsed_rows if r['dimension_type'] not in ['width', 'height']), None)

        if width_row:
            print(f"  - width: {width_row['parsed_value']:.0f}")
        if height_row:
            print(f"  - height: {height_row['parsed_value']:.0f}")
        if type_row:
            print(f"  - 타입: {type_row['dimension_type']} (parsed_value: {type_row['parsed_value']})")

    print("\n" + "="*80)
    print("예상 DB 저장 형태")
    print("="*80)
    print("""
각 해상도당 생성되는 row:
1. dimension_type='width', parsed_value=3840
2. dimension_type='height', parsed_value=2160
3. dimension_type='4K', parsed_value=None (또는 특정 값)

이렇게 3개의 row가 생성됩니다.
""")


if __name__ == "__main__":
    test_resolution_fixed()