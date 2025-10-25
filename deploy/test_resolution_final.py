#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
최종 해상도 파서 테스트
dimension_type='resolution_name', parsed_value='4K' 형식 확인
"""

import pandas as pd
import sys
sys.path.append('.')

from parsers.resolution_parser import ResolutionParser


def test_resolution_final():
    """최종 해상도 파서 테스트"""

    # 테스트 샘플
    test_samples = [
        "4K (3,840 x 2,160)",     # width, height, resolution_name
        "FHD (1,920 x 1,080)",    # width, height, resolution_name
        "1920 x 1080",            # width, height만
        "up to 4K 60Hz",          # width, height, resolution_name
        "QHD (2,560 x 1,440)",    # width, height, resolution_name
    ]

    parser = ResolutionParser()

    print("="*80)
    print("최종 해상도 파서 테스트")
    print("dimension_type='resolution_name', parsed_value='4K' 형식")
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

                if dim_type == 'resolution_name':
                    print(f"  [{j}] dimension_type: 'resolution_name' | parsed_value: '{parsed_val}'")
                elif parsed_val is not None:
                    print(f"  [{j}] dimension_type: '{dim_type:10s}'      | parsed_value: {parsed_val:.0f}")
                else:
                    print(f"  [{j}] dimension_type: '{dim_type:10s}'      | parsed_value: None")
        else:
            print("✗ 실패")

    # 상세 분석
    print("\n" + "="*80)
    print("파싱 결과 상세 분석")
    print("="*80)

    sample = "4K (3,840 x 2,160)"
    test_row = pd.Series({'value': sample})
    parsed_rows, success, _ = parser.parse(test_row)

    if success:
        print(f"\n'{sample}' 파싱 결과:")

        width_row = next((r for r in parsed_rows if r['dimension_type'] == 'width'), None)
        height_row = next((r for r in parsed_rows if r['dimension_type'] == 'height'), None)
        type_row = next((r for r in parsed_rows if r['dimension_type'] == 'resolution_name'), None)

        if width_row:
            print(f"  1. dimension_type='width'           → parsed_value={width_row['parsed_value']:.0f}")
        if height_row:
            print(f"  2. dimension_type='height'          → parsed_value={height_row['parsed_value']:.0f}")
        if type_row:
            print(f"  3. dimension_type='resolution_name' → parsed_value='{type_row['parsed_value']}'")

    print("\n" + "="*80)
    print("✅ 예상 DB 저장 형태")
    print("="*80)
    print("""
'4K (3,840 x 2,160)' 파싱 결과:
  dimension_type = 'width',            parsed_value = 3840
  dimension_type = 'height',           parsed_value = 2160
  dimension_type = 'resolution_name',  parsed_value = '4K'

각 해상도당 최대 3개의 row가 생성됩니다.
(표준 타입이 없는 경우 2개 row만 생성)
""")


if __name__ == "__main__":
    test_resolution_final()