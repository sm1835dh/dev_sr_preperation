#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
해상도 파서 테스트 스크립트
제공된 샘플 데이터로 파싱 테스트를 수행합니다.
"""

import pandas as pd
import sys
sys.path.append('.')  # 현재 디렉토리를 패스에 추가

from parsers.resolution_parser import ResolutionParser


def test_resolution_samples():
    """제공된 샘플 데이터 테스트"""

    # 샘플 데이터
    test_samples = [
        "1920 x 1200 (WUXGA)",
        "2960 x 1848 (WQXGA+)",
        "256 x 402",
        "450 x 450",
        "480 x 480",
        "438 x 438",
        "1080 x 2408 (FHD+)",
        "1080 x 2340 (FHD+)",
        "3120 x 1440 (Quad HD+)",
        "2640 x 1080 (FHD+)",
        "up to 4K 60Hz",
        "3120 x 1440 (Quad HD+)",
        "2640 x 1080 (FHD+)",
        "1,366 x 768",
        "FHD (1,920 x 1,080)",
        "4 K (3,840 x 2,160)",
        "UWQHD (3,440 x 1,440)",
        "WUHD (5,120 x 2,160)",
        "FHD (1,920 x 1,080)",
        "1,920 x 1,080",
        "3,840 x 2,160",
        "4,968 x 2,808",
        "4K (3,840 x 2,160)",
        "HD (1,366 x 768)",
        "8K (7,680 x 4,320)",
        "7,680 x 4,320",
        "4K (3,840 x 2,160)",
        "QHD (2,560 x 1,440)",
        "3,840 x 2,160",
    ]

    # 파서 인스턴스 생성
    parser = ResolutionParser()

    print("="*80)
    print("해상도 파서 테스트")
    print("="*80)
    print(f"테스트할 샘플 수: {len(test_samples)}")
    print("-"*80)

    # 통계 초기화
    success_count = 0
    failed_samples = []

    # 각 샘플 테스트
    for i, sample in enumerate(test_samples, 1):
        # 테스트 데이터 생성
        test_row = pd.Series({
            'mdl_code': f'TEST{i:03d}',
            'goods_nm': '테스트 제품',
            'value': sample,
            'target_disp_nm2': '해상도'
        })

        # 파싱
        parsed_rows, success, needs_check = parser.parse(test_row)

        # 결과 출력
        print(f"\n[{i:2d}] {sample}")

        if success and parsed_rows:
            success_count += 1

            # width와 height 찾기
            width_data = next((r for r in parsed_rows if r['dimension_type'] == 'width'), None)
            height_data = next((r for r in parsed_rows if r['dimension_type'] == 'height'), None)

            if width_data and height_data:
                width = width_data['parsed_value']
                height = height_data['parsed_value']
                resolution_type = width_data.get('resolution_type', 'N/A')

                print(f"     ✓ 성공: {width:.0f} x {height:.0f}")
                if resolution_type and resolution_type != 'N/A':
                    print(f"     → 해상도 타입: {resolution_type}")
            else:
                print(f"     ⚠️ 부분 파싱: width 또는 height 누락")
        else:
            failed_samples.append(sample)
            print(f"     ✗ 실패")

    # 전체 통계 출력
    print("\n" + "="*80)
    print("테스트 결과 요약")
    print("="*80)
    print(f"전체: {len(test_samples)}개")
    print(f"성공: {success_count}개 ({success_count/len(test_samples)*100:.1f}%)")
    print(f"실패: {len(failed_samples)}개 ({len(failed_samples)/len(test_samples)*100:.1f}%)")

    if failed_samples:
        print("\n실패한 샘플:")
        for sample in failed_samples:
            print(f"  - {sample}")

    # 해상도 타입별 통계
    print("\n" + "="*80)
    print("해상도 타입 감지 테스트")
    print("="*80)

    type_counts = {}
    for sample in test_samples:
        test_row = pd.Series({'value': sample})
        parsed_rows, success, _ = parser.parse(test_row)
        if success and parsed_rows:
            res_type = parsed_rows[0].get('resolution_type')
            if res_type:
                type_counts[res_type] = type_counts.get(res_type, 0) + 1

    if type_counts:
        print("감지된 해상도 타입:")
        for res_type, count in sorted(type_counts.items()):
            print(f"  - {res_type}: {count}개")
    else:
        print("감지된 해상도 타입 없음")

    return success_count == len(test_samples)


if __name__ == "__main__":
    # 테스트 실행
    all_passed = test_resolution_samples()

    print("\n" + "="*80)
    if all_passed:
        print("✅ 모든 테스트 통과!")
    else:
        print("⚠️ 일부 테스트 실패 - 파서 개선 필요")
    print("="*80)