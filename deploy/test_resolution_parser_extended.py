#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
확장된 해상도 파서 테스트 스크립트
모든 제공된 샘플 데이터로 파싱 테스트를 수행합니다.
"""

import pandas as pd
import sys
sys.path.append('.')  # 현재 디렉토리를 패스에 추가

from parsers.resolution_parser import ResolutionParser


def test_extended_resolution_samples():
    """확장된 샘플 데이터 테스트"""

    # 전체 샘플 데이터
    test_samples = [
        "1080 x 2340 (FHD+)",
        "1080 x 2408 (FHD+)",
        "1340 x 800 (WXGA+)",
        "1,366 x 768",
        "1,920 x 1,080",
        "1920 x 1200 (WUXGA)",
        "2000 x 1200 (WUXGA+)",
        "2112 x 1320 (WUXGA+)",
        "2160 x 1856 (QXGA+)",
        "2176 x 1812 (QXGA+)",
        "2184 x 1968 (QXGA+)",
        "2304 x 1440 (WUXGA+)",
        "2340 x 1080 (FHD+)",
        "2520 x 1080 (FHD+)",
        "2,560 x 1,440",
        "2560 x 1600 (WQXGA)",
        "256 x 402",
        "2640 x 1080 (FHD+)",
        "2800 x 1752 (WQXGA+)",
        "2880 x 1800 (WQXGA+)",
        "2960 x 1848 (WQXGA+)",
        "3088 x 1440 (Quad HD+)",
        "3120 x 1440 (Quad HD+)",
        "3,440 x 1,440",
        "3,840 x 2,160",
        "3,864 x 2,184",
        "396 x 396",
        "432 x 432",
        "438 x 438",
        "4,416 x 2,496",
        "450 x 450",
        "480 x 480",
        "4,968 x 2,808",
        "4 K (3,840 x 2,160)",
        "4K (3,840 x 2,160)",
        "5,120 x 1,440",
        "5,120 x 2,880",
        "7,680 x 2,160",
        "7,680 x 4,320",
        "8K (7,680 x 4,320)",
        "DQHD (5,120 x 1,440)",
        "FHD (1,920 x 1,080)",
        "HD (1,366 x 768)",
        "QHD (2,560 x 1,440)",
        "up to 4K 60Hz",
        "UWQHD (3,440 x 1,440)",
        "WUHD (5,120 x 2,160)",
    ]

    # 파서 인스턴스 생성
    parser = ResolutionParser()

    print("="*80)
    print("확장된 해상도 파서 테스트")
    print("="*80)
    print(f"테스트할 샘플 수: {len(test_samples)}")
    print("-"*80)

    # 통계 초기화
    success_count = 0
    failed_samples = []
    resolution_stats = {}

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
        if success and parsed_rows:
            success_count += 1

            # width와 height 찾기
            width_data = next((r for r in parsed_rows if r['dimension_type'] == 'width'), None)
            height_data = next((r for r in parsed_rows if r['dimension_type'] == 'height'), None)

            if width_data and height_data:
                width = width_data['parsed_value']
                height = height_data['parsed_value']
                resolution_type = width_data.get('resolution_type', None)

                # 해상도 크기 통계
                resolution_key = f"{int(width)}x{int(height)}"
                if resolution_key in resolution_stats:
                    resolution_stats[resolution_key].append(sample)
                else:
                    resolution_stats[resolution_key] = [sample]

                # 간략한 출력 (성공한 경우만)
                if resolution_type:
                    print(f"[{i:2d}] ✓ {sample:40s} → {width:.0f}x{height:.0f} ({resolution_type})")
                else:
                    print(f"[{i:2d}] ✓ {sample:40s} → {width:.0f}x{height:.0f}")
            else:
                print(f"[{i:2d}] ⚠️ {sample:40s} → 부분 파싱")
        else:
            failed_samples.append(sample)
            print(f"[{i:2d}] ✗ {sample:40s} → 파싱 실패")

    # 전체 통계 출력
    print("\n" + "="*80)
    print("테스트 결과 요약")
    print("="*80)
    print(f"전체: {len(test_samples)}개")
    print(f"성공: {success_count}개 ({success_count/len(test_samples)*100:.1f}%)")
    print(f"실패: {len(failed_samples)}개 ({len(failed_samples)/len(test_samples)*100:.1f}%)")

    if failed_samples:
        print("\n❌ 실패한 샘플:")
        for sample in failed_samples:
            print(f"  - {sample}")

    # 해상도 타입별 통계
    print("\n" + "="*80)
    print("해상도 타입 감지 통계")
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
        print("감지된 해상도 타입 (빈도순):")
        for res_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {res_type:12s}: {count:2d}개")
    else:
        print("감지된 해상도 타입 없음")

    # 중복 해상도 값 확인
    print("\n" + "="*80)
    print("동일한 해상도 값을 가진 샘플들")
    print("="*80)

    duplicates_found = False
    for resolution, samples in sorted(resolution_stats.items()):
        if len(samples) > 1:
            duplicates_found = True
            print(f"\n{resolution}:")
            for sample in samples:
                print(f"  - {sample}")

    if not duplicates_found:
        print("중복된 해상도 값 없음")

    return success_count == len(test_samples)


if __name__ == "__main__":
    # 테스트 실행
    all_passed = test_extended_resolution_samples()

    print("\n" + "="*80)
    if all_passed:
        print("✅ 모든 테스트 통과!")
    else:
        print("⚠️ 일부 테스트 실패 - 파서 개선 필요")
    print("="*80)