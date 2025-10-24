#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
해상도 파서 요약 테스트
"""

import pandas as pd
import sys
sys.path.append('.')

from parsers.resolution_parser import ResolutionParser


def test_resolution_summary():
    """해상도 파서 요약 테스트"""

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
    print(" " * 25 + "🖥️  해상도 파서 테스트 요약")
    print("="*80)

    # 테스트 실행
    success_count = 0
    failed_samples = []
    type_detected = 0
    no_type = 0

    for sample in test_samples:
        test_row = pd.Series({'value': sample})
        parsed_rows, success, _ = parser.parse(test_row)

        if success and parsed_rows:
            success_count += 1
            if parsed_rows[0].get('resolution_type'):
                type_detected += 1
            else:
                no_type += 1
        else:
            failed_samples.append(sample)

    # 결과 출력
    print(f"📊 테스트 샘플: {len(test_samples)}개")
    print("-"*80)

    print(f"✅ 파싱 성공: {success_count}개 ({success_count/len(test_samples)*100:.1f}%)")
    if failed_samples:
        print(f"❌ 파싱 실패: {len(failed_samples)}개 ({len(failed_samples)/len(test_samples)*100:.1f}%)")
        for sample in failed_samples[:5]:  # 처음 5개만 표시
            print(f"   - {sample}")
        if len(failed_samples) > 5:
            print(f"   ... 외 {len(failed_samples)-5}개")

    print("-"*80)
    print(f"🏷️  표준 타입 감지: {type_detected}개 ({type_detected/success_count*100:.1f}%)")
    print(f"📝 타입 미감지: {no_type}개 ({no_type/success_count*100:.1f}%)")

    # 지원하는 표준 타입 목록
    print("\n" + "="*80)
    print(" " * 20 + "📋 지원하는 표준 해상도 타입")
    print("="*80)

    # 카테고리별로 정리
    categories = {
        'HD 계열': ['HD', 'HD+', 'FHD', 'FHD+'],
        'QHD 계열': ['QHD', 'QUAD HD', 'QUAD HD+', 'WQHD', 'DQHD'],
        'XGA 계열': ['WXGA', 'WXGA+', 'WUXGA', 'WUXGA+', 'QXGA+', 'WQXGA', 'WQXGA+'],
        '4K/8K 계열': ['4K', '4 K', 'UHD', '8K'],
        'Ultra Wide': ['UWQHD', 'WUHD'],
    }

    for category, types in categories.items():
        print(f"\n{category}:")
        for res_type in types:
            if res_type in parser.RESOLUTION_STANDARDS:
                width, height = parser.RESOLUTION_STANDARDS[res_type]
                print(f"  • {res_type:12s}: {width:5.0f} x {height:5.0f}")

    # 파싱 기능 요약
    print("\n" + "="*80)
    print(" " * 25 + "✨ 파싱 기능 요약")
    print("="*80)
    print("""
✓ 다양한 형식 지원:
  • 숫자 x 숫자 (예: 1920 x 1080)
  • 콤마 포함 (예: 1,920 x 1,080)
  • 괄호 내 표준 타입 (예: FHD (1,920 x 1,080))
  • "up to" 표현 (예: up to 4K 60Hz)

✓ 출력 데이터 구조:
  • dimension_type: 'width' 또는 'height'
  • parsed_value: 숫자 값
  • resolution_type: 표준 해상도 타입 (감지된 경우)

✓ 데이터베이스 저장:
  • 각 해상도는 2개 row로 저장 (width, height)
  • goal='해상도'로 구분
""")

    print("="*80)
    if success_count == len(test_samples):
        print(" " * 25 + "✅ 모든 테스트 통과!")
    else:
        print(" " * 20 + f"⚠️ {len(failed_samples)}개 실패 - 개선 필요")
    print("="*80)

    return success_count == len(test_samples)


if __name__ == "__main__":
    test_resolution_summary()