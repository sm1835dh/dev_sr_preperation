#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
간단한 parsed_string_value 테스트 (pandas 의존성 없음)
"""

import sys
sys.path.append('.')

# pandas 대신 간단한 dict 사용
class MockSeries:
    def __init__(self, data):
        self.data = data

    def get(self, key, default=None):
        return self.data.get(key, default)

    def to_dict(self):
        return self.data.copy()


def test_parser_manually():
    """파서 직접 테스트"""
    print("="*80)
    print("parsed_string_value 필드 수동 테스트")
    print("="*80)

    # ResolutionParser 직접 import
    from parsers.resolution_parser import ResolutionParser

    parser = ResolutionParser()

    test_cases = [
        ('4K (3,840 x 2,160)', True, 3),
        ('FHD (1,920 x 1,080)', True, 3),
        ('1920 x 1080', True, 2),
    ]

    for value, expected_success, expected_rows in test_cases:
        print(f"\n테스트: '{value}'")
        print("-"*40)

        # Mock Series 생성
        row = MockSeries({'value': value})

        # 파싱 실행
        parsed_rows, success, _ = parser.parse(row)

        if success == expected_success:
            print(f"✓ 파싱 {'성공' if success else '실패'} (예상대로)")

            if success:
                if len(parsed_rows) == expected_rows:
                    print(f"✓ {len(parsed_rows)}개 row 생성 (예상: {expected_rows}개)")
                else:
                    print(f"✗ {len(parsed_rows)}개 row 생성 (예상: {expected_rows}개)")

                # 각 row 검사
                for i, row in enumerate(parsed_rows, 1):
                    dim_type = row.get('dimension_type')

                    if dim_type == 'resolution_name':
                        # resolution_name은 parsed_string_value에 있어야 함
                        if 'parsed_string_value' in row and 'parsed_value' not in row:
                            print(f"  [{i}] ✓ {dim_type}: parsed_string_value='{row['parsed_string_value']}'")
                        else:
                            print(f"  [{i}] ✗ {dim_type}: 잘못된 필드 구조")
                            if 'parsed_value' in row:
                                print(f"       (오류: parsed_value에 저장됨)")

                    elif dim_type in ['width', 'height']:
                        # width/height는 parsed_value에 있어야 함
                        if 'parsed_value' in row and 'parsed_string_value' not in row:
                            print(f"  [{i}] ✓ {dim_type}: parsed_value={row['parsed_value']}")
                        else:
                            print(f"  [{i}] ✗ {dim_type}: 잘못된 필드 구조")
                            if 'parsed_string_value' in row:
                                print(f"       (오류: parsed_string_value에 저장됨)")
        else:
            print(f"✗ 예상과 다른 결과: {'성공' if success else '실패'}")

    print("\n" + "="*80)
    print("✅ 올바른 저장 규칙:")
    print("-"*40)
    print("• dimension_type='width'  → parsed_value (float)")
    print("• dimension_type='height' → parsed_value (float)")
    print("• dimension_type='resolution_name' → parsed_string_value (string)")
    print("="*80)


if __name__ == "__main__":
    test_parser_manually()