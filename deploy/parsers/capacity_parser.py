"""
용량(Capacity) 파싱 모듈

이 모듈은 제품 스펙 데이터에서 용량 정보를 파싱합니다.
다양한 단위와 형식의 용량 데이터를 처리할 수 있습니다.

예시:
- "5,000 mAh" → mAh 5000
- "512 GB" → GB 512
- "1TB" → TB 1
- "25 kg" → kg 25
- "62.6 ㎡" → 제곱미터 62.6
- "6인용" → 인용 6
- "상의 5~9 벌 + 하의 5 벌" → 상의 9, 하의 5
"""
import re
import pandas as pd
from .base_parser import BaseParser


class CapacityParser(BaseParser):
    """
    용량 데이터 파싱 클래스
    """

    def get_goal(self):
        """파서가 처리하는 goal 값"""
        return '용량'

    def identify_type(self, text):
        """
        텍스트에서 용량 단위 타입을 식별

        Parameters:
        -----------
        text : str
            분석할 텍스트

        Returns:
        --------
        str or None : 식별된 타입
        """
        text = text.strip()

        # 단위 키워드 매핑 (순서 중요 - 더 구체적인 것을 먼저)
        type_patterns = [
            # 배터리 용량
            (r'mAh', 'mAh'),
            (r'Wh', 'Wh'),
            # 저장 용량
            (r'TB', 'TB'),
            (r'GB', 'GB'),
            (r'MB', 'MB'),
            (r'KB', 'KB'),
            # 무게
            (r'kg', 'kg'),
            (r'g\b', 'g'),
            # 부피/용량
            (r'[ℓL]\b', '리터'),
            (r'ml', 'ml'),
            (r'cc', 'cc'),
            # 면적
            (r'㎡|m²|m2', '제곱미터'),
            (r'평', '평'),
            # 수량 단위
            (r'인용', '인용'),
            (r'켤레', '켤레'),
            (r'매\b', '매'),
            (r'벌\b', '벌'),
            (r'개\b', '개'),
            # 의류 관련
            (r'상의', '상의'),
            (r'하의', '하의'),
        ]

        for pattern, dtype in type_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return dtype

        return None

    def parse(self, row):
        """
        용량 파싱 함수

        Parameters:
        -----------
        row : pandas.Series
            파싱할 데이터 행

        Returns:
        --------
        tuple : (parsed_rows, success, needs_check)
        """
        parsed_rows = []
        value = str(row['value']).strip()
        disp_nm2 = str(row.get('disp_nm2', '')).strip()

        # 기본 행 데이터
        base_row = row.to_dict()

        # ============================================
        # 전처리: "환산용량", "정격용량" 등 부가 설명 제거
        # ============================================
        # "정격용량 (7.4 V) 5,000 mAh, 37Wh  /    환산용량 (3.7 V) 10,000 mAh"
        # → 첫 번째 값만 추출
        if '/' in value and ('환산' in value or '정격' in value):
            # '/' 앞부분만 사용
            value = value.split('/')[0].strip()

        # 괄호 안의 전압 정보 제거: "(7.4 V)", "(3.7 V)" 등
        value = re.sub(r'\([0-9.]+\s*V\)', '', value)

        # ============================================
        # 패턴 1: "상의 5~9 벌 + 하의 5 벌" 형식 (복수 항목)
        # ============================================
        # "타입 숫자~숫자 단위" 패턴
        clothing_pattern = r'(상의|하의)\s*([0-9]+)(?:~([0-9]+))?\s*([가-힣]+)'
        clothing_matches = re.findall(clothing_pattern, value)

        if len(clothing_matches) >= 1:
            for clothing_type, num1, num2, unit in clothing_matches:
                # 범위가 있으면 최대값 사용
                number = float(num2) if num2 else float(num1)
                dim_type = clothing_type

                new_row = base_row.copy()
                new_row['dimension_type'] = dim_type
                new_row['parsed_value'] = number
                new_row['needs_check'] = False
                parsed_rows.append(new_row)

            if parsed_rows:
                return parsed_rows, True, False

        # ============================================
        # 패턴 2: "숫자 + 단위" 또는 "숫자단위" 형식
        # ============================================
        # 예: "512 GB", "1TB", "25 kg", "62.6 ㎡", "420 ℓ"

        # 모든 숫자와 단위 조합 추출
        unit_patterns = [
            # 배터리
            (r'([0-9,]+(?:\.[0-9]+)?)\s*mAh', 'mAh'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*Wh', 'Wh'),
            # 저장 용량
            (r'([0-9,]+(?:\.[0-9]+)?)\s*TB', 'TB'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*GB', 'GB'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*MB', 'MB'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*KB', 'KB'),
            # 무게
            (r'([0-9,]+(?:\.[0-9]+)?)\s*kg', 'kg'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*g\b', 'g'),
            # 부피
            (r'([0-9,]+(?:\.[0-9]+)?)\s*[ℓL]', '리터'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*ml', 'ml'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*cc', 'cc'),
            # 면적
            (r'([0-9,]+(?:\.[0-9]+)?)\s*(?:㎡|m²|m2)', '제곱미터'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*평', '평'),
            # 수량
            (r'([0-9,]+(?:\.[0-9]+)?)\s*켤레', '켤레'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*매\b', '매'),
            (r'([0-9,]+(?:\.[0-9]+)?)\s*개\b', '개'),
        ]

        for pattern, unit_type in unit_patterns:
            match = re.search(pattern, value, re.IGNORECASE)
            if match:
                num_str = match.group(1).replace(',', '')
                try:
                    number = float(num_str)
                    new_row = base_row.copy()
                    new_row['dimension_type'] = unit_type
                    new_row['parsed_value'] = number
                    new_row['needs_check'] = False
                    parsed_rows.append(new_row)
                    return parsed_rows, True, False
                except ValueError:
                    continue

        # ============================================
        # 패턴 3: "숫자인용" 또는 "0숫자인용" 형식
        # ============================================
        # 예: "6인용", "06인용"
        inyong_match = re.search(r'0?([0-9]+)\s*인용', value)
        if inyong_match:
            number = float(inyong_match.group(1))
            new_row = base_row.copy()
            new_row['dimension_type'] = '인용'
            new_row['parsed_value'] = number
            new_row['needs_check'] = False
            parsed_rows.append(new_row)
            return parsed_rows, True, False

        # ============================================
        # 패턴 4: 단순 숫자만 있는 경우
        # ============================================
        # 예: "23.5", "4400"
        simple_number_match = re.search(r'^([0-9,]+(?:\.[0-9]+)?)$', value.strip())
        if simple_number_match:
            num_str = simple_number_match.group(1).replace(',', '')
            try:
                number = float(num_str)
                new_row = base_row.copy()
                new_row['dimension_type'] = None
                new_row['parsed_value'] = number
                new_row['needs_check'] = False
                parsed_rows.append(new_row)
                return parsed_rows, True, False
            except ValueError:
                pass

        return parsed_rows, False, False
