"""
에너지 소비효율 등급 파싱 모듈

이 모듈은 에너지 소비효율 등급 정보를 파싱합니다.
다양한 형식의 등급 데이터를 처리하여 최종 등급 숫자를 추출합니다.
"""
import re
import pandas as pd
from .base_parser import BaseParser


class EnergyEfficiencyParser(BaseParser):
    """
    에너지 소비효율 등급 파싱 클래스

    처리 예시:
    - "3등급 (21.10.1일 부 에너지등급 개정 기준)" -> 3
    - "(개정 전) 1등급 → (23.5.1일 부 개정 기준) 2등급" -> 2 (최종 등급)
    - "1등급(구효율등급)→3등급(23.5.1일 부 개정 기준)" -> 3 (최종 등급)
    - "1 등급" -> 1
    - "5" -> 5

    스킵 패턴:
    - "최저소비효율기준 만족"
    - "등급 外"
    - "무등급"
    """

    def get_goal(self):
        """파서가 처리하는 goal 값"""
        return '소비효율'

    def identify_type(self, text):
        """소비효율은 단일 타입이므로 None 반환"""
        return None

    def should_skip(self, text):
        """
        스킵해야 하는 패턴인지 확인

        Parameters:
        -----------
        text : str
            확인할 텍스트

        Returns:
        --------
        bool : 스킵해야 하면 True
        """
        if not text:
            return True

        text = text.strip()

        # 스킵 패턴 목록
        skip_patterns = [
            '최저소비효율기준',
            '최저소비효율',
            '등급 외',
            '등급外',
            '무등급',
            '해당없음',
            '해당 없음',
        ]

        text_lower = text.lower()
        for pattern in skip_patterns:
            if pattern.lower() in text_lower:
                return True

        return False

    def extract_grade_number(self, text):
        """
        텍스트에서 등급 숫자 추출

        Parameters:
        -----------
        text : str
            분석할 텍스트

        Returns:
        --------
        int or None : 추출된 등급 숫자 (1-5)
        """
        if not text:
            return None

        # 스킵 패턴 확인
        if self.should_skip(text):
            return None

        # 패턴 1: 화살표(→)가 있는 경우 - 개정 후 등급 추출
        # 예: "(개정 전) 1등급 → (23.5.1일 부 개정 기준) 2등급"
        # 예: "1등급(구효율등급)→3등급(23.5.1일 부 개정 기준)"
        if '→' in text:
            # 화살표 뒤의 텍스트에서 등급 추출
            after_arrow = text.split('→')[-1]
            # 등급 패턴: 숫자 + "등급" 또는 단독 숫자
            grade_match = re.search(r'(\d+)\s*등급', after_arrow)
            if grade_match:
                grade = int(grade_match.group(1))
                if 1 <= grade <= 5:
                    return grade

        # 패턴 2: "N등급" 형식 (가장 마지막에 나오는 등급 추출)
        # 예: "3등급 (21.10.1일 부 에너지등급 개정 기준)"
        # 예: "1 등급"
        grade_matches = re.findall(r'(\d+)\s*등급', text)
        if grade_matches:
            # 마지막 등급 숫자 사용
            grade = int(grade_matches[-1])
            if 1 <= grade <= 5:
                return grade

        # 패턴 3: 단독 숫자 (1-5 사이의 숫자만)
        # 예: "1", "5"
        # 괄호나 날짜가 아닌 독립적인 숫자만 추출
        # 날짜 패턴 제외 (예: "21.10.1")
        text_cleaned = re.sub(r'\d+\.\d+\.\d+', '', text)  # 날짜 제거
        text_cleaned = re.sub(r'\([^)]*\)', '', text_cleaned)  # 괄호 내용 제거

        # 1-5 사이의 단독 숫자 찾기
        single_digit_match = re.search(r'\b([1-5])\b', text_cleaned)
        if single_digit_match:
            grade = int(single_digit_match.group(1))
            return grade

        return None

    def parse(self, row):
        """
        에너지 소비효율 데이터 파싱

        Parameters:
        -----------
        row : pandas.Series
            파싱할 데이터 행

        Returns:
        --------
        tuple : (parsed_rows, success, needs_check)
        """
        parsed_rows = []
        value = str(row.get('value', ''))

        if not value or value == 'nan':
            return [], False, False

        # 등급 숫자 추출
        grade = self.extract_grade_number(value)

        if grade is not None:
            base_row = row.to_dict()

            # 등급 정보 추가
            parsed_rows.append({
                **base_row,
                'dimension_type': None,  # width/height/depth만 허용하므로 None
                'parsed_value': float(grade),
                'parsed_symbols': '등급',  # 에너지 효율 등급을 parsed_symbols에 저장
                'needs_check': False
            })

            return parsed_rows, True, False

        # 파싱 실패
        return [], False, False
