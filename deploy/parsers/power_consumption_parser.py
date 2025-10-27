"""
소비전력(Power Consumption) 파싱 모듈

이 모듈은 제품 스펙 데이터에서 소비전력 정보를 파싱합니다.
다양한 형식의 소비전력 데이터를 처리할 수 있습니다.

예시:
- "2050" → 2050 (dimension_type=None)
- "(23.5.1일 부 개정 기준) 15.9 kWh/월" → 15.9 (dimension_type=None)
- "(세트) 70W (스테이션) 1200W" → 세트 70, 스테이션 1200
- "가열세탁 시 2200W" → 가열세탁 2200
- "가열세탁 시 2200 W/건조 2400 W" → 가열세탁 2200, 건조 2400
- "표준세탁 시 800 W/가열세탁 시 1400 W" → 표준세탁 800, 가열세탁 1400
"""
import re
import pandas as pd
from .base_parser import BaseParser


class PowerConsumptionParser(BaseParser):
    """
    소비전력 데이터 파싱 클래스
    """

    def get_goal(self):
        """파서가 처리하는 goal 값"""
        return '소비전력'

    def identify_type(self, text):
        """
        텍스트에서 소비전력 타입을 식별

        Parameters:
        -----------
        text : str
            분석할 텍스트

        Returns:
        --------
        str or None : 식별된 타입
        """
        text = text.strip()

        # 타입 키워드 매핑
        type_patterns = [
            # 세탁기 관련
            (r'가열\s*세탁', '가열세탁'),
            (r'표준\s*세탁', '표준세탁'),
            (r'냉수\s*세탁', '냉수세탁'),
            (r'온수', '온수'),
            (r'냉수', '냉수'),
            (r'건조', '건조'),
            (r'삶음', '삶음'),
            # 세트/스테이션
            (r'세트', '세트'),
            (r'스테이션', '스테이션'),
            # 프린터/복합기 관련
            (r'인쇄\s*시', '인쇄'),
            (r'printing', '인쇄'),
            (r'평균\s*작동', '평균'),
            (r'평균', '평균'),
            (r'동작\s*시', '동작'),
            (r'작동\s*모드', '작동'),
            (r'사용\s*중', '사용중'),
            (r'대기\s*모드', '대기'),
            (r'대기\s*시', '대기'),
            (r'ready', '대기'),
            (r'sleep', None),  # skip
            (r'슬립\s*모드', None),  # skip
            (r'절전', None),  # skip
            (r'off', None),  # skip
        ]

        for pattern, dtype in type_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return dtype

        return None

    def parse(self, row):
        """
        소비전력 파싱 함수

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
        # 제외 조건: skip 키워드가 있는 경우
        # ============================================
        skip_patterns = [
            r'슬립\s*모드',
            r'오프\s*모드',
            r'절전',
            r'sleep\s*mode',
            r'sleep\b',
            r'off\s*mode',
            r'auto-off',
            r'manual-off',
            r'\boff\b',  # OFF 단독
        ]

        # 슬립/오프/절전 모드가 있으면서 다른 모드가 없는 경우만 skip
        has_skip_keyword = any(re.search(pattern, value, re.IGNORECASE) for pattern in skip_patterns)
        # 유효한 키워드: 인쇄, 동작, 세탁, 건조, 세트, 스테이션, ready, 평균, 사용중, 작동
        has_valid_keyword = any(keyword in value.lower() for keyword in [
            '인쇄', '동작', '세탁', '건조', '세트', '스테이션', 'printing',
            'ready', '평균', '사용중', '작동'
        ])

        if has_skip_keyword and not has_valid_keyword:
            # 슬립/오프만 있고 유효한 키워드가 없으면 skip
            return parsed_rows, False, False

        # ============================================
        # 전처리: 날짜 형식 필터링 (소비전력량 개정 기준 등)
        # ============================================
        # 날짜 패턴 제거: "23.5.1일 부", "21.10.1일 부" 등
        # 괄호 안에 있는 날짜 패턴 제거
        date_pattern = r'\([0-9]{2,4}\.[0-9]{1,2}\.[0-9]{1,2}일?\s*부[^)]*\)'
        value = re.sub(date_pattern, '', value)

        # ============================================
        # 패턴 1: 복수 값 - "타입1 숫자1, 타입2 숫자2" 형식
        # ============================================
        # 예: "(세트) 70W (스테이션) 1200W"
        # 예: "가열세탁 시 2200 W/건조 2400 W"
        # 예: "표준세탁 시 800 W/가열세탁 시 1400 W"
        # 예: "냉수세탁 시 : 120 W/가열세탁 시 : 1800 W"

        # 특수 패턴: "(타입) 숫자W" 형식
        paren_type_pattern = r'\(([^)]+)\)\s*([0-9,]+(?:\.[0-9]+)?)\s*(?:k?W|watts?)'
        paren_type_matches = re.findall(paren_type_pattern, value, re.IGNORECASE)

        if len(paren_type_matches) >= 2:
            # "(세트) 70W (스테이션) 1200W" 같은 패턴
            for type_name, num in paren_type_matches:
                dim_type = self.identify_type(type_name)
                try:
                    clean_num = num.replace(',', '').strip()
                    parsed_num = float(clean_num)

                    new_row = base_row.copy()
                    new_row['dimension_type'] = None  # width/height/depth만 허용하므로 None
                    new_row['parsed_value'] = parsed_num
                    new_row['parsed_symbols'] = dim_type if dim_type else 'W'  # 타입 정보를 parsed_symbols에 저장
                    new_row['needs_check'] = False
                    parsed_rows.append(new_row)
                except ValueError:
                    continue

            if parsed_rows:
                return parsed_rows, True, False

        # 괄호 또는 슬래시로 구분된 여러 항목 찾기
        # 콤마로 구분된 "타입 : 숫자" 패턴도 처리
        # 예: "OFF : 0.11 W, Ready : 3.02 W, Sleep : 1.96W"
        comma_sep_pattern = r'([가-힣a-zA-Z\s]+?)\s*[:：]\s*([0-9,]+(?:\.[0-9]+)?)\s*(?:k?Wh?|watts?)?'
        comma_matches = re.findall(comma_sep_pattern, value, re.IGNORECASE)

        if len(comma_matches) >= 2:
            # 콤마로 구분된 항목이 있으면 우선 처리
            for prefix, num in comma_matches:
                prefix = prefix.strip()
                # skip 키워드 체크
                is_skip = any(re.search(pattern, prefix, re.IGNORECASE) for pattern in skip_patterns)
                if not is_skip:
                    dim_type = self.identify_type(prefix)
                    try:
                        clean_num = num.replace(',', '').strip()
                        parsed_num = float(clean_num)

                        new_row = base_row.copy()
                        new_row['dimension_type'] = dim_type
                        new_row['parsed_value'] = parsed_num
                        new_row['needs_check'] = False
                        parsed_rows.append(new_row)
                    except ValueError:
                        continue

            if parsed_rows:
                return parsed_rows, True, False

        # 일반 패턴 (슬래시로 구분)
        # 패턴 개선: 단어 경계를 더 명확하게
        multi_pattern = r'([가-힣a-zA-Z\s]+?)\s*[:：]?\s*([0-9,]+(?:\.[0-9]+)?)\s*(?:k?Wh?|watts?)?'
        matches = re.findall(multi_pattern, value, re.IGNORECASE)

        # 유효한 매치만 필터링 (타입이나 숫자가 있는 것)
        valid_matches = []
        for prefix, num in matches:
            prefix = prefix.strip()
            if num and prefix:  # 둘 다 있어야 함
                # skip 키워드 체크
                is_skip = any(re.search(pattern, prefix, re.IGNORECASE) for pattern in skip_patterns)
                if not is_skip:
                    valid_matches.append((prefix, num))

        # "숫자 W (타입)" 패턴도 처리
        # 예: "400 W (평균 작동 모드) / 50 W (대기 모드)"
        num_then_type_pattern = r'([0-9,]+(?:\.[0-9]+)?)\s*(?:k?W|watts?)\s*\(([^)]+)\)'
        num_then_type_matches = re.findall(num_then_type_pattern, value, re.IGNORECASE)

        if len(num_then_type_matches) >= 2:
            # "숫자 (타입)" 형식이 여러 개 있으면 우선 처리
            for num, type_desc in num_then_type_matches:
                # skip 키워드 체크
                is_skip = any(re.search(pattern, type_desc, re.IGNORECASE) for pattern in skip_patterns)
                if is_skip:
                    continue

                dim_type = self.identify_type(type_desc)
                try:
                    clean_num = num.replace(',', '').strip()
                    parsed_num = float(clean_num)

                    new_row = base_row.copy()
                    new_row['dimension_type'] = None  # width/height/depth만 허용하므로 None
                    new_row['parsed_value'] = parsed_num
                    new_row['parsed_symbols'] = dim_type if dim_type else 'W'  # 타입 정보를 parsed_symbols에 저장
                    new_row['needs_check'] = False
                    parsed_rows.append(new_row)
                except ValueError:
                    continue

            if parsed_rows:
                return parsed_rows, True, False

        # 2개 이상의 유효한 값이 있으면 복수 값 패턴으로 처리
        if len(valid_matches) >= 2:
            # 냉수/온수 패턴 필터링: W가 아닌 A 단위인 경우 제외
            # "냉수 : 0.75A / 온수 : 2,600W" → 온수만 선택
            filtered_matches = []
            for prefix, num in valid_matches:
                # 해당 숫자 뒤의 원본 텍스트 찾기
                num_pos = value.find(num)
                if num_pos != -1:
                    # 숫자 뒤 10자 확인
                    after_num = value[num_pos + len(num):num_pos + len(num) + 10]
                    # A 단위인지 확인 (W가 아닌 경우)
                    if re.search(r'^\s*A\b', after_num) and not re.search(r'^\s*W', after_num):
                        # A 단위는 skip
                        continue

                filtered_matches.append((prefix, num))

            # 필터링 후 여전히 2개 이상이면 처리
            if len(filtered_matches) >= 2:
                for prefix, num in filtered_matches:
                    dim_type = self.identify_type(prefix)

                    # skip으로 식별되면 건너뛰기
                    if dim_type is None and any(re.search(pattern, prefix, re.IGNORECASE) for pattern in skip_patterns):
                        continue

                    try:
                        # 숫자 정리
                        clean_num = num.replace(',', '').strip()
                        parsed_num = float(clean_num)

                        new_row = base_row.copy()
                        new_row['dimension_type'] = dim_type
                        new_row['parsed_value'] = parsed_num
                        new_row['needs_check'] = False
                        parsed_rows.append(new_row)
                    except ValueError:
                        continue

                if parsed_rows:
                    return parsed_rows, True, False

        # ============================================
        # 패턴 2: 단일 값 - 가장 큰 또는 가장 관련성 높은 숫자 추출
        # ============================================
        # 예: "2050" → 2050
        # 예: "15.9 kWh/월" → 15.9
        # 예: "가열 세탁 시 2200W" → 2200
        # 예: "820 watts (40ppm, printing), 41.5 watts (ready), 0.8 watts (sleep)"

        # 특수 케이스: 괄호 안에 여러 모드가 나열된 경우
        # "820 watts (40ppm, printing), 41.5 watts (ready), 0.8 watts (sleep)"
        # → printing, ready는 유효, sleep은 skip
        paren_pattern = r'([0-9,]+(?:\.[0-9]+)?)\s*(?:k?Wh?|watts?)\s*\(([^)]+)\)'
        paren_matches = re.findall(paren_pattern, value, re.IGNORECASE)

        if paren_matches:
            # 괄호 안의 설명을 확인하여 유효한 값만 추출
            valid_paren_numbers = []
            for num, desc in paren_matches:
                # 설명에 skip 키워드가 없는지 확인
                is_skip = any(re.search(pattern, desc, re.IGNORECASE) for pattern in skip_patterns)
                if not is_skip:
                    try:
                        clean_num = num.replace(',', '')
                        valid_paren_numbers.append(float(clean_num))
                    except ValueError:
                        continue

            if valid_paren_numbers:
                # 가장 큰 값 선택
                max_value = max(valid_paren_numbers)
                dim_type = self.identify_type(value)

                new_row = base_row.copy()
                new_row['dimension_type'] = None  # width/height/depth만 허용하므로 None
                new_row['parsed_value'] = max_value
                new_row['parsed_symbols'] = dim_type if dim_type else 'W'  # 타입 정보를 parsed_symbols에 저장
                new_row['needs_check'] = False
                parsed_rows.append(new_row)

                return parsed_rows, True, False

        # 일반 패턴: 모든 숫자 추출
        all_numbers = re.findall(r'([0-9,]+(?:\.[0-9]+)?)', value)

        if not all_numbers:
            return parsed_rows, False, False

        # skip 키워드가 없는 부분에서 숫자 찾기
        # 각 숫자 앞뒤의 텍스트를 확인하여 skip 키워드가 있으면 제외
        # A 단위도 제외 (전류가 아닌 전력만)
        valid_numbers = []
        for num in all_numbers:
            # 해당 숫자의 위치 찾기
            num_pos = value.find(num)
            # 숫자 앞의 텍스트 추출 (최대 50자)
            prefix_text = value[max(0, num_pos - 50):num_pos]
            # 숫자 뒤의 텍스트 추출 (최대 10자)
            suffix_text = value[num_pos + len(num):min(len(value), num_pos + len(num) + 10)]

            # skip 키워드 체크
            is_skip = any(re.search(pattern, prefix_text, re.IGNORECASE) for pattern in skip_patterns)
            # A 단위 체크 (W가 아닌 경우)
            is_ampere = re.search(r'^\s*A\b', suffix_text) and not re.search(r'^\s*W', suffix_text)

            if not is_skip and not is_ampere:
                try:
                    clean_num = num.replace(',', '')
                    valid_numbers.append(float(clean_num))
                except ValueError:
                    continue

        if not valid_numbers:
            return parsed_rows, False, False

        # 가장 큰 값 선택
        max_value = max(valid_numbers)

        # dimension_type 식별
        # 전체 텍스트에서 타입 식별 시도
        dim_type = self.identify_type(value)

        # 특수 케이스: 여러 모드가 있을 때 타입이 None이면 안 됨
        # "600 W (인쇄 시), 13 W (대기 시)" → dimension_type should be None (단순히 가장 큰 값)
        # 하지만 유저 요구사항에 따르면 None이어야 함
        # 따라서 복수 항목이 있어도 타입이 명확하지 않으면 None으로 처리
        if dim_type:
            # 타입이 있으면 그대로 사용
            pass
        else:
            # 타입이 없으면 None으로 (사용자 요구사항에 따라)
            dim_type = None

        # dimension_type이 None이어도 파싱 성공으로 처리 (단일 값인 경우)
        new_row = base_row.copy()
        new_row['dimension_type'] = None  # width/height/depth만 허용하므로 None
        new_row['parsed_value'] = max_value
        new_row['parsed_symbols'] = dim_type if dim_type else 'W'  # 타입 정보를 parsed_symbols에 저장
        new_row['needs_check'] = False
        parsed_rows.append(new_row)

        return parsed_rows, True, False
