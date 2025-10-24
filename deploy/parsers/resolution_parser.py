"""
해상도(Resolution) 파싱 모듈

이 모듈은 디스플레이 해상도 정보를 파싱합니다.
다양한 형식의 해상도 데이터를 처리하고 표준 해상도 타입도 추출합니다.
"""
import re
import pandas as pd
from .base_parser import BaseParser


class ResolutionParser(BaseParser):
    """
    해상도 데이터 파싱 클래스
    """

    # 표준 해상도 매핑
    RESOLUTION_STANDARDS = {
        # HD 계열
        'HD': (1366, 768),
        'HD+': (1600, 900),
        'FHD': (1920, 1080),
        'FHD+': (2340, 1080),  # 모바일용 변형 포함

        # QHD/WQHD 계열
        'QHD': (2560, 1440),
        'QUAD HD': (2560, 1440),
        'QUAD HD+': (3120, 1440),
        'WQHD': (2560, 1440),
        'DQHD': (5120, 1440),  # Double QHD

        # XGA 계열
        'WXGA': (1280, 800),
        'WXGA+': (1340, 800),
        'WUXGA': (1920, 1200),
        'WUXGA+': (2304, 1440),  # 여러 변형 존재
        'QXGA+': (2176, 1812),  # 여러 변형 존재
        'WQXGA': (2560, 1600),
        'WQXGA+': (2960, 1848),  # 여러 변형 존재

        # 4K/UHD 계열
        '4K': (3840, 2160),
        'UHD': (3840, 2160),
        '4K UHD': (3840, 2160),
        '4 K': (3840, 2160),  # 띄어쓰기 변형

        # 8K 계열
        '8K': (7680, 4320),
        '8K UHD': (7680, 4320),

        # Wide/Ultra Wide 계열
        'UWQHD': (3440, 1440),
        'WUHD': (5120, 2160),

        # 기타
        'WSXGA+': (1680, 1050),
    }

    def get_goal(self):
        """파서가 처리하는 goal 값"""
        return '해상도'

    def identify_type(self, text):
        """해상도 타입 식별은 사용하지 않음"""
        return None

    def extract_resolution_type(self, text):
        """
        텍스트에서 해상도 타입 추출

        Parameters:
        -----------
        text : str
            분석할 텍스트

        Returns:
        --------
        str or None : 표준 해상도 타입 (예: 'FHD', '4K' 등)
        """
        text_upper = text.upper()

        # 정확한 매칭을 위해 긴 것부터 확인
        sorted_standards = sorted(self.RESOLUTION_STANDARDS.keys(), key=len, reverse=True)

        for standard in sorted_standards:
            # 괄호 안에 있는 경우 (예: "(FHD)")
            if f"({standard})" in text_upper:
                return standard
            # 독립적으로 있는 경우
            if re.search(r'\b' + re.escape(standard) + r'\b', text_upper):
                return standard

        return None

    def normalize_number(self, num_str):
        """
        숫자 문자열 정규화 (콤마 제거 등)

        Parameters:
        -----------
        num_str : str
            숫자 문자열

        Returns:
        --------
        str : 정규화된 숫자 문자열
        """
        # 콤마 제거
        normalized = num_str.replace(',', '').replace(' ', '')
        return normalized

    def parse(self, row):
        """
        해상도 데이터 파싱

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

        # 해상도 타입 추출
        resolution_type = self.extract_resolution_type(value)

        # 패턴 1: "up to 4K 60Hz" 형식 처리
        if 'up to' in value.lower():
            # 표준 해상도 타입이 있으면 그 값을 사용
            if resolution_type and resolution_type in self.RESOLUTION_STANDARDS:
                width, height = self.RESOLUTION_STANDARDS[resolution_type]
                base_row = row.to_dict()

                # width
                parsed_rows.append({
                    **base_row,
                    'dimension_type': 'width',
                    'parsed_value': float(width),
                    'needs_check': False
                })
                # height
                parsed_rows.append({
                    **base_row,
                    'dimension_type': 'height',
                    'parsed_value': float(height),
                    'needs_check': False
                })
                # resolution_type을 dimension_type으로 저장
                if resolution_type:
                    parsed_rows.append({
                        **base_row,
                        'dimension_type': resolution_type,
                        'parsed_value': None,  # 또는 적절한 값
                        'needs_check': False
                    })
                return parsed_rows, True, False
            else:
                # 표준 타입이 없으면 파싱 실패
                return [], False, False

        # 패턴 2: 숫자 x 숫자 형식 (다양한 구분자 지원)
        # 예: "1920 x 1080", "1,920 x 1,080", "1920x1080"
        resolution_pattern = r'([0-9,]+(?:\.[0-9]+)?)\s*[xX×]\s*([0-9,]+(?:\.[0-9]+)?)'
        match = re.search(resolution_pattern, value)

        if match:
            width_str, height_str = match.groups()

            try:
                # 콤마 제거하고 숫자 파싱
                width = float(self.normalize_number(width_str))
                height = float(self.normalize_number(height_str))

                base_row = row.to_dict()

                # width 추가
                parsed_rows.append({
                    **base_row,
                    'dimension_type': 'width',
                    'parsed_value': width,
                    'needs_check': False
                })

                # height 추가
                parsed_rows.append({
                    **base_row,
                    'dimension_type': 'height',
                    'parsed_value': height,
                    'needs_check': False
                })

                # resolution_type이 있으면 dimension_type으로 추가
                if resolution_type:
                    parsed_rows.append({
                        **base_row,
                        'dimension_type': resolution_type,
                        'parsed_value': None,  # 또는 적절한 값
                        'needs_check': False
                    })

                return parsed_rows, True, False

            except ValueError:
                pass

        # 패턴 3: 표준 해상도 타입만 있는 경우
        # 예: "FHD", "4K", "8K"
        if resolution_type and resolution_type in self.RESOLUTION_STANDARDS:
            width, height = self.RESOLUTION_STANDARDS[resolution_type]
            base_row = row.to_dict()

            # width
            parsed_rows.append({
                **base_row,
                'dimension_type': 'width',
                'parsed_value': float(width),
                'needs_check': False
            })
            # height
            parsed_rows.append({
                **base_row,
                'dimension_type': 'height',
                'parsed_value': float(height),
                'needs_check': False
            })
            # resolution_type을 dimension_type으로 저장
            parsed_rows.append({
                **base_row,
                'dimension_type': resolution_type,
                'parsed_value': None,  # 또는 적절한 값
                'needs_check': False
            })
            return parsed_rows, True, False

        # 파싱 실패
        return [], False, False