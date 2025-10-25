"""
무게(Weight) 파싱 모듈

이 모듈은 제품 스펙 데이터에서 무게 정보를 파싱합니다.
다양한 형식의 무게 데이터를 처리할 수 있습니다.
"""
import re
import pandas as pd
from .base_parser import BaseParser


class WeightParser(BaseParser):
    """
    무게 데이터 파싱 클래스
    """

    def get_goal(self):
        """파서가 처리하는 goal 값"""
        return '무게'

    def identify_type(self, text):
        """
        텍스트에서 무게 관련 타입을 식별

        Parameters:
        -----------
        text : str
            분석할 텍스트 (disp_nm2)
        """
        # 무게 파서는 단일 타입만 반환
        return None

    def extract_weight_value(self, text):
        """
        텍스트에서 무게 값을 추출

        Parameters:
        -----------
        text : str
            무게 정보가 포함된 텍스트

        Returns:
        --------
        float or None : 추출된 무게 값 (단위 제거된 순수 숫자)
        """
        if not text:
            return None

        # 전처리: 불필요한 공백 정리
        text = ' '.join(text.split())

        # 특수 케이스: ml 단위는 무게가 아니지만 숫자만 추출
        ml_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)\s*ml', text, re.IGNORECASE)
        if ml_match:
            return float(ml_match.group(1).replace(',', ''))

        # 케이스 1: 여러 무게가 있을 때 첫 번째 값 추출
        # 예: "7.6 kg (포장 무게)/ 6.4 kg (제품 무게)" → 7.6
        # 예: "실 중량 : 96kg / 총 중량 : 116kg" → 96
        # 예: "Net: 9.8 kg (21.6 lb) / Gross: 11.9kg ( 26.2 lb)" → 9.8

        # 슬래시(/)로 구분된 경우 첫 번째 부분만 사용
        if '/' in text:
            # 콜론이 있는 경우 (예: "실 중량 : 96kg / 총 중량 : 116kg")
            if ':' in text.split('/')[0]:
                first_part = text.split('/')[0].split(':', 1)[-1].strip()
            else:
                first_part = text.split('/')[0].strip()
        else:
            first_part = text

        # 케이스 2: 여러 제품이 나열된 경우 첫 번째 값
        # 예: "키보드 800 g, 리시버 1.7 g" → 800
        # 예: "키보드 462 g, 마우스 49 g, 리시버 1.8 g" → 462
        # 예: "케이스 : 47 g / 키링 : 43 g" → 47

        # 콤마로 구분된 여러 항목이 있는 경우
        if '키보드' in first_part or '마우스' in first_part or '케이스' in first_part or '스트랩' in first_part:
            # 첫 번째 항목의 무게만 추출
            item_pattern = r'([0-9,]+(?:\.[0-9]+)?)\s*[gGkKㄱ㎏]'
            item_match = re.search(item_pattern, first_part)
            if item_match:
                return float(item_match.group(1).replace(',', ''))

        # 케이스 3: TB 용량과 함께 무게가 표시된 경우
        # 예: "2 TB : 125 g" → 125
        if 'TB' in first_part or 'GB' in first_part:
            colon_split = first_part.split(':')
            if len(colon_split) > 1:
                first_part = colon_split[-1].strip()

        # 케이스 4: Set Only, Set with Supplies 등의 경우
        # 예: "Set Only : 19.3 kg, Set with Supplies : 22.3 kg" → 19.3
        if 'Set Only' in first_part:
            set_match = re.search(r'Set Only\s*:\s*([0-9,]+(?:\.[0-9]+)?)', first_part, re.IGNORECASE)
            if set_match:
                return float(set_match.group(1).replace(',', ''))

        # 케이스 5: Net/Gross 무게가 있는 경우 Net 우선
        if 'Net' in first_part:
            net_match = re.search(r'Net\s*:?\s*([0-9,]+(?:\.[0-9]+)?)', first_part, re.IGNORECASE)
            if net_match:
                return float(net_match.group(1).replace(',', ''))

        # 일반 패턴들
        patterns = [
            # 패턴 1: 숫자 + 단위 (공백 있음)
            # 예: "5.6 g", "34.7 kg", "4.9 ㎏"
            r'([0-9,]+(?:\.[0-9]+)?)\s+(?:g|kg|ㄱ|㎏|Kg|KG)',

            # 패턴 2: 숫자 + 단위 (공백 없음)
            # 예: "5Kg", "7.3Kg", "860g"
            r'([0-9,]+(?:\.[0-9]+)?)\s*(?:g|kg|ㄱ|㎏|Kg|KG)',

            # 패턴 3: 숫자 (단위) 형식
            # 예: "9.79 (kg)"
            r'([0-9,]+(?:\.[0-9]+)?)\s*\([^)]*(?:g|kg|ㄱ|㎏|Kg|KG)[^)]*\)',

            # 패턴 4: 약/Approx. + 숫자
            # 예: "약 5kg", "Approx. 0.25g"
            r'(?:약|Approx\.?)\s*([0-9,]+(?:\.[0-9]+)?)',

            # 패턴 5: 숫자만 있는 경우 (단위 없음)
            # 예: "3.3", "508"
            r'^([0-9,]+(?:\.[0-9]+)?)\s*$',
        ]

        for pattern in patterns:
            match = re.search(pattern, first_part, re.IGNORECASE)
            if match:
                # 콤마 제거하고 float로 변환
                value = match.group(1).replace(',', '')
                try:
                    return float(value)
                except ValueError:
                    continue

        # 특수 케이스: cm가 포함된 경우 (오타로 보임)
        # 예: "0.87 (무게는 제조 환경에 따라 달라질 수 있음) cm" → 0.87
        if 'cm' in text and '무게' in text:
            cm_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)', text)
            if cm_match:
                return float(cm_match.group(1).replace(',', ''))

        # 마지막 시도: 텍스트에서 첫 번째 숫자 추출
        # ※, <br> 등의 특수 문자 이전까지만
        clean_text = re.split(r'[※<]', first_part)[0]
        last_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)', clean_text)
        if last_match:
            try:
                return float(last_match.group(1).replace(',', ''))
            except ValueError:
                pass

        return None

    def parse(self, row):
        """
        무게 파싱 함수

        Parameters:
        -----------
        row : pandas.Series
            파싱할 데이터 행

        Returns:
        --------
        tuple : (parsed_rows, success, needs_check)
        """
        parsed_rows = []
        value = str(row['value'])
        target_disp_nm2 = row.get('target_disp_nm2', '무게')

        # value가 비어있거나 'nan'인 경우
        if not value or value.lower() == 'nan':
            return parsed_rows, False, False

        # 무게 값 추출
        weight_value = self.extract_weight_value(value)

        if weight_value is not None:
            # 파싱 성공
            base_row = row.to_dict()
            parsed_row = {
                **base_row,
                'dimension_type': None,  # 무게는 dimension_type이 없음
                'parsed_value': weight_value,
                'parsed_string_value': str(weight_value),  # 숫자를 문자열로도 저장
                'needs_check': False
            }
            parsed_rows.append(parsed_row)
            return parsed_rows, True, False

        # 파싱 실패
        return parsed_rows, False, False