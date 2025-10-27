#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
메모리 용량 파서
메모리 스펙에서 GB 단위 용량을 추출
"""

import re
import pandas as pd
from .base_parser import BaseParser


class MemoryParser(BaseParser):
    """
    메모리 용량 파싱 클래스

    파싱 예시:
    - "8 GB LPDDR4x Memory" → 8
    - "16 GB LPDDR5X Memory (On Board 16 GB)" → 16
    - "32 GB LPDDR5X Memory (On Package)" → 32
    - "6 GB" → 6
    - "1.5 GB" → 1.5
    """

    def __init__(self):
        super().__init__()
        # 메모리 용량 추출 패턴들
        self.patterns = [
            # 패턴 1: "숫자 GB" (소수점 포함)
            r'(\d+(?:\.\d+)?)\s*GB',
            # 패턴 2: "숫자GB" (공백 없이)
            r'(\d+(?:\.\d+)?)GB',
            # 패턴 3: 단독 숫자 (GB가 뒤에 오지 않는 경우)
            r'^(\d+(?:\.\d+)?)\s*$',
        ]

    def get_goal(self):
        """파서가 처리하는 goal 값 반환"""
        return '메모리'

    def identify_type(self, text):
        """메모리 타입 식별 - 메모리의 경우 None 반환"""
        return None

    def parse(self, row):
        """
        메모리 데이터 파싱

        Parameters:
        -----------
        row : pandas.Series
            파싱할 데이터 행

        Returns:
        --------
        tuple : (parsed_rows, success, needs_check)
        """
        try:
            # 입력 데이터 확인
            if not self.validate_input(row):
                return [], False, False

            value = str(row.get('value', '')).strip()
            if not value:
                return [], False, False

            # 메모리 용량 추출
            memory_size = self.extract_memory_size(value)

            if memory_size is None:
                return [], False, False

            # 결과 행 생성
            parsed_row = {
                'mdl_code': row.get('mdl_code'),
                'goods_nm': row.get('goods_nm'),
                'disp_lv1': row.get('disp_lv1'),
                'disp_lv2': row.get('disp_lv2'),
                'disp_lv3': row.get('disp_lv3'),
                'category_lv1': row.get('category_lv1'),
                'category_lv2': row.get('category_lv2'),
                'category_lv3': row.get('category_lv3'),
                'disp_nm1': row.get('disp_nm1'),
                'disp_nm2': row.get('disp_nm2'),
                'value': value,
                'is_numeric': row.get('is_numeric'),
                'symbols': row.get('symbols'),
                'new_value': row.get('new_value'),
                'target_disp_nm2': row.get('target_disp_nm2', '메모리'),
                'dimension_type': None,  # width/height/depth만 허용하므로 None
                'parsed_value': memory_size,
                'parsed_string_value': None,
                'parsed_symbols': 'GB',  # 메모리 단위를 parsed_symbols에 저장
                'needs_check': False,
                'goal': '메모리'
            }

            return [parsed_row], True, False

        except Exception as e:
            print(f"파싱 오류: {e}")
            return [], False, True

    def extract_memory_size(self, text):
        """
        텍스트에서 메모리 용량(GB) 추출

        Parameters:
        -----------
        text : str
            메모리 사양 텍스트

        Returns:
        --------
        float or None : 추출된 메모리 용량 (GB 단위)
        """
        # 텍스트 정규화
        text = text.strip()

        # 각 패턴으로 시도
        for pattern in self.patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                try:
                    # 첫 번째 매치된 숫자 반환
                    # 여러 개가 매치되면 첫 번째 것을 사용 (예: "On Board 16 GB"에서 16)
                    return float(matches[0])
                except ValueError:
                    continue

        return None

    def validate_input(self, row):
        """
        입력 데이터 유효성 검증

        Parameters:
        -----------
        row : pandas.Series
            검증할 데이터 행

        Returns:
        --------
        bool : 유효성 여부
        """
        # value 필드가 있는지 확인
        if 'value' not in row or pd.isna(row['value']):
            return False

        value = str(row['value']).strip()

        # 빈 값인지 확인
        if not value:
            return False

        # 메모리와 관련 없는 값 필터링 (필요시 추가)
        exclude_keywords = ['MHz', 'RPM', 'inch', 'mm', 'kg']
        for keyword in exclude_keywords:
            if keyword in value:
                return False

        return True