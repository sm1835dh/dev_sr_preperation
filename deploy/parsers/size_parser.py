"""
크기(Dimension) 파싱 모듈

이 모듈은 제품 스펙 데이터에서 크기 정보(width, height, depth)를 파싱합니다.
다양한 형식의 크기 데이터를 처리할 수 있습니다.
"""
import re
import pandas as pd
from .base_parser import BaseParser


class SizeParser(BaseParser):
    """
    크기 데이터 파싱 클래스
    """

    def get_goal(self):
        """파서가 처리하는 goal 값"""
        return '크기작업'

    def identify_type(self, text):
        """
        텍스트에서 dimension 타입을 식별

        Parameters:
        -----------
        text : str
            분석할 텍스트 (disp_nm2)
        """
        text_lower = text.lower()

        # L(Length) 키워드 - depth로 매핑
        if any(keyword in text_lower for keyword in ['길이', 'l', 'length']):
            return 'depth'
        # Depth 키워드
        elif any(keyword in text_lower for keyword in ['두께', '깊이', 'd']):
            return 'depth'
        # Width 키워드
        elif any(keyword in text_lower for keyword in ['너비', '가로', '폭', 'w']):
            return 'width'
        # Height 키워드
        elif any(keyword in text_lower for keyword in ['세로', '높이', 'h']):
            return 'height'

        return None

    def parse(self, row):
        """
        dimension 파싱 함수

        validation_rule에 따라 데이터를 파싱
        (화이트리스트 체크 제거 - staging 테이블의 is_target으로 대체)
        """
        parsed_rows = []
        value = str(row['value'])
        disp_nm2 = str(row.get('disp_nm2', ''))

        # ============================================
        # 전처리: 제외 조건 체크 및 값 추출
        # ============================================
        # 제외 조건: 각도 조정 관련 텍스트가 포함된 경우
        if any(keyword in value.lower() for keyword in ['각도 조정', '각도조정']):
            return parsed_rows, False, False

        # 복수 개의 값이 있는 경우 첫 번째 값만 추출
        # 예: "TOP/BOTTOM : 1460.0(L) x 24.6(W) x 17.7(H), LEFT/RIGHT : 837.4(L) x 24.6(W) x 17.7(H) mm"
        # 예: "TOP/BOTTOM : 730.8(L), 17.7(W) x 24.6(H) mm, LEFT/RIGHT : 425.3(L), 17.7(W) x 24.6(H) mm"
        # → 첫 번째 세트만 추출

        # 복수 세트 감지: LEFT/RIGHT/TOP/BOTTOM 키워드가 2번 이상 나타나는지 확인
        direction_keywords = ['LEFT', 'RIGHT', 'TOP', 'BOTTOM']
        keyword_count = sum(1 for keyword in direction_keywords if keyword in value.upper())

        if keyword_count >= 2:
            # 복수 세트가 있는 경우
            # 정규식으로 첫 번째 세트 추출: "라벨 : 값들" 패턴에서 다음 라벨 앞까지
            # LEFT/RIGHT/TOP/BOTTOM 키워드 앞에서 분리
            first_set_match = re.search(
                r'(?:TOP|BOTTOM|LEFT|RIGHT)[^:]*:\s*([^:]+?)(?=\s*(?:,\s*)?(?:LEFT|RIGHT|TOP|BOTTOM)|$)',
                value,
                re.IGNORECASE
            )

            if first_set_match:
                # 첫 번째 세트의 값 부분만 추출
                extracted_value = first_set_match.group(1).strip()
                # 끝에 있는 불필요한 콤마 제거
                if extracted_value.endswith(','):
                    extracted_value = extracted_value[:-1].strip()
                value = extracted_value
            else:
                # Fallback: 콜론이 있으면 첫 번째 콜론 다음부터 두 번째 방향 키워드까지
                if ':' in value:
                    # 콜론 뒤의 내용 추출
                    after_colon = value.split(':', 1)[1]
                    # 두 번째 방향 키워드 찾기
                    second_keyword_match = re.search(r'(LEFT|RIGHT|TOP|BOTTOM)', after_colon, re.IGNORECASE)
                    if second_keyword_match:
                        value = after_colon[:second_keyword_match.start()].strip()
                        # 끝에 콤마가 있으면 제거
                        if value.endswith(','):
                            value = value[:-1].strip()
                    else:
                        value = after_colon.strip()
        else:
            # 단일 세트인 경우 - 콜론이 있고 방향 키워드가 있으면 콜론 뒤의 값만 추출
            if ':' in value and any(keyword in value.upper() for keyword in direction_keywords):
                value = value.split(':', 1)[1].strip()

        # 키보드 세트의 경우 첫 번째 제품만 파싱
        if '키보드' in value and ':' in value:
            keyboard_match = re.search(r'키보드\s*:\s*([^가-힣]*?)(?:마우스|리시버|$)', value)
            if keyboard_match:
                value = keyboard_match.group(1).strip()

        # 패턴 0: W숫자 x D숫자 x H숫자 형식 (예: "W269 x D375 x H269 mm") + L 매핑 지원
        wdh_pattern = r'([WwHhDdLl])\s*([0-9,]+(?:\.[0-9]+)?)'
        wdh_matches = re.findall(wdh_pattern, value)

        if len(wdh_matches) >= 2:  # 최소 2개 이상의 dimension이 있는 경우
            base_row = row.to_dict()
            dimension_map = {'w': 'width', 'h': 'height', 'd': 'depth', 'l': 'depth'}  # L을 depth로 매핑

            for dim_letter, num_val in wdh_matches:
                dim_type = dimension_map.get(dim_letter.lower())
                if dim_type:
                    # 콤마 제거 후 숫자 파싱
                    clean_num = num_val.replace(',', '')
                    try:
                        parsed_num = float(clean_num)
                        new_row = base_row.copy()
                        new_row['dimension_type'] = dim_type
                        new_row['parsed_value'] = parsed_num
                        new_row['parsed_symbols'] = 'mm'  # 크기 단위
                        new_row['needs_check'] = False
                        parsed_rows.append(new_row)
                    except ValueError:
                        continue

            if parsed_rows:
                return parsed_rows, True, False

        # 패턴 1: value에 숫자(W), 숫자(H), 숫자(D), 숫자(L)가 명시된 경우
        whd_pattern = r'([0-9,]+(?:\.[0-9]+)?)\s*(?:mm)?\s*\(?\s*([WwHhDdLl])\s*\)?'
        whd_matches = re.findall(whd_pattern, value)

        if len(whd_matches) >= 2:  # 최소 2개 이상의 dimension이 있는 경우
            base_row = row.to_dict()
            dimension_map = {'w': 'width', 'h': 'height', 'd': 'depth', 'l': 'depth'}  # L을 depth로 매핑

            for num_val, dim_letter in whd_matches:
                dim_type = dimension_map.get(dim_letter.lower())
                if dim_type:
                    # 콤마 제거 후 숫자 파싱
                    clean_num = num_val.replace(',', '')
                    try:
                        parsed_num = float(clean_num)
                        new_row = base_row.copy()
                        new_row['dimension_type'] = dim_type
                        new_row['parsed_value'] = parsed_num
                        new_row['parsed_symbols'] = 'mm'  # 크기 단위
                        new_row['needs_check'] = False
                        parsed_rows.append(new_row)
                    except ValueError:
                        continue

            if parsed_rows:
                return parsed_rows, True, False

        # 패턴 2: 한글 키워드로 순서 명시 (우선순위 높음)
        # value 또는 disp_nm2에서 키워드 확인
        # 예: disp_nm2="본체 크기 (너비x두께, mm)", value="7.0 x 2.6"

        combined_text = value + ' ' + disp_nm2  # 두 필드를 합쳐서 키워드 검색

        # 숫자 추출
        nums = re.findall(r'([0-9,]+(?:\.[0-9]+)?)', value)
        base_row = row.to_dict()

        # 키워드 순서 파싱: disp_nm2에서 키워드 순서대로 추출
        # 예: "가로x세로x두께" → ['가로', '세로', '두께']
        keyword_pattern = r'(가로|세로|너비|폭|높이|두께|깊이|길이)'
        keyword_order = re.findall(keyword_pattern, disp_nm2)

        # 키워드가 2개 이상 있고, 숫자도 충분히 있으면 순서대로 매핑
        if len(keyword_order) >= 2 and len(nums) >= len(keyword_order):
            # 한글 키워드 → dimension_type 매핑 (기본값)
            keyword_map = {
                '가로': 'width',
                '너비': 'width',
                '폭': 'width',
                '세로': 'height',   # 기본: 세로=height
                '높이': 'height',
                '두께': 'depth',
                '깊이': 'depth',
                '길이': 'depth',
            }

            # 예외 처리: 특정 조합에서 세로의 의미가 달라짐
            # "가로x높이x세로" 패턴 → 세로를 depth로 해석
            if '높이' in keyword_order and '세로' in keyword_order:
                # 높이와 세로가 함께 있으면, 세로=depth
                keyword_map['세로'] = 'depth'

            try:
                for i, keyword in enumerate(keyword_order):
                    if i < len(nums):
                        dim_type = keyword_map.get(keyword)
                        if dim_type:
                            parsed_rows.append({
                                **base_row,
                                'dimension_type': dim_type,
                                'parsed_value': float(nums[i].replace(',', '')),
                                'parsed_symbols': 'mm',
                                'needs_check': False
                            })

                if parsed_rows:
                    return parsed_rows, True, False
            except ValueError:
                pass

        # 키워드 순서 파싱 실패 시, 기존 로직 사용

        # 2-1. 3개 값: 가로x높이x깊이 (명시적)
        if '가로' in combined_text and '높이' in combined_text and '깊이' in combined_text and len(nums) >= 3:
            try:
                parsed_rows.append({**base_row, 'dimension_type': 'width', 'parsed_value': float(nums[0].replace(',', '')), 'parsed_symbols': 'mm', 'needs_check': False})
                parsed_rows.append({**base_row, 'dimension_type': 'height', 'parsed_value': float(nums[1].replace(',', '')), 'parsed_symbols': 'mm', 'needs_check': False})
                parsed_rows.append({**base_row, 'dimension_type': 'depth', 'parsed_value': float(nums[2].replace(',', '')), 'parsed_symbols': 'mm', 'needs_check': False})
                return parsed_rows, True, False
            except ValueError:
                pass

        # 2-2. 2개 값: 너비x두께, 가로x두께, 폭x두께, 가로x깊이 등
        if ('너비' in combined_text or '가로' in combined_text or '폭' in combined_text) and ('두께' in combined_text or '깊이' in combined_text):
            # 높이 키워드가 없어야 함 (우선순위 구분)
            if '높이' not in combined_text and len(nums) >= 2:
                try:
                    parsed_rows.append({**base_row, 'dimension_type': 'width', 'parsed_value': float(nums[0].replace(',', '')), 'parsed_symbols': 'mm', 'needs_check': False})
                    # 두께/깊이는 depth
                    parsed_rows.append({**base_row, 'dimension_type': 'depth', 'parsed_value': float(nums[1].replace(',', '')), 'parsed_symbols': 'mm', 'needs_check': False})
                    return parsed_rows, True, False
                except ValueError:
                    pass

        # 2-3. 2개 값: 너비x높이, 가로x높이
        if ('너비' in combined_text or '가로' in combined_text or '폭' in combined_text) and '높이' in combined_text and len(nums) >= 2:
            try:
                parsed_rows.append({**base_row, 'dimension_type': 'width', 'parsed_value': float(nums[0].replace(',', '')), 'parsed_symbols': 'mm', 'needs_check': False})
                parsed_rows.append({**base_row, 'dimension_type': 'height', 'parsed_value': float(nums[1].replace(',', '')), 'parsed_symbols': 'mm', 'needs_check': False})
                return parsed_rows, True, False
            except ValueError:
                pass

        # 패턴 3: WxHxD 형식 (x로 구분, 단위 명시 없음) (예: "180 x 70 x 72 mm", "223 x 96.5 x 94 mm")
        wxhxd_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)\s*[xX×]\s*([0-9,]+(?:\.[0-9]+)?)\s*[xX×]\s*([0-9,]+(?:\.[0-9]+)?)', value)
        if wxhxd_match:
            val1, val2, val3 = wxhxd_match.groups()
            base_row = row.to_dict()

            # 기본 가정: 가로 x 높이 x 깊이
            dimensions = [
                ('width', val1),
                ('height', val2),
                ('depth', val3)
            ]

            try:
                for dim_type, val in dimensions:
                    new_row = base_row.copy()
                    new_row['dimension_type'] = dim_type
                    new_row['parsed_value'] = float(val.replace(',', ''))
                    new_row['parsed_symbols'] = 'mm'  # 크기 단위
                    new_row['needs_check'] = True  # 단위가 명확하지 않음
                    parsed_rows.append(new_row)

                return parsed_rows, True, True
            except ValueError:
                pass

        # 패턴 4: WxH 형식 (예: "500x600 mm")
        wxh_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)\s*[xX×]\s*([0-9,]+(?:\.[0-9]+)?)', value)
        if wxh_match:
            val1, val2 = wxh_match.groups()
            base_row = row.to_dict()

            # 기본 가정: 가로 x 높이
            dimensions = [
                ('width', val1),
                ('height', val2)
            ]

            try:
                for dim_type, val in dimensions:
                    new_row = base_row.copy()
                    new_row['dimension_type'] = dim_type
                    new_row['parsed_value'] = float(val.replace(',', ''))
                    new_row['parsed_symbols'] = 'mm'  # 크기 단위
                    new_row['needs_check'] = True  # 단위가 명확하지 않음
                    parsed_rows.append(new_row)

                return parsed_rows, True, True
            except ValueError:
                pass

        # 패턴 5: 단일 값 (disp_nm2에서 dimension 타입 식별)
        single_match = re.search(r'([0-9,]+(?:\.[0-9]+)?)', value)
        if single_match:
            dim_type = self.identify_type(disp_nm2)
            if dim_type:
                try:
                    clean_num = single_match.group(1).replace(',', '')
                    parsed_num = float(clean_num)
                    base_row = row.to_dict()
                    base_row['dimension_type'] = dim_type
                    base_row['parsed_value'] = parsed_num
                    base_row['parsed_symbols'] = 'mm'  # 크기 단위
                    base_row['needs_check'] = False
                    parsed_rows.append(base_row)
                    return parsed_rows, True, False
                except ValueError:
                    pass

        return parsed_rows, False, False