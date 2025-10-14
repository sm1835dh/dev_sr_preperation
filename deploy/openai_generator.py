import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from openai import AzureOpenAI
import pandas as pd

load_dotenv()

class OpenAIMetadataGenerator:
    """Azure OpenAI를 활용한 메타데이터 생성"""

    def __init__(self):
        self.client = self._initialize_client()
        self.deployment_name = os.getenv('DEPLOYMENT_NAME')

    def _initialize_client(self) -> Optional[AzureOpenAI]:
        """Azure OpenAI 클라이언트 초기화"""
        try:
            endpoint = os.getenv('ENDPOINT_URL')
            api_key = os.getenv('AZURE_OPENAI_API_KEY')
            api_version = os.getenv('AZURE_API_VERSION', '2024-02-01')

            if not endpoint:
                print("❌ ENDPOINT_URL 환경 변수가 설정되지 않았습니다.")
                return None

            if not api_key:
                print("❌ AZURE_OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
                return None

            print(f"📋 Azure OpenAI 설정:")
            print(f"  - Endpoint: {endpoint[:50]}...")
            print(f"  - API Version: {api_version}")
            print(f"  - Deployment: {os.getenv('DEPLOYMENT_NAME')}")

            client = AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version=api_version,
            )
            print(f"✅ Azure OpenAI 클라이언트 생성 성공")
            return client

        except Exception as e:
            print(f"❌ Azure OpenAI 클라이언트 생성 실패: {e}")
            return None

    def generate_column_description(self, column_info: Dict[str, Any], table_context: str) -> Optional[str]:
        """컬럼 설명 생성"""
        if not self.client:
            print("⚠️ OpenAI 클라이언트가 초기화되지 않았습니다.")
            return None

        # 데이터 타입 확인
        data_type = column_info.get('data_type', '')
        is_numeric = any(dtype in data_type.lower() for dtype in ['int', 'float', 'numeric', 'decimal', 'double'])
        is_string = 'object' in data_type.lower() or 'varchar' in data_type.lower() or 'text' in data_type.lower()
        is_date = 'date' in data_type.lower() or 'time' in data_type.lower()

        # 프롬프트 구성
        prompt = self._build_prompt(column_info, table_context, is_numeric, is_string, is_date)

        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 데이터베이스 전문가입니다. 컬럼의 비즈니스 의미를 명확하게 설명해주세요."
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=3000,
                temperature=1,
            )

            if response and response.choices and len(response.choices) > 0:
                content = response.choices[0].message.content
                if content:
                    print(f"✅ 컬럼 '{column_info.get('column_name')}' 설명 생성 성공")
                    return content
                else:
                    return self._get_default_description(column_info)
            else:
                return self._get_default_description(column_info)

        except Exception as e:
            print(f"❌ OpenAI 설명 생성 실패: {e}")
            return self._get_default_description(column_info)

    def _build_prompt(self, column_info: Dict, table_context: str,
                      is_numeric: bool, is_string: bool, is_date: bool) -> str:
        """프롬프트 구성"""
        base_info = f"""
        테이블명: {table_context}
        컬럼 정보:
        - 컬럼명: {column_info.get('column_name')}
        - 데이터 타입: {column_info.get('data_type')}
        - NULL 비율: {column_info.get('null_ratio', 'N/A')}
        - 고유값 개수: {column_info.get('unique_count', 'N/A')}"""

        column_comment = column_info.get('column_comment')
        if column_comment and column_comment != 'None' and pd.notna(column_comment):
            base_info += f"\n    - 기존 설명: {column_comment}"

        type_specific_info = self._get_type_specific_info(column_info, is_numeric, is_string, is_date)

        if column_comment and column_comment != 'None' and pd.notna(column_comment):
            prompt = f"""{base_info}{type_specific_info}

            기존 설명을 참고하여 다음 형식으로 개선된 설명을 생성해주세요:
            1. 짧은 설명 (한 줄, 20자 이내)
            2. 상세 설명 (2-3줄, 비즈니스 의미 포함)
            3. 데이터 특성 (NULL 허용 여부, 값 범위 등)"""
        else:
            prompt = f"""{base_info}{type_specific_info}

            다음 형식으로 응답해주세요:
            1. 짧은 설명 (한 줄, 20자 이내)
            2. 상세 설명 (2-3줄, 비즈니스 의미 포함)
            3. 데이터 특성 (NULL 허용 여부, 값 범위 등)"""

        return prompt

    def _get_type_specific_info(self, column_info: Dict, is_numeric: bool,
                                 is_string: bool, is_date: bool) -> str:
        """데이터 타입별 추가 정보 구성"""
        type_specific_info = ""

        if is_numeric:
            min_val = column_info.get('min')
            max_val = column_info.get('max')
            mean_val = column_info.get('mean')
            median_val = column_info.get('median')
            std_val = column_info.get('std')

            if min_val is not None or max_val is not None or mean_val is not None:
                type_specific_info += "\n    - 통계 정보:"
                if min_val is not None:
                    type_specific_info += f"\n      - 최소값: {min_val}"
                if max_val is not None:
                    type_specific_info += f"\n      - 최대값: {max_val}"
                if mean_val is not None:
                    type_specific_info += f"\n      - 평균: {mean_val:.2f}"
                if median_val is not None:
                    type_specific_info += f"\n      - 중앙값: {median_val:.2f}"
                if std_val is not None:
                    type_specific_info += f"\n      - 표준편차: {std_val:.2f}"

        elif is_string:
            min_length = column_info.get('min_length')
            max_length = column_info.get('max_length')
            avg_length = column_info.get('avg_length')
            most_common = column_info.get('most_common', {})

            if min_length is not None or max_length is not None:
                type_specific_info += "\n    - 문자열 길이 정보:"
                if min_length is not None:
                    type_specific_info += f"\n      - 최소 길이: {min_length}"
                if max_length is not None:
                    type_specific_info += f"\n      - 최대 길이: {max_length}"
                if avg_length is not None:
                    type_specific_info += f"\n      - 평균 길이: {avg_length:.1f}"

            if most_common and len(most_common) > 0:
                type_specific_info += "\n    - 가장 빈번한 값:"
                for value, count in list(most_common.items())[:5]:
                    display_value = value if len(str(value)) <= 100 else str(value)[:100] + "..."
                    type_specific_info += f"\n      - '{display_value}': {count}개"

        elif is_date:
            min_date = column_info.get('min_date')
            max_date = column_info.get('max_date')
            date_range = column_info.get('date_range')

            if min_date or max_date:
                type_specific_info += "\n    - 날짜 범위:"
                if min_date:
                    type_specific_info += f"\n      - 최소 날짜: {min_date}"
                if max_date:
                    type_specific_info += f"\n      - 최대 날짜: {max_date}"
                if date_range:
                    type_specific_info += f"\n      - 날짜 범위: {date_range}"

        return type_specific_info

    def _get_default_description(self, column_info: Dict) -> str:
        """기본 설명 반환"""
        column_comment = column_info.get('column_comment')
        if column_comment and pd.notna(column_comment):
            return f"1. {column_comment}\n2. {column_comment} 정보를 저장하는 컬럼입니다.\n3. NULL 허용, 문자열 타입"
        else:
            return f"1. {column_info.get('column_name')} 정보\n2. {column_info.get('column_name')} 관련 데이터를 저장합니다.\n3. {column_info.get('null_ratio')} NULL 비율"

    def generate_table_description(self, table_name: str, table_stats: Dict = None,
                                    column_stats: pd.DataFrame = None) -> Optional[str]:
        """테이블 전체 설명 생성"""
        if not self.client:
            return None

        key_columns = []
        if column_stats is not None and len(column_stats) > 0:
            key_columns = column_stats.head(10)['column_name'].tolist()

        prompt = f"""
        다음 데이터베이스 테이블에 대한 종합적인 설명을 영어로 작성해주세요.
        테이블은 삼성닷컴의 제품 정보를 담고 있습니다.

        테이블명: {table_name}

        테이블 통계:
        - 전체 행 수: {table_stats.get('total_rows', 'N/A') if table_stats else 'N/A'}
        - 테이블 크기: {table_stats.get('table_size', 'N/A') if table_stats else 'N/A'}
        - 전체 컬럼 수: {len(column_stats) if column_stats is not None else 'N/A'}

        주요 컬럼: {', '.join(key_columns)}

        다음 내용을 포함하여 설명해주세요:
        1. 테이블의 주요 목적과 역할 (2-3줄)
        2. 저장되는 데이터의 비즈니스 의미
        3. 다른 테이블과의 잠재적 연관 관계
        4. 데이터 활용 사례 (예: 리포트, 분석, API 등)
        """

        try:
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 데이터 아키텍트입니다. 테이블의 비즈니스 목적과 활용 방안을 명확하게 설명해주세요."
                    },
                    {"role": "user", "content": prompt}
                ],
                max_completion_tokens=500
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"❌ 테이블 설명 생성 실패: {e}")
            return None