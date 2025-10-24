"""
기본 파서 추상 클래스
"""
from abc import ABC, abstractmethod

class BaseParser(ABC):
    """
    모든 파서가 상속해야 하는 기본 추상 클래스
    """

    def __init__(self):
        self.goal = self.get_goal()

    @abstractmethod
    def get_goal(self):
        """
        파서가 처리하는 goal 값 반환
        """
        pass

    @abstractmethod
    def parse(self, row):
        """
        데이터 파싱 메서드

        Parameters:
        -----------
        row : pandas.Series
            파싱할 데이터 행

        Returns:
        --------
        tuple : (parsed_rows, success, needs_check)
            - parsed_rows: 파싱된 결과 리스트
            - success: 파싱 성공 여부
            - needs_check: 추가 확인 필요 여부
        """
        pass

    @abstractmethod
    def identify_type(self, text):
        """
        텍스트에서 타입 식별

        Parameters:
        -----------
        text : str
            분석할 텍스트

        Returns:
        --------
        str or None : 식별된 타입
        """
        pass

    def validate_input(self, row):
        """
        입력 데이터 유효성 검증 (선택적 오버라이드)

        Parameters:
        -----------
        row : pandas.Series
            검증할 데이터 행

        Returns:
        --------
        bool : 유효성 여부
        """
        return True

    def post_process(self, parsed_data):
        """
        파싱 후 후처리 (선택적 오버라이드)

        Parameters:
        -----------
        parsed_data : list
            파싱된 데이터 리스트

        Returns:
        --------
        list : 후처리된 데이터
        """
        return parsed_data