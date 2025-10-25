"""
파싱 모듈 초기화
"""

from .size_parser import SizeParser
from .resolution_parser import ResolutionParser
from .memory_parser import MemoryParser
from .weight_parser import WeightParser
from .energy_efficiency_parser import EnergyEfficiencyParser
from .power_consumption_parser import PowerConsumptionParser

# 파서 레지스트리 - goal 값에 따른 파서 매핑
PARSER_REGISTRY = {
    '크기작업': SizeParser,
    '해상도': ResolutionParser,
    '메모리': MemoryParser,
    '무게': WeightParser,
    '소비효율': EnergyEfficiencyParser,
    '소비전력': PowerConsumptionParser,
    # 향후 추가될 파서들
    # '색상작업': ColorParser,
    # '소재작업': MaterialParser,
    # '기능작업': FeatureParser,
    # '성능작업': PerformanceParser,
    # '디자인작업': DesignParser,
}

def get_parser(goal):
    """
    goal 값에 따른 파서 인스턴스 반환

    Parameters:
    -----------
    goal : str
        파싱 목표 (예: '크기작업', '색상작업' 등)

    Returns:
    --------
    Parser instance or None
    """
    parser_class = PARSER_REGISTRY.get(goal)
    if parser_class:
        return parser_class()
    return None

def list_available_parsers():
    """
    사용 가능한 파서 목록 반환
    """
    return list(PARSER_REGISTRY.keys())