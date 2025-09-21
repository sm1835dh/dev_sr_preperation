"""
Data Filtering and Quality Control Module
Arctic-Text2SQL의 데이터 품질 관리 시스템
"""

import sqlite3
import time
import re
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
import logging
from pathlib import Path
import json
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class DataSample:
    """데이터 샘플"""
    question: str
    sql: str
    schema: str
    database_path: Optional[str] = None
    evidence: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class FilterResult:
    """필터링 결과"""
    passed: bool
    reason: Optional[str] = None
    execution_time: Optional[float] = None
    result_count: Optional[int] = None


class ArcticDataFilter:
    """
    Arctic-Text2SQL의 데이터 필터링 시스템

    주요 필터링 기준:
    1. Empty result 제거 (BIRD 1,400개, SPIDER 1,700개 제거)
    2. 실행 시간 5초 초과 쿼리 제거
    3. 문법 오류 쿼리 제거
    4. Model-based filtering (선택적)
    """

    def __init__(self,
                 max_execution_time: float = 5.0,
                 min_sql_length: int = 30,
                 max_sql_length: int = 2000,
                 enable_model_filter: bool = False):
        """
        Args:
            max_execution_time: 최대 실행 시간 (초)
            min_sql_length: 최소 SQL 길이
            max_sql_length: 최대 SQL 길이
            enable_model_filter: 모델 기반 필터링 활성화
        """
        self.max_execution_time = max_execution_time
        self.min_sql_length = min_sql_length
        self.max_sql_length = max_sql_length
        self.enable_model_filter = enable_model_filter

        self.statistics = {
            "total_samples": 0,
            "passed_samples": 0,
            "filtered_empty_results": 0,
            "filtered_timeout": 0,
            "filtered_syntax_error": 0,
            "filtered_length": 0,
            "filtered_model_based": 0
        }

    def filter_dataset(self,
                       samples: List[DataSample],
                       parallel: bool = False) -> List[DataSample]:
        """
        데이터셋 필터링

        Args:
            samples: 필터링할 샘플들
            parallel: 병렬 처리 여부 (CPU 환경에서는 False 권장)

        Returns:
            필터링된 샘플들
        """
        filtered_samples = []
        self.statistics["total_samples"] = len(samples)

        for sample in samples:
            filter_result = self._filter_sample(sample)

            if filter_result.passed:
                filtered_samples.append(sample)
                self.statistics["passed_samples"] += 1
            else:
                # 필터링 이유별 통계
                self._update_filter_statistics(filter_result.reason)

            # 진행상황 로깅
            if len(filtered_samples) % 100 == 0:
                logger.info(f"Filtered {len(filtered_samples)}/{len(samples)} samples")

        # 최종 통계 출력
        self._print_statistics()

        return filtered_samples

    def _filter_sample(self, sample: DataSample) -> FilterResult:
        """
        단일 샘플 필터링

        Args:
            sample: 필터링할 샘플

        Returns:
            필터링 결과
        """
        # 1. SQL 길이 체크
        if not self._check_sql_length(sample.sql):
            return FilterResult(passed=False, reason="length")

        # 2. 문법 체크
        if not self._check_syntax(sample.sql):
            return FilterResult(passed=False, reason="syntax_error")

        # 3. 실행 체크 (데이터베이스가 있는 경우)
        if sample.database_path:
            exec_result = self._check_execution(sample)
            if not exec_result.passed:
                return exec_result

        # 4. 모델 기반 필터링 (활성화된 경우)
        if self.enable_model_filter:
            if not self._model_based_filter(sample):
                return FilterResult(passed=False, reason="model_based")

        return FilterResult(passed=True)

    def _check_sql_length(self, sql: str) -> bool:
        """SQL 길이 체크"""
        sql_length = len(sql.strip())
        return self.min_sql_length <= sql_length <= self.max_sql_length

    def _check_syntax(self, sql: str) -> bool:
        """SQL 문법 체크"""
        try:
            # 기본 문법 패턴 체크
            sql_upper = sql.upper().strip()

            # SELECT 문 체크
            if not sql_upper.startswith('SELECT'):
                return False

            # FROM 절 체크
            if 'FROM' not in sql_upper:
                return False

            # 괄호 균형 체크
            if sql.count('(') != sql.count(')'):
                return False

            # 따옴표 균형 체크
            if sql.count("'") % 2 != 0:
                return False

            return True
        except Exception:
            return False

    def _check_execution(self, sample: DataSample) -> FilterResult:
        """
        SQL 실행 체크

        Args:
            sample: 체크할 샘플

        Returns:
            필터링 결과
        """
        try:
            start_time = time.time()

            # 데이터베이스 연결
            conn = sqlite3.connect(sample.database_path,
                                  timeout=self.max_execution_time)
            cursor = conn.cursor()

            # 스키마 설정 (필요한 경우)
            if sample.schema:
                self._setup_schema(cursor, sample.schema)

            # SQL 실행
            cursor.execute(sample.sql)
            results = cursor.fetchall()

            execution_time = time.time() - start_time
            conn.close()

            # 실행 시간 체크
            if execution_time > self.max_execution_time:
                return FilterResult(
                    passed=False,
                    reason="timeout",
                    execution_time=execution_time
                )

            # Empty result 체크
            if len(results) == 0:
                return FilterResult(
                    passed=False,
                    reason="empty_results",
                    result_count=0
                )

            return FilterResult(
                passed=True,
                execution_time=execution_time,
                result_count=len(results)
            )

        except sqlite3.Error as e:
            return FilterResult(
                passed=False,
                reason="syntax_error"
            )
        except Exception as e:
            logger.error(f"Execution error: {e}")
            return FilterResult(
                passed=False,
                reason="execution_error"
            )

    def _setup_schema(self, cursor: sqlite3.Cursor, schema: str):
        """데이터베이스 스키마 설정"""
        # CREATE TABLE 문 실행
        schema_statements = schema.split(';')
        for statement in schema_statements:
            statement = statement.strip()
            if statement:
                cursor.execute(statement)

    def _model_based_filter(self, sample: DataSample) -> bool:
        """
        모델 기반 필터링
        Arctic 논문: 10개 생성 중 1개 이상 정답인 샘플만 유지
        """
        # 실제 구현에서는 모델을 사용하여 여러 SQL 생성 후 검증
        # 여기서는 간단한 휴리스틱 사용
        sql_complexity = self._calculate_sql_complexity(sample.sql)
        return sql_complexity >= 2  # 복잡도 2 이상만 유지

    def _calculate_sql_complexity(self, sql: str) -> int:
        """SQL 복잡도 계산"""
        complexity = 0
        sql_upper = sql.upper()

        # 복잡도 요소들
        if 'JOIN' in sql_upper:
            complexity += sql_upper.count('JOIN')
        if 'WHERE' in sql_upper:
            complexity += 1
        if 'GROUP BY' in sql_upper:
            complexity += 2
        if 'HAVING' in sql_upper:
            complexity += 2
        if 'SUBQUERY' in sql_upper or '(SELECT' in sql_upper:
            complexity += 3
        if 'CASE' in sql_upper:
            complexity += 2

        return complexity

    def _update_filter_statistics(self, reason: str):
        """필터링 통계 업데이트"""
        if reason == "empty_results":
            self.statistics["filtered_empty_results"] += 1
        elif reason == "timeout":
            self.statistics["filtered_timeout"] += 1
        elif reason == "syntax_error":
            self.statistics["filtered_syntax_error"] += 1
        elif reason == "length":
            self.statistics["filtered_length"] += 1
        elif reason == "model_based":
            self.statistics["filtered_model_based"] += 1

    def _print_statistics(self):
        """통계 출력"""
        total = self.statistics["total_samples"]
        passed = self.statistics["passed_samples"]

        print("\nData Filtering Statistics:")
        print("=" * 50)
        print(f"Total samples: {total}")
        print(f"Passed samples: {passed} ({passed/total*100:.1f}%)")
        print(f"Filtered by empty results: {self.statistics['filtered_empty_results']}")
        print(f"Filtered by timeout: {self.statistics['filtered_timeout']}")
        print(f"Filtered by syntax error: {self.statistics['filtered_syntax_error']}")
        print(f"Filtered by length: {self.statistics['filtered_length']}")
        if self.enable_model_filter:
            print(f"Filtered by model: {self.statistics['filtered_model_based']}")


class SyntheticDataFilter:
    """
    합성 데이터 필터링 (Gretel-Synth 등)
    """

    def __init__(self, min_complexity: int = 2):
        """
        Args:
            min_complexity: 최소 SQL 복잡도
        """
        self.min_complexity = min_complexity
        self.processed_count = 0

    def filter_synthetic_data(self,
                             samples: List[DataSample],
                             add_distractor_tables: bool = True) -> List[DataSample]:
        """
        합성 데이터 필터링 및 증강

        Args:
            samples: 필터링할 샘플들
            add_distractor_tables: Distractor 테이블 추가 여부

        Returns:
            필터링된 샘플들
        """
        filtered_samples = []

        for sample in samples:
            # SQL 복잡도 체크
            if self._check_complexity(sample.sql):
                # Distractor 테이블 추가
                if add_distractor_tables:
                    sample = self._add_distractor_tables(sample)

                filtered_samples.append(sample)
                self.processed_count += 1

        logger.info(f"Filtered synthetic data: {len(filtered_samples)}/{len(samples)}")
        return filtered_samples

    def _check_complexity(self, sql: str) -> bool:
        """복잡도 체크"""
        complexity = 0
        sql_upper = sql.upper()

        # 복잡도 계산
        if 'JOIN' in sql_upper:
            complexity += 2
        if 'GROUP BY' in sql_upper:
            complexity += 1
        if 'HAVING' in sql_upper:
            complexity += 1
        if '(SELECT' in sql_upper:
            complexity += 2

        return complexity >= self.min_complexity

    def _add_distractor_tables(self, sample: DataSample) -> DataSample:
        """
        Distractor 테이블 추가
        실제 쿼리에 사용되지 않는 관련 테이블 추가로 난이도 증가
        """
        # 실제 구현에서는 도메인별 관련 테이블 풀에서 선택
        distractor_schemas = [
            "CREATE TABLE distractor1 (id INT, data TEXT);",
            "CREATE TABLE distractor2 (id INT, value REAL);"
        ]

        # 스키마에 distractor 추가
        sample.schema += "\n" + "\n".join(distractor_schemas)
        sample.metadata["has_distractors"] = True

        return sample


class DataQualityChecker:
    """
    데이터 품질 체크 및 통계
    """

    def __init__(self):
        self.quality_metrics = {
            "sql_diversity": 0.0,
            "schema_coverage": 0.0,
            "complexity_distribution": {},
            "error_patterns": {}
        }

    def analyze_dataset(self, samples: List[DataSample]) -> Dict[str, Any]:
        """
        데이터셋 품질 분석

        Args:
            samples: 분석할 샘플들

        Returns:
            품질 메트릭
        """
        # SQL 다양성 계산
        self.quality_metrics["sql_diversity"] = self._calculate_diversity(samples)

        # 스키마 커버리지 계산
        self.quality_metrics["schema_coverage"] = self._calculate_schema_coverage(samples)

        # 복잡도 분포 계산
        self.quality_metrics["complexity_distribution"] = self._analyze_complexity_distribution(samples)

        # 에러 패턴 분석
        self.quality_metrics["error_patterns"] = self._analyze_error_patterns(samples)

        return self.quality_metrics

    def _calculate_diversity(self, samples: List[DataSample]) -> float:
        """SQL 패턴 다양성 계산"""
        sql_patterns = set()

        for sample in samples:
            # SQL 패턴 추출 (간단한 해시 사용)
            pattern = self._extract_sql_pattern(sample.sql)
            sql_patterns.add(pattern)

        # 다양성 = unique patterns / total samples
        return len(sql_patterns) / len(samples) if samples else 0.0

    def _extract_sql_pattern(self, sql: str) -> str:
        """SQL 패턴 추출"""
        # 리터럴 값 제거하고 구조만 추출
        pattern = re.sub(r"'[^']*'", "'?'", sql)
        pattern = re.sub(r"\d+", "?", pattern)
        return hashlib.md5(pattern.encode()).hexdigest()[:8]

    def _calculate_schema_coverage(self, samples: List[DataSample]) -> float:
        """스키마 테이블/컬럼 사용률 계산"""
        used_tables = set()
        total_tables = set()

        for sample in samples:
            # 사용된 테이블 추출
            tables_in_sql = re.findall(r'FROM\s+(\w+)', sample.sql, re.IGNORECASE)
            tables_in_sql.extend(re.findall(r'JOIN\s+(\w+)', sample.sql, re.IGNORECASE))
            used_tables.update(tables_in_sql)

            # 전체 테이블 추출
            tables_in_schema = re.findall(r'CREATE\s+TABLE\s+(\w+)', sample.schema, re.IGNORECASE)
            total_tables.update(tables_in_schema)

        return len(used_tables) / len(total_tables) if total_tables else 0.0

    def _analyze_complexity_distribution(self, samples: List[DataSample]) -> Dict[str, int]:
        """복잡도 분포 분석"""
        distribution = {
            "simple": 0,
            "medium": 0,
            "complex": 0
        }

        for sample in samples:
            complexity = self._calculate_complexity(sample.sql)
            if complexity <= 2:
                distribution["simple"] += 1
            elif complexity <= 5:
                distribution["medium"] += 1
            else:
                distribution["complex"] += 1

        return distribution

    def _calculate_complexity(self, sql: str) -> int:
        """SQL 복잡도 계산"""
        complexity = 0
        sql_upper = sql.upper()

        if 'JOIN' in sql_upper:
            complexity += sql_upper.count('JOIN') * 2
        if 'WHERE' in sql_upper:
            complexity += 1
        if 'GROUP BY' in sql_upper:
            complexity += 2
        if 'HAVING' in sql_upper:
            complexity += 2
        if '(SELECT' in sql_upper:
            complexity += 3

        return complexity

    def _analyze_error_patterns(self, samples: List[DataSample]) -> Dict[str, int]:
        """에러 패턴 분석"""
        patterns = {
            "missing_from": 0,
            "unbalanced_parentheses": 0,
            "missing_join_condition": 0,
            "ambiguous_column": 0
        }

        for sample in samples:
            sql = sample.sql.upper()

            if 'SELECT' in sql and 'FROM' not in sql:
                patterns["missing_from"] += 1
            if sql.count('(') != sql.count(')'):
                patterns["unbalanced_parentheses"] += 1
            if 'JOIN' in sql and 'ON' not in sql:
                patterns["missing_join_condition"] += 1

        return patterns


def demo_data_filtering():
    """데이터 필터링 데모"""
    print("Arctic-Text2SQL Data Filtering Demo")
    print("=" * 50)

    # 샘플 데이터 생성
    samples = [
        DataSample(
            question="Find all customers",
            sql="SELECT * FROM customers",
            schema="CREATE TABLE customers (id INT, name TEXT);"
        ),
        DataSample(
            question="Get customer count",
            sql="SELECT COUNT(*) FROM customers WHERE age > 20",
            schema="CREATE TABLE customers (id INT, name TEXT, age INT);"
        ),
        DataSample(
            question="Complex query",
            sql="""
                SELECT c.name, COUNT(o.id) as order_count
                FROM customers c
                LEFT JOIN orders o ON c.id = o.customer_id
                GROUP BY c.id, c.name
                HAVING COUNT(o.id) > 5
            """,
            schema="""
                CREATE TABLE customers (id INT, name TEXT);
                CREATE TABLE orders (id INT, customer_id INT);
            """
        ),
        DataSample(
            question="Syntax error query",
            sql="SELCT * FRM users",  # 문법 오류
            schema="CREATE TABLE users (id INT);"
        )
    ]

    # 1. 기본 필터링
    print("\n1. Basic Filtering:")
    filter = ArcticDataFilter()
    filtered = filter.filter_dataset(samples)
    print(f"Filtered: {len(filtered)}/{len(samples)} samples passed")

    # 2. 품질 분석
    print("\n2. Quality Analysis:")
    checker = DataQualityChecker()
    metrics = checker.analyze_dataset(filtered)
    for key, value in metrics.items():
        print(f"  {key}: {value}")

    # 3. 합성 데이터 필터링
    print("\n3. Synthetic Data Filtering:")
    syn_filter = SyntheticDataFilter()
    syn_filtered = syn_filter.filter_synthetic_data(samples[:2])
    print(f"Synthetic filtered: {len(syn_filtered)} samples")


if __name__ == "__main__":
    demo_data_filtering()