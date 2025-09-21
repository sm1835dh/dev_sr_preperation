"""
Execution-based Reward System
Arctic-Text2SQL의 핵심: 단순하지만 효과적인 보상 시스템
"""

import sqlite3
import re
import time
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
import tempfile
import os
import logging

logger = logging.getLogger(__name__)

@dataclass
class RewardResult:
    """보상 계산 결과"""
    reward: float
    execution_status: str  # 'correct', 'syntax_valid', 'error'
    execution_time: float
    result_rows: Optional[List] = None
    error_message: Optional[str] = None


class ExecutionRewardCalculator:
    """
    Arctic-Text2SQL의 핵심 보상 시스템 구현

    보상 체계:
    - 1.0: 정확한 실행 결과 (ground truth와 일치)
    - 0.1: 문법은 맞지만 결과가 다름
    - 0.0: 문법 오류 또는 실행 불가

    특징:
    - 단순한 보상 체계로 reward hacking 방지
    - 실행 기반 평가로 실용성 확보
    """

    def __init__(self,
                 max_execution_time: float = 5.0,
                 enable_cache: bool = True):
        """
        Args:
            max_execution_time: 최대 실행 시간 (초)
            enable_cache: 실행 결과 캐싱 여부
        """
        self.max_execution_time = max_execution_time
        self.enable_cache = enable_cache
        self.cache = {} if enable_cache else None
        self.stats = {
            "total_evaluations": 0,
            "correct": 0,
            "syntax_valid": 0,
            "errors": 0
        }

    def calculate_reward(self,
                        generated_sql: str,
                        ground_truth_sql: str,
                        database_path: str,
                        schema: Optional[str] = None) -> RewardResult:
        """
        SQL 쿼리의 보상 계산

        Args:
            generated_sql: 생성된 SQL 쿼리
            ground_truth_sql: 정답 SQL 쿼리
            database_path: SQLite 데이터베이스 경로
            schema: 데이터베이스 스키마 (선택적)

        Returns:
            RewardResult 객체
        """
        self.stats["total_evaluations"] += 1

        # 캐시 확인
        cache_key = (generated_sql, ground_truth_sql, database_path)
        if self.enable_cache and cache_key in self.cache:
            return self.cache[cache_key]

        # 1. SQL 정규화
        generated_sql = self._normalize_sql(generated_sql)
        ground_truth_sql = self._normalize_sql(ground_truth_sql)

        # 2. 문법 검증
        if not self._validate_syntax(generated_sql):
            result = RewardResult(
                reward=0.0,
                execution_status='error',
                execution_time=0.0,
                error_message="SQL syntax error"
            )
            self.stats["errors"] += 1
            return self._cache_result(cache_key, result)

        # 3. 실행 및 결과 비교
        try:
            start_time = time.time()

            # Ground truth 실행
            gt_results = self._execute_sql(ground_truth_sql, database_path)

            # Generated SQL 실행
            gen_results = self._execute_sql(generated_sql, database_path)

            execution_time = time.time() - start_time

            # 실행 시간 체크
            if execution_time > self.max_execution_time:
                result = RewardResult(
                    reward=0.0,
                    execution_status='error',
                    execution_time=execution_time,
                    error_message=f"Execution timeout ({execution_time:.2f}s)"
                )
                self.stats["errors"] += 1
                return self._cache_result(cache_key, result)

            # 4. 결과 비교
            if self._compare_results(gen_results, gt_results):
                # 정확히 일치
                result = RewardResult(
                    reward=1.0,
                    execution_status='correct',
                    execution_time=execution_time,
                    result_rows=gen_results
                )
                self.stats["correct"] += 1
            else:
                # 문법은 맞지만 결과가 다름
                result = RewardResult(
                    reward=0.1,
                    execution_status='syntax_valid',
                    execution_time=execution_time,
                    result_rows=gen_results
                )
                self.stats["syntax_valid"] += 1

            return self._cache_result(cache_key, result)

        except Exception as e:
            result = RewardResult(
                reward=0.0,
                execution_status='error',
                execution_time=0.0,
                error_message=str(e)
            )
            self.stats["errors"] += 1
            return self._cache_result(cache_key, result)

    def _normalize_sql(self, sql: str) -> str:
        """
        SQL 쿼리 정규화

        Args:
            sql: 원본 SQL

        Returns:
            정규화된 SQL
        """
        # 주석 제거
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

        # 공백 정규화
        sql = ' '.join(sql.split())

        # 세미콜론 제거
        sql = sql.rstrip(';')

        # 대소문자 정규화 (키워드만)
        keywords = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT',
                   'INNER', 'OUTER', 'ON', 'GROUP', 'BY', 'ORDER',
                   'HAVING', 'LIMIT', 'OFFSET', 'AS', 'AND', 'OR',
                   'IN', 'NOT', 'EXISTS', 'BETWEEN', 'LIKE', 'IS',
                   'NULL', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN']

        for keyword in keywords:
            sql = re.sub(r'\b' + keyword + r'\b', keyword, sql, flags=re.IGNORECASE)

        return sql.strip()

    def _validate_syntax(self, sql: str) -> bool:
        """
        SQL 문법 검증

        Args:
            sql: 검증할 SQL

        Returns:
            문법 유효 여부
        """
        try:
            # SQLite parser로 검증
            with tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False) as f:
                temp_db = f.name

            conn = sqlite3.connect(temp_db)
            cursor = conn.cursor()

            # EXPLAIN으로 문법 체크
            cursor.execute(f"EXPLAIN {sql}")
            conn.close()

            os.unlink(temp_db)
            return True

        except sqlite3.Error:
            return False
        except Exception:
            return False

    def _execute_sql(self,
                    sql: str,
                    database_path: str,
                    timeout: Optional[float] = None) -> List[Tuple]:
        """
        SQL 쿼리 실행

        Args:
            sql: 실행할 SQL
            database_path: 데이터베이스 경로
            timeout: 실행 제한 시간

        Returns:
            쿼리 결과
        """
        timeout = timeout or self.max_execution_time

        conn = sqlite3.connect(database_path, timeout=timeout)
        cursor = conn.cursor()

        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        finally:
            conn.close()

    def _compare_results(self,
                        result1: List[Tuple],
                        result2: List[Tuple]) -> bool:
        """
        두 쿼리 결과 비교

        Args:
            result1: 첫 번째 결과
            result2: 두 번째 결과

        Returns:
            일치 여부
        """
        # BIRD 벤치마크 가이드라인 따름
        # 1. 행 수 비교
        if len(result1) != len(result2):
            return False

        # 2. 정렬 (ORDER BY가 없을 수 있으므로)
        try:
            sorted1 = sorted(result1)
            sorted2 = sorted(result2)
            return sorted1 == sorted2
        except TypeError:
            # 정렬 불가능한 타입이 있을 경우 순서 그대로 비교
            return result1 == result2

    def _cache_result(self,
                     cache_key: Tuple,
                     result: RewardResult) -> RewardResult:
        """결과 캐싱"""
        if self.enable_cache:
            self.cache[cache_key] = result
        return result

    def get_statistics(self) -> Dict[str, Any]:
        """통계 정보 반환"""
        total = self.stats["total_evaluations"]
        if total == 0:
            return self.stats

        return {
            **self.stats,
            "correct_rate": self.stats["correct"] / total,
            "syntax_valid_rate": self.stats["syntax_valid"] / total,
            "error_rate": self.stats["errors"] / total,
            "cache_size": len(self.cache) if self.cache else 0
        }


class SimplifiedRewardCalculator:
    """
    데이터베이스 없이 사용 가능한 간소화된 보상 계산기
    개발 및 테스트용
    """

    def __init__(self):
        self.stats = {
            "total": 0,
            "syntax_checks": 0,
            "execution_checks": 0
        }

    def calculate_reward(self,
                        generated_sql: str,
                        ground_truth_sql: Optional[str] = None) -> float:
        """
        간소화된 보상 계산

        Args:
            generated_sql: 생성된 SQL
            ground_truth_sql: 정답 SQL (선택적)

        Returns:
            보상값 (0.0, 0.1, 1.0)
        """
        self.stats["total"] += 1

        # 1. 기본 문법 체크
        if not self._basic_syntax_check(generated_sql):
            return 0.0

        self.stats["syntax_checks"] += 1

        # 2. Ground truth와 비교 (있는 경우)
        if ground_truth_sql:
            if self._normalize_for_comparison(generated_sql) == \
               self._normalize_for_comparison(ground_truth_sql):
                self.stats["execution_checks"] += 1
                return 1.0

        # 3. 문법만 맞는 경우
        return 0.1

    def _basic_syntax_check(self, sql: str) -> bool:
        """기본적인 SQL 문법 체크"""
        sql = sql.strip().upper()

        # SELECT 문 체크
        if not sql.startswith('SELECT'):
            return False

        # FROM 절 체크
        if 'FROM' not in sql:
            return False

        # 기본 균형 체크
        if sql.count('(') != sql.count(')'):
            return False

        if sql.count("'") % 2 != 0:
            return False

        return True

    def _normalize_for_comparison(self, sql: str) -> str:
        """비교를 위한 SQL 정규화"""
        # 공백 정규화
        sql = ' '.join(sql.split())

        # 대소문자 통일
        sql = sql.upper()

        # 세미콜론 제거
        sql = sql.rstrip(';')

        return sql


def create_test_database() -> str:
    """테스트용 데이터베이스 생성"""
    db_path = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 샘플 테이블 생성
    cursor.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount REAL,
            order_date DATE
        )
    """)

    # 샘플 데이터 삽입
    customers = [
        (1, 'Alice', 30),
        (2, 'Bob', 25),
        (3, 'Charlie', 35)
    ]
    cursor.executemany("INSERT INTO customers VALUES (?, ?, ?)", customers)

    orders = [
        (1, 1, 100.0, '2024-01-01'),
        (2, 1, 200.0, '2024-01-02'),
        (3, 2, 150.0, '2024-01-03')
    ]
    cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", orders)

    conn.commit()
    conn.close()

    return db_path


def demo_reward_calculation():
    """보상 계산 데모"""
    print("Arctic-Text2SQL Reward System Demo")
    print("=" * 50)

    # 1. 간소화된 보상 계산기 테스트
    simple_calc = SimplifiedRewardCalculator()

    test_cases = [
        ("SELECT * FROM users", "SELECT * FROM users", 1.0),
        ("SELECT name FROM users WHERE age > 20", None, 0.1),
        ("SELCT * FRM users", None, 0.0),  # 문법 오류
    ]

    print("\n1. Simplified Reward Calculator:")
    for gen_sql, gt_sql, expected in test_cases:
        reward = simple_calc.calculate_reward(gen_sql, gt_sql)
        print(f"  SQL: {gen_sql[:50]}...")
        print(f"  Expected: {expected}, Got: {reward}")
        print()

    # 2. 실제 실행 기반 보상 계산기 테스트
    print("2. Execution-based Reward Calculator:")

    db_path = create_test_database()
    exec_calc = ExecutionRewardCalculator()

    test_queries = [
        ("SELECT * FROM customers", "SELECT * FROM customers"),
        ("SELECT name FROM customers WHERE age > 25",
         "SELECT name FROM customers WHERE age > 25"),
        ("SELECT COUNT(*) FROM orders", "SELECT COUNT(*) FROM orders"),
    ]

    for gen_sql, gt_sql in test_queries:
        result = exec_calc.calculate_reward(gen_sql, gt_sql, db_path)
        print(f"  Generated: {gen_sql}")
        print(f"  Reward: {result.reward}")
        print(f"  Status: {result.execution_status}")
        print()

    # 통계 출력
    print("Statistics:")
    stats = exec_calc.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Cleanup
    os.unlink(db_path)


if __name__ == "__main__":
    demo_reward_calculation()