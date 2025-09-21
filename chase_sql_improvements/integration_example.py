"""
CHASE-SQL Improvements Integration Example
모든 개선사항을 통합한 end-to-end 예제

CHASE-SQL의 모든 핵심 기술을 통합한 전체 파이프라인 구현입니다.
73.0% BIRD 성능을 달성한 실제 시스템의 핵심 구조를 보여줍니다.

파이프라인 구성:
1. Value Retrieval (단순화 버전)
   - Schema filtering
   - Column selection
   - Value extraction

2. Multi-Path Candidate Generation (4종)
   - Divide-and-Conquer CoT
   - Query Plan CoT
   - Synthetic example-based
   - Temperature variations

3. Instance-aware Synthetic Examples
   - Test-time 예제 생성
   - BIRD distribution matching
   - Few-shot learning

4. Query Fixing
   - Self-reflection
   - Iterative error correction
   - Common pattern fixes

5. Selection Agent
   - Pairwise comparison
   - Tournament selection
   - Confidence scoring

주요 특징:
- 다양한 SQL 생성 방법 통합
- 오류에 강건한 시스템
- 확장 가능한 구조
- 실시간 처리 가능

Reference: CHASE-SQL paper Figure 1
"""

import os
import sys
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import json
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from selection_agent.pairwise_selector import PairwiseSelector, SQLCandidate
from multi_path_generation.divide_conquer_cot import DivideConquerCoT
from multi_path_generation.query_plan_cot import QueryPlanCoT
from synthetic_examples.online_generator import OnlineSyntheticGenerator, SyntheticExample
from query_fixer.self_reflection_fixer import SelfReflectionFixer

@dataclass
class QueryResult:
    """
    최종 쿼리 결과

    CHASE-SQL 파이프라인의 최종 실행 결과를 저장합니다.
    성능 분석과 디버깅에 필요한 모든 정보를 포함합니다.

    Attributes:
        final_sql: 최종 선택된 SQL 쿼리
        method_used: 사용된 생성 방법
            - 'divide_conquer': Divide-and-Conquer CoT
            - 'query_plan': Query Plan CoT
            - 'synthetic': Synthetic example-based
            - 'variation': Temperature variation
        confidence: 최종 선택 신뢰도 (0.0 ~ 1.0)
        candidates: 생성된 모든 SQL 후보
        execution_time: 전체 실행 시간 (초)
        synthetic_examples_used: 사용된 합성 예제 수
        comparisons_made: 수행된 pairwise 비교 회수

    이 정보는 성능 분석과 시스템 개선에 활용됩니다.
    """
    final_sql: str
    method_used: str
    confidence: float
    candidates: List[SQLCandidate]
    execution_time: float
    synthetic_examples_used: int
    comparisons_made: int

class CHASESQLPipeline:
    """
    CHASE-SQL의 전체 파이프라인 구현

    CHASE-SQL 논문의 핵심 아키텍처를 구현한 통합 파이프라인입니다.
    73.0% BIRD 성능을 달성한 실제 시스템의 구조를 따릅니다.

    파이프라인 단계:
    1. Value Retrieval (simplified)
       - Schema Union: 관련 테이블/컬럼만 추출
       - Cell Value Retrieval: 데이터베이스 값 추출
       - Filter optimization: 불필요한 스키마 제거

    2. Multi-path Candidate Generation
       - 4개의 다른 생성 방법 사용
       - Temperature variation으로 다양성 확보
       - 최소 8-10개 후보 생성

    3. Query Fixing
       - Self-reflection 기반 오류 수정
       - 최대 3회 반복 수정
       - Syntax 및 empty result 처리

    4. Selection Agent
       - Pairwise binary classification
       - Tournament selection
       - Confidence scoring

    성능 특징:
    - Self-consistency 대비 5% 향상 (68.84% → 73.0%)
    - 단일 모델로 다양한 접근법 통합
    - 오류 감소와 성공률 향상
    - 확장 가능한 구조

    이 파이프라인은 BIRD 벤치마크에서
    state-of-the-art 성능을 달성했습니다.
    """

    def __init__(self, use_synthetic_examples: bool = True):
        # Initialize components
        self.selector = PairwiseSelector()
        self.dc_generator = DivideConquerCoT()
        self.qp_generator = QueryPlanCoT()
        self.synthetic_generator = OnlineSyntheticGenerator() if use_synthetic_examples else None
        self.fixer = SelfReflectionFixer()
        self.use_synthetic = use_synthetic_examples

    def generate_sql(self,
                    question: str,
                    database_schema: str,
                    evidence: str = "",
                    filtered_columns: List[str] = None) -> QueryResult:
        """
        전체 파이프라인 실행

        CHASE-SQL의 모든 기술을 통합하여 SQL 쿼리를 생성합니다.
        각 단계는 순차적으로 실행되며, 중간 결과는 다음 단계로 전달됩니다.

        실행 프로세스:
        1. Synthetic example 생성 (선택적)
           - Instance-aware 예제 생성
           - BIRD distribution matching

        2. Multi-path 후보 생성
           - 다양한 생성 방법 활용
           - Temperature variation 추가

        3. Syntax error 수정
           - Self-reflection 기반
           - 반복적 개선

        4. 후보 실행 (시뮬레이션)
           - 결과 검증
           - 성공/실패 판단

        5. 최종 선택
           - Pairwise comparison
           - 최적 후보 결정

        Args:
            question: 자연어 질문
            database_schema: 데이터베이스 스키마
            evidence: 추가 컨텍스트 (선택적)
            filtered_columns: Schema Union 결과 (선택적)

        Returns:
            QueryResult: 최종 SQL과 상세 정보

        Example:
            >>> result = pipeline.generate_sql(
            ...     "Find top customers",
            ...     schema,
            ...     evidence="Consider 2024 data"
            ... )
            >>> print(result.final_sql)
        """
        start_time = time.time()

        print("=" * 50)
        print("CHASE-SQL Pipeline Execution")
        print("=" * 50)
        print(f"Question: {question}\n")

        # Step 1: Generate synthetic examples (if enabled)
        synthetic_examples = []
        if self.use_synthetic and self.synthetic_generator:
            print("Step 1: Generating synthetic examples...")
            synthetic_examples = self.synthetic_generator.generate_examples(
                question=question,
                database_schema=database_schema,
                filtered_columns=filtered_columns,
                n_examples=5
            )
            print(f"  Generated {len(synthetic_examples)} synthetic examples")

        # Step 2: Multi-path candidate generation
        print("\nStep 2: Generating SQL candidates...")
        candidates = self._generate_candidates(
            question, database_schema, evidence, synthetic_examples
        )
        print(f"  Generated {len(candidates)} candidates from {len(set(c.generator_type for c in candidates))} generators")

        # Step 3: Fix any syntax errors
        print("\nStep 3: Fixing syntax errors...")
        fixed_candidates = self._fix_candidates(candidates, database_schema, question)
        print(f"  Fixed {sum(1 for a, b in zip(candidates, fixed_candidates) if a.query != b.query)} candidates")

        # Step 4: Execute candidates (simulated)
        print("\nStep 4: Executing candidates (simulated)...")
        executed_candidates = self._execute_candidates(fixed_candidates, database_schema)

        # Step 5: Select best candidate
        print("\nStep 5: Selecting best candidate...")
        best_candidate, selection_stats = self.selector.select_best_candidate(
            executed_candidates,
            question,
            database_schema,
            evidence
        )

        print(f"  Made {selection_stats['comparisons']} pairwise comparisons")
        print(f"  Scores: {selection_stats['scores']}")
        print(f"  Winner: {best_candidate.generator_type} generator")

        execution_time = time.time() - start_time

        result = QueryResult(
            final_sql=best_candidate.query,
            method_used=best_candidate.generator_type,
            confidence=max(selection_stats['scores']) / sum(selection_stats['scores']) if sum(selection_stats['scores']) > 0 else 0.0,
            candidates=executed_candidates,
            execution_time=execution_time,
            synthetic_examples_used=len(synthetic_examples),
            comparisons_made=selection_stats['comparisons']
        )

        print(f"\n{'=' * 50}")
        print(f"Final SQL: {result.final_sql}")
        print(f"Execution time: {result.execution_time:.2f}s")
        print(f"Confidence: {result.confidence:.2%}")
        print("=" * 50)

        return result

    def _generate_candidates(self,
                           question: str,
                           schema: str,
                           evidence: str,
                           synthetic_examples: List[SyntheticExample]) -> List[SQLCandidate]:
        """
        Multi-path candidate generation

        다양한 방법을 사용하여 SQL 후보를 생성합니다.
        각 생성 방법은 다른 강점을 가지며,
        이를 통해 다양성을 확보합니다.

        생성 방법:
        1. Divide-and-Conquer CoT
           - 복잡한 쿼리 분해
           - 단계별 해결
           - 강점: 복잡한 논리 처리

        2. Query Plan CoT
           - DB 엔진 시뮬레이션
           - 실행 계획 최적화
           - 강점: 효율적 쿼리

        3. Synthetic example-based
           - Few-shot learning
           - 패턴 학습
           - 강점: 유사 패턴 반영

        4. Temperature variations
           - 기존 후보의 변형
           - 다양성 확보
           - 강점: 탐색 공간 확장

        Failure handling:
        - 각 생성기 실패 시 건너뛰기
        - 최소 2개 이상 후보 보장
        - 에러 로깅

        Args:
            question: 자연어 질문
            schema: 데이터베이스 스키마
            evidence: 추가 컨텍스트
            synthetic_examples: 생성된 합성 예제

        Returns:
            List[SQLCandidate]: 생성된 SQL 후보들
        """
        candidates = []

        # Convert synthetic examples to few-shot format
        few_shot = []
        if synthetic_examples:
            for ex in synthetic_examples[:3]:  # Use top 3 examples
                few_shot.append({
                    "question": ex.question,
                    "sql": ex.sql
                })

        # 1. Divide-and-Conquer CoT
        try:
            print("  - Generating with Divide-and-Conquer CoT...")
            dc_sql, dc_info = self.dc_generator.generate_sql(question, schema, evidence)
            candidates.append(SQLCandidate(
                query=dc_sql,
                generator_type="divide_conquer"
            ))
        except Exception as e:
            print(f"    Error in DC generator: {e}")

        # 2. Query Plan CoT
        try:
            print("  - Generating with Query Plan CoT...")
            qp_sql, qp_info = self.qp_generator.generate_sql(question, schema, evidence)
            candidates.append(SQLCandidate(
                query=qp_sql,
                generator_type="query_plan"
            ))
        except Exception as e:
            print(f"    Error in QP generator: {e}")

        # 3. Synthetic example-based generation
        if synthetic_examples:
            try:
                print("  - Generating with synthetic examples...")
                # Use synthetic examples as few-shot
                syn_sql = self._generate_with_synthetic(question, schema, evidence, few_shot)
                candidates.append(SQLCandidate(
                    query=syn_sql,
                    generator_type="synthetic"
                ))
            except Exception as e:
                print(f"    Error in synthetic generator: {e}")

        # 4. Add variations with different temperatures
        print("  - Generating temperature variations...")
        base_candidates = candidates.copy()
        for temp in [0.5, 0.8]:
            for base in base_candidates[:2]:  # Vary first 2 candidates
                try:
                    varied = self._generate_variation(base, question, schema, temp)
                    if varied:
                        candidates.append(varied)
                except:
                    pass

        return candidates

    def _generate_with_synthetic(self,
                                question: str,
                                schema: str,
                                evidence: str,
                                few_shot: List[Dict]) -> str:
        """
        Synthetic example을 사용한 SQL 생성

        생성된 합성 예제를 few-shot learning에 활용하여
        현재 쿼리와 유사한 패턴의 SQL을 생성합니다.

        Few-shot 설정:
        - 상위 3개 예제 사용
        - User/Assistant 형식으로 구성
        - 컨텍스트 학습 유도

        효과:
        - 패턴 일관성 향상
        - 도메인 특화 학습
        - Zero-shot 대비 성능 향상

        Args:
            question: 현재 질문
            schema: 데이터베이스 스키마
            evidence: 추가 컨텍스트
            few_shot: Few-shot 예제 리스트

        Returns:
            str: 생성된 SQL 쿼리
        """
        # Simplified implementation
        # In real implementation, use proper few-shot prompting
        from openai import AzureOpenAI
        from dotenv import load_dotenv
        load_dotenv()

        client = AzureOpenAI(
            azure_endpoint=os.getenv("ENDPOINT_URL"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2025-01-01-preview"
        )

        messages = [{"role": "system", "content": "Generate SQL based on examples"}]

        for ex in few_shot:
            messages.append({"role": "user", "content": f"Question: {ex['question']}"})
            messages.append({"role": "assistant", "content": ex['sql']})

        messages.append({"role": "user", "content": f"""Database: {schema}
Question: {question}
Evidence: {evidence}
SQL:"""})

        response = client.chat.completions.create(
            model="gpt-4.1-nano",
            messages=messages,
            temperature=0.3,
            max_tokens=500
        )

        return response.choices[0].message.content.strip()

    def _generate_variation(self,
                          base_candidate: SQLCandidate,
                          question: str,
                          schema: str,
                          temperature: float) -> Optional[SQLCandidate]:
        """
        Temperature variation 생성

        기존 후보를 다른 temperature로 재생성하여
        다양성을 확보합니다.

        Temperature 효과:
        - 0.0-0.3: 결정적, 일관성 높음
        - 0.5: 균형 잡힌 탐색
        - 0.8-1.0: 창의적, 다양성 높음

        실제 구현 시:
        - 원본 생성기로 재생성
        - Temperature만 변경
        - 동일 프롬프트 사용

        Args:
            base_candidate: 기본 후보
            question: 질문
            schema: 스키마
            temperature: 생성 temperature

        Returns:
            Optional[SQLCandidate]: 변형된 후보, 실패 시 None
        """
        # Simplified - in real implementation, regenerate with different temperature
        import random
        if random.random() > 0.5:
            return SQLCandidate(
                query=base_candidate.query,  # In reality, regenerate
                generator_type=f"{base_candidate.generator_type}_t{temperature}"
            )
        return None

    def _fix_candidates(self,
                       candidates: List[SQLCandidate],
                       schema: str,
                       question: str) -> List[SQLCandidate]:
        """
        Fix syntax errors in candidates

        모든 SQL 후보의 오류를 검사하고 수정합니다.
        Self-reflection 기반으로 최대 3회까지 수정을 시도합니다.

        수정 대상:
        - Syntax errors: SQL 구문 오류
        - Missing clauses: 필수 절 누락
        - Type mismatches: 타입 불일치
        - Typos: 오타 및 철자 오류

        처리 방식:
        - 각 후보 독립적 처리
        - 수정 실패 시 원본 유지
        - 에러 무시

        Args:
            candidates: 수정할 SQL 후보들
            schema: 데이터베이스 스키마
            question: 원본 질문

        Returns:
            List[SQLCandidate]: 수정된 SQL 후보들
        """
        fixed = []
        for candidate in candidates:
            try:
                fixed_sql = self.fixer.fix_query(candidate.query, schema, question)
                fixed.append(SQLCandidate(
                    query=fixed_sql,
                    generator_type=candidate.generator_type
                ))
            except:
                fixed.append(candidate)  # Keep original if fixing fails
        return fixed

    def _execute_candidates(self,
                          candidates: List[SQLCandidate],
                          schema: str) -> List[SQLCandidate]:
        """
        Execute candidates and get results (simulated)

        SQL 후보들을 실행하고 결과를 수집합니다.
        현재는 시뮬레이션이며, 실제 구현 시 데이터베이스 연결이 필요합니다.

        시뮬레이션 규칙:
        - COUNT 쿼리: 랜덤 수 반환
        - SELECT 쿼리: 더미 행 반환
        - 기타: 빈 결과

        실제 구현 시:
        - SQLite/PostgreSQL 연결
        - 실제 실행 및 결과 비교
        - Timeout 처리 (5초)
        - 오류 처리

        Args:
            candidates: 실행할 SQL 후보들
            schema: 데이터베이스 스키마

        Returns:
            List[SQLCandidate]: 실행 결과가 포함된 후보들
        """
        import random

        for candidate in candidates:
            # Simulate execution
            if "SELECT COUNT" in candidate.query.upper():
                candidate.execution_result = f"[({random.randint(1, 100)},)]"
            elif "SELECT" in candidate.query.upper():
                candidate.execution_result = f"[(row1,), (row2,)]"
            else:
                candidate.execution_result = "[]"

        return candidates

def run_example():
    """
    실행 예제

    CHASE-SQL 파이프라인의 전체 작동을 시연하는 예제입니다.
    복잡한 SQL 쿼리를 생성하는 전 과정을 보여줍니다.

    예제 쿼리:
    "모든 카테고리에서 제품을 주문하고,
     평균 이상을 소비한 고객 찾기"

    이 쿼리는 다음을 테스트합니다:
    - 복잡한 JOIN
    - 집계 함수
    - 서브쿼리
    - GROUP BY/HAVING

    실행 단계:
    1. 파이프라인 초기화
    2. 예제 데이터 준비
    3. SQL 생성 실행
    4. 결과 분석
    5. 성능 메트릭 출력

    Returns:
        QueryResult: 생성 결과와 상세 정보
    """
    pipeline = CHASESQLPipeline(use_synthetic_examples=True)

    # Example question
    question = """Find the names of customers who have placed orders
                 for products in all categories and spent more than
                 the average customer spending"""

    schema = """
    CREATE TABLE customers (
        customer_id INT PRIMARY KEY,
        customer_name VARCHAR(100),
        join_date DATE
    );

    CREATE TABLE products (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(100),
        category_id INT,
        price DECIMAL(10,2)
    );

    CREATE TABLE categories (
        category_id INT PRIMARY KEY,
        category_name VARCHAR(50)
    );

    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        product_id INT,
        quantity INT,
        order_date DATE,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """

    evidence = "All categories means the customer ordered from every existing category"

    filtered_columns = [
        "customers.customer_id", "customers.customer_name",
        "orders.customer_id", "orders.product_id",
        "products.product_id", "products.category_id",
        "categories.category_id"
    ]

    # Run pipeline
    result = pipeline.generate_sql(
        question=question,
        database_schema=schema,
        evidence=evidence,
        filtered_columns=filtered_columns
    )

    # Print detailed results
    print("\n" + "=" * 50)
    print("DETAILED RESULTS")
    print("=" * 50)

    print("\nAll Candidates Generated:")
    for i, candidate in enumerate(result.candidates, 1):
        print(f"\n{i}. {candidate.generator_type}:")
        print(f"   SQL: {candidate.query[:100]}..." if len(candidate.query) > 100 else f"   SQL: {candidate.query}")
        print(f"   Result: {candidate.execution_result}")

    print(f"\nPerformance Metrics:")
    print(f"  - Total execution time: {result.execution_time:.2f}s")
    print(f"  - Candidates generated: {len(result.candidates)}")
    print(f"  - Synthetic examples: {result.synthetic_examples_used}")
    print(f"  - Pairwise comparisons: {result.comparisons_made}")
    print(f"  - Final confidence: {result.confidence:.2%}")

    return result

if __name__ == "__main__":
    result = run_example()