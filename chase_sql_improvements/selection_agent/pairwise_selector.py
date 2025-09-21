"""
Pairwise Selection Agent for SQL Candidate Selection
CHASE-SQL 논문의 핵심 개선사항: Binary classifier를 통한 pairwise comparison

이 모듈은 CHASE-SQL 논문의 Selection Agent를 구현합니다.
기존의 self-consistency voting 대신 fine-tuned binary classifier를 사용하여
두 SQL 후보를 비교하고 더 나은 것을 선택합니다.

주요 개선점:
1. Pairwise comparison으로 더 정확한 판단
2. Schema union 사용으로 효율성 증대
3. Comparison matrix algorithm으로 최적 후보 선택
4. 71.01% 성능 달성 (self-consistency는 68.84% 한계)
"""

import numpy as np
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
import json
from openai import AzureOpenAI
import os
from dotenv import load_dotenv

# 환경 변수 로드 (Azure OpenAI API 키 등)
load_dotenv()

@dataclass
class SQLCandidate:
    """
    SQL 후보 쿼리를 나타내는 데이터 클래스

    Attributes:
        query: SQL 쿼리 문자열
        generator_type: 생성 방법 ('divide_conquer', 'query_plan', 'synthetic')
        execution_result: 실행 결과 (선택적, 예: "[(42,)]")
        confidence: 신뢰도 점수 (0.0 ~ 1.0)
    """
    query: str
    generator_type: str  # 'divide_conquer', 'query_plan', 'synthetic'
    execution_result: Optional[str] = None
    confidence: float = 0.0

class PairwiseSelector:
    """
    Pairwise Binary Classification을 통한 SQL 선택 엔진

    이 클래스는 CHASE-SQL의 핵심인 Selection Agent를 구현합니다.
    여러 SQL 후보 중에서 최적의 쿼리를 선택하기 위해 pairwise comparison을 수행합니다.

    특징:
    - Fine-tuned model 사용 (실제로는 Gemini-1.5-Flash with LoRA 권장)
    - Comparison matrix 구성으로 모든 쌍 비교
    - Cumulative scoring으로 최종 승자 결정
    - 캐싱을 통한 중복 비교 방지

    알고리즘:
    1. N개 후보에 대해 N(N-1)/2 번의 pairwise comparison 수행
    2. 각 승리마다 1점 획득
    3. 최고 점수 후보 선택
    4. 동점일 경우 generator type 우선순위로 결정
    """

    def __init__(self, model_name: str = "gpt-4.1-nano"):
        """
        PairwiseSelector 초기화

        Args:
            model_name: 사용할 모델 이름 (기본값: "gpt-4.1-nano")
                      실제 프로덕션에서는 fine-tuned Gemini-1.5-Flash 사용 권장
        """
        # Azure OpenAI 클라이언트 초기화
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("ENDPOINT_URL"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2025-01-01-preview"
        )
        self.model_name = model_name

        # 비교 결과 캐시 (동일한 쌍 재비교 방지)
        self.comparison_cache = {}

    def compare_candidates(self,
                         candidate_a: SQLCandidate,
                         candidate_b: SQLCandidate,
                         question: str,
                         schema: str,
                         evidence: str = "") -> str:
        """
        두 SQL 후보를 비교하여 더 나은 것을 선택하는 핵심 메서드

        Binary classification을 통해 두 SQL 중 어느 것이 질문에 더 적합한지 판단합니다.
        Fine-tuned 모델을 사용하여 높은 정확도를 달성합니다.

        Args:
            candidate_a: 첫 번째 SQL 후보
            candidate_b: 두 번째 SQL 후보
            question: 자연어 질문
            schema: 데이터베이스 스키마
            evidence: 추가 컨텍스트나 힌트 (선택적)

        Returns:
            'A' or 'B': 더 나은 후보를 나타내는 문자

        최적화:
        - 실행 결과가 동일한 경우 즉시 반환하여 불필요한 API 호출 방지
        - Schema union을 사용하여 프롬프트 크기 감소
        """
        # 실행 결과가 같으면 첫 번째 선택 (효율성)
        if candidate_a.execution_result == candidate_b.execution_result:
            return 'A'

        # Schema union 구성 (효율성을 위해 관련 테이블만 포함)
        schema_union = self._get_schema_union(candidate_a.query, candidate_b.query, schema)

        prompt = f"""Given the DB info and question, there are two candidate SQL queries.
Compare the two candidates and choose the correct one.

Database Schema:
{schema_union}

Question: {question}
Evidence: {evidence}

Candidate A:
{candidate_a.query}
Execution result: {candidate_a.execution_result or 'No result'}

Candidate B:
{candidate_b.query}
Execution result: {candidate_b.execution_result or 'No result'}

Analyze the differences and select the better query.
Consider:
1. Correctness of JOIN conditions
2. Proper use of WHERE clauses
3. Correct column selection
4. Appropriate aggregation functions

Output only 'A' or 'B'."""

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=10
        )

        result = response.choices[0].message.content.strip().upper()
        return 'A' if result == 'A' else 'B'

    def select_best_candidate(self,
                            candidates: List[SQLCandidate],
                            question: str,
                            schema: str,
                            evidence: str = "") -> Tuple[SQLCandidate, Dict]:
        """
        모든 후보 중에서 최고의 SQL을 선택하는 메인 메서드

        Comparison Matrix Algorithm을 사용하여 모든 후보를 쌍으로 비교하고
        가장 많은 승리를 거둔 후보를 선택합니다.

        알고리즘 상세:
        1. N개 후보에 대해 N×N comparison matrix 생성
        2. 각 (i,j) 쌍에 대해 binary classification 수행
        3. 승리 횟수를 누적하여 점수 계산
        4. 최고 점수 후보 선택

        Args:
            candidates: SQL 후보 리스트
            question: 자연어 질문
            schema: 데이터베이스 스키마
            evidence: 추가 컨텍스트 (선택적)

        Returns:
            Tuple[SQLCandidate, Dict]: 최고의 후보와 통계 정보
                - SQLCandidate: 선택된 최적 SQL
                - Dict: {
                    'comparisons': 총 비교 횟수,
                    'scores': 각 후보의 점수 리스트,
                    'matrix': 비교 결과 매트릭스,
                    'winner_idx': 승자 인덱스
                  }

        시간 복잡도: O(N²) where N = number of candidates
        """
        n = len(candidates)
        # 후보가 1개면 비교 없이 반환
        if n == 1:
            return candidates[0], {"comparisons": 0, "scores": [1]}

        # 점수 배열 초기화 (각 후보의 승리 횟수)
        scores = [0] * n
        # 비교 매트릭스 초기화 (matrix[i][j] = i가 j를 이긴 경우 True)
        comparison_matrix = [[None for _ in range(n)] for _ in range(n)]

        # 모든 쌍에 대해 pairwise comparison 수행
        total_comparisons = 0
        for i in range(n):
            for j in range(i+1, n):  # 상삼각 행렬만 계산 (중복 방지)
                # 두 후보 비교
                winner_idx = self._compare_pair(
                    i, j, candidates, question, schema, evidence
                )
                # 비교 결과 매트릭스 업데이트
                comparison_matrix[i][j] = (winner_idx == i)
                comparison_matrix[j][i] = (winner_idx == j)
                # 승자 점수 증가
                scores[winner_idx] += 1
                total_comparisons += 1

        # 최고 점수 후보 찾기
        best_idx = np.argmax(scores)

        # 동점인 경우 generator type 우선순위로 결정
        if scores.count(max(scores)) > 1:
            best_idx = self._break_tie(candidates, scores)

        return candidates[best_idx], {
            "comparisons": total_comparisons,
            "scores": scores,
            "matrix": comparison_matrix,
            "winner_idx": best_idx
        }

    def _compare_pair(self, i: int, j: int,
                     candidates: List[SQLCandidate],
                     question: str, schema: str, evidence: str) -> int:
        """
        두 후보의 인덱스를 받아 승자 인덱스를 반환하는 헬퍼 메서드

        캐싱을 통해 동일한 쌍의 재비교를 방지하여 효율성을 높입니다.

        Args:
            i: 첫 번째 후보의 인덱스
            j: 두 번째 후보의 인덱스
            candidates: 전체 후보 리스트
            question: 자연어 질문
            schema: 데이터베이스 스키마
            evidence: 추가 컨텍스트

        Returns:
            int: 승자의 인덱스 (i 또는 j)
        """
        # 캐시 키 생성 (쿼리 쌍으로 유니크 키 생성)
        cache_key = (candidates[i].query, candidates[j].query)

        # 캐시에서 확인 (이미 비교한 쌍인 경우)
        if cache_key in self.comparison_cache:
            return i if self.comparison_cache[cache_key] == 'A' else j

        # 실제 비교 수행
        result = self.compare_candidates(
            candidates[i], candidates[j], question, schema, evidence
        )

        # 결과 캐싱
        self.comparison_cache[cache_key] = result
        return i if result == 'A' else j

    def _get_schema_union(self, query_a: str, query_b: str, full_schema: str) -> str:
        """
        두 쿼리에서 사용된 테이블의 스키마만 추출하는 메서드

        프롬프트 크기를 줄이기 위해 실제로 사용되는 테이블의 스키마만 포함합니다.
        이는 토큰 사용량을 줄이고 모델의 집중도를 높입니다.

        Args:
            query_a: 첫 번째 SQL 쿼리
            query_b: 두 번째 SQL 쿼리
            full_schema: 전체 데이터베이스 스키마

        Returns:
            str: 관련 테이블만 포함된 스키마 (현재는 단순화되어 전체 스키마 반환)

        TODO:
            - sqlglot 등의 SQL parser 사용하여 정확한 테이블 추출
            - CREATE TABLE 문에서 해당 테이블만 필터링
        """
        # 간단한 구현: FROM, JOIN 절에서 테이블 추출
        tables = set()
        for query in [query_a, query_b]:
            query_upper = query.upper()
            # FROM 절 파싱
            if 'FROM' in query_upper:
                # 단순화된 파싱 (실제로는 sqlglot 등 사용 권장)
                parts = query_upper.split('FROM')[1].split('WHERE')[0]
                for part in parts.split(','):
                    if 'AS' in part:
                        table = part.split('AS')[0].strip()
                        tables.add(table)

        # 실제로는 full_schema에서 해당 테이블만 추출해야 함
        # 현재는 단순화를 위해 전체 스키마 반환
        return full_schema

    def _break_tie(self, candidates: List[SQLCandidate], scores: List[int]) -> int:
        """
        동점인 후보들 중에서 최종 승자를 결정하는 메서드

        Generator type의 우선순위를 기반으로 동점을 해결합니다.
        일반적으로 더 정교한 방법이 높은 우선순위를 가집니다.

        우선순위:
        1. divide_conquer: 가장 체계적인 분해 방법 (우선순위 3)
        2. query_plan: 실행 계획 기반 방법 (우선순위 2)
        3. synthetic: 예제 기반 방법 (우선순위 1)

        Args:
            candidates: 전체 후보 리스트
            scores: 각 후보의 점수 리스트

        Returns:
            int: 최종 선택된 후보의 인덱스
        """
        max_score = max(scores)
        # 최고 점수를 가진 모든 후보의 인덱스 찾기
        tied_indices = [i for i, s in enumerate(scores) if s == max_score]

        # Generator type별 우선순위 정의
        priority = {'divide_conquer': 3, 'query_plan': 2, 'synthetic': 1}

        # 가장 높은 우선순위를 가진 후보 찾기
        best_idx = tied_indices[0]
        best_priority = priority.get(candidates[best_idx].generator_type, 0)

        for idx in tied_indices[1:]:
            current_priority = priority.get(candidates[idx].generator_type, 0)
            if current_priority > best_priority:
                best_idx = idx
                best_priority = current_priority

        return best_idx

class SelectionAgentTrainer:
    """
    Selection Agent 훈련을 위한 데이터 생성 및 Fine-tuning 관리 클래스

    이 클래스는 pairwise comparison 모델을 훈련하기 위한 데이터를 생성하고
    fine-tuning 프로세스를 관리합니다.

    훈련 프로세스:
    1. 여러 SQL 생성기로부터 후보 생성
    2. 실행 결과로 클러스터링
    3. 정답 클러스터와 오답 클러스터 간 pairwise 훈련 데이터 생성
    4. Fine-tuning (Vertex AI 또는 OpenAI API 사용)

    실제 프로덕션에서는:
    - Gemini-1.5-Flash with LoRA (rank=16) 사용
    - 약 10 epochs 훈련
    - Binary classification objective
    """

    def __init__(self):
        # 생성된 훈련 데이터 저장
        self.training_data = []

    def generate_training_data(self,
                              questions: List[str],
                              gold_sqls: List[str],
                              candidate_sets: List[List[str]]) -> List[Dict]:
        """
        Pairwise comparison을 위한 훈련 데이터 생성

        실행 결과를 기반으로 정답/오답 클러스터를 구분하고,
        정답 클러스터의 SQL과 오답 클러스터의 SQL 간 비교 쌍을 생성합니다.

        Args:
            questions: 자연어 질문 리스트
            gold_sqls: 정답 SQL 리스트
            candidate_sets: 각 질문에 대한 후보 SQL 집합 리스트

        Returns:
            List[Dict]: 훈련 데이터 리스트
                각 딕셔너리 포맷: {
                    'question': str,
                    'candidate_a': str,
                    'candidate_b': str,
                    'label': 'A' or 'B'
                }

        훈련 데이터 생성 전략:
        - 정답 클러스터의 모든 SQL vs 오답 클러스터의 모든 SQL
        - 순서를 랜덤화하여 위치 편향 방지
        - 균형잡힌 A/B 레이블 분포 유지
        """
        training_examples = []

        for q_idx, (question, gold_sql, candidates) in enumerate(
            zip(questions, gold_sqls, candidate_sets)):

            # Execute and cluster candidates
            clusters = self._cluster_by_execution(candidates)

            # Find correct and incorrect clusters
            correct_cluster = None
            for cluster in clusters:
                if gold_sql in cluster:
                    correct_cluster = cluster
                    break

            if correct_cluster and len(clusters) > 1:
                # Create pairwise examples
                for correct_sql in correct_cluster:
                    for other_cluster in clusters:
                        if other_cluster != correct_cluster:
                            for incorrect_sql in other_cluster:
                                # Random order to avoid bias
                                if np.random.random() > 0.5:
                                    example = {
                                        "question": question,
                                        "candidate_a": correct_sql,
                                        "candidate_b": incorrect_sql,
                                        "label": "A"
                                    }
                                else:
                                    example = {
                                        "question": question,
                                        "candidate_a": incorrect_sql,
                                        "candidate_b": correct_sql,
                                        "label": "B"
                                    }
                                training_examples.append(example)

        return training_examples

    def _cluster_by_execution(self, candidates: List[str]) -> List[List[str]]:
        """
        실행 결과로 후보들을 클러스터링
        실제로는 DB에서 실행 필요
        """
        # 단순화: 랜덤 클러스터링
        clusters = []
        for candidate in candidates:
            # 실제로는 실행 결과 비교
            placed = False
            for cluster in clusters:
                if np.random.random() > 0.7:  # 단순화
                    cluster.append(candidate)
                    placed = True
                    break
            if not placed:
                clusters.append([candidate])

        return clusters

    def train_model(self, training_data: List[Dict],
                    model_base: str = "gemini-1.5-flash") -> str:
        """
        Fine-tuning 수행
        실제로는 Vertex AI API 사용
        """
        print(f"Training with {len(training_data)} examples...")
        print(f"Base model: {model_base}")
        print("Parameters: LoRA rank=16, epochs=10")

        # 실제 fine-tuning API 호출 코드
        # model_id = vertex_ai.fine_tune(...)

        return "fine_tuned_model_id"


if __name__ == "__main__":
    # 사용 예제
    selector = PairwiseSelector()

    # 샘플 후보들
    candidates = [
        SQLCandidate(
            query="SELECT COUNT(*) FROM users WHERE age > 20",
            generator_type="divide_conquer",
            execution_result="[(42,)]"
        ),
        SQLCandidate(
            query="SELECT COUNT(*) FROM users WHERE age >= 21",
            generator_type="query_plan",
            execution_result="[(38,)]"
        ),
        SQLCandidate(
            query="SELECT COUNT(id) FROM users WHERE age > 20",
            generator_type="synthetic",
            execution_result="[(42,)]"
        )
    ]

    question = "How many users are over 20 years old?"
    schema = "CREATE TABLE users (id INT, name VARCHAR, age INT)"

    best_candidate, stats = selector.select_best_candidate(
        candidates, question, schema
    )

    print(f"Best SQL: {best_candidate.query}")
    print(f"Generator: {best_candidate.generator_type}")
    print(f"Scores: {stats['scores']}")
    print(f"Total comparisons: {stats['comparisons']}")