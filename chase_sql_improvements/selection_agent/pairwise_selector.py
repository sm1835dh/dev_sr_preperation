"""
Pairwise Selection Agent for SQL Candidate Selection
CHASE-SQL 논문의 핵심 개선사항: Binary classifier를 통한 pairwise comparison
"""

import numpy as np
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
import json
from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class SQLCandidate:
    """SQL 후보 쿼리 데이터 클래스"""
    query: str
    generator_type: str  # 'divide_conquer', 'query_plan', 'synthetic'
    execution_result: Optional[str] = None
    confidence: float = 0.0

class PairwiseSelector:
    """
    Pairwise Binary Classification을 통한 SQL 선택
    - Fine-tuned model 사용 (실제로는 Gemini-1.5-Flash 권장)
    - Comparison matrix 구성
    - Cumulative scoring
    """

    def __init__(self, model_name: str = "gpt-4.1-nano"):
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("ENDPOINT_URL"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2025-01-01-preview"
        )
        self.model_name = model_name
        self.comparison_cache = {}

    def compare_candidates(self,
                         candidate_a: SQLCandidate,
                         candidate_b: SQLCandidate,
                         question: str,
                         schema: str,
                         evidence: str = "") -> str:
        """
        두 SQL 후보를 비교하여 더 나은 것을 선택
        Returns: 'A' or 'B'
        """
        # 실행 결과가 같으면 첫 번째 선택
        if candidate_a.execution_result == candidate_b.execution_result:
            return 'A'

        # Schema union 구성 (효율성을 위해)
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
        모든 후보 중에서 최고의 SQL 선택
        Comparison matrix algorithm 사용
        """
        n = len(candidates)
        if n == 1:
            return candidates[0], {"comparisons": 0, "score": [1]}

        # Initialize scores
        scores = [0] * n
        comparison_matrix = [[None for _ in range(n)] for _ in range(n)]

        # Pairwise comparisons
        total_comparisons = 0
        for i in range(n):
            for j in range(i+1, n):
                winner_idx = self._compare_pair(
                    i, j, candidates, question, schema, evidence
                )
                comparison_matrix[i][j] = (winner_idx == i)
                comparison_matrix[j][i] = (winner_idx == j)
                scores[winner_idx] += 1
                total_comparisons += 1

        # Find candidate with highest score
        best_idx = np.argmax(scores)

        # Break ties by generator priority
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
        """두 후보의 인덱스를 받아 승자 인덱스 반환"""
        cache_key = (candidates[i].query, candidates[j].query)

        # Check cache
        if cache_key in self.comparison_cache:
            return i if self.comparison_cache[cache_key] == 'A' else j

        result = self.compare_candidates(
            candidates[i], candidates[j], question, schema, evidence
        )

        self.comparison_cache[cache_key] = result
        return i if result == 'A' else j

    def _get_schema_union(self, query_a: str, query_b: str, full_schema: str) -> str:
        """
        두 쿼리에서 사용된 테이블의 schema만 추출 (효율성)
        실제 구현에서는 SQL parser 사용 권장
        """
        # 간단한 구현: FROM, JOIN 절에서 테이블 추출
        tables = set()
        for query in [query_a, query_b]:
            query_upper = query.upper()
            # FROM 절 파싱
            if 'FROM' in query_upper:
                # 단순화된 파싱 (실제로는 sqlglot 등 사용)
                parts = query_upper.split('FROM')[1].split('WHERE')[0]
                for part in parts.split(','):
                    if 'AS' in part:
                        table = part.split('AS')[0].strip()
                        tables.add(table)

        # 실제로는 full_schema에서 해당 테이블만 추출
        return full_schema  # 단순화

    def _break_tie(self, candidates: List[SQLCandidate], scores: List[int]) -> int:
        """
        동점일 경우 generator type 우선순위로 결정
        Priority: divide_conquer > query_plan > synthetic
        """
        max_score = max(scores)
        tied_indices = [i for i, s in enumerate(scores) if s == max_score]

        priority = {'divide_conquer': 3, 'query_plan': 2, 'synthetic': 1}

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
    Selection Agent 훈련을 위한 데이터 생성 및 훈련
    실제로는 Vertex AI 또는 fine-tuning API 사용
    """

    def __init__(self):
        self.training_data = []

    def generate_training_data(self,
                              questions: List[str],
                              gold_sqls: List[str],
                              candidate_sets: List[List[str]]) -> List[Dict]:
        """
        훈련 데이터 생성
        Format: (Qu, Ci, Cj, Dij, yij)
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