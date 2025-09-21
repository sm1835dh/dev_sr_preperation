"""
CHASE-SQL Improvements Integration Example
모든 개선사항을 통합한 end-to-end 예제
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
    """최종 쿼리 결과"""
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
    1. Value Retrieval (simplified)
    2. Multi-path Candidate Generation
    3. Query Fixing
    4. Selection Agent
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
        """Multi-path candidate generation"""
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
        """Synthetic example을 사용한 SQL 생성"""
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
        """Temperature variation 생성"""
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
        """Fix syntax errors in candidates"""
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
        """Execute candidates and get results (simulated)"""
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
    """실행 예제"""
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