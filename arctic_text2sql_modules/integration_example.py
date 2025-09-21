"""
Arctic-Text2SQL Integration Example
기존 src/ 코드와 통합하여 사용하는 예제
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import sqlite3
import time

# Arctic 모듈 import
from grpo_trainer import GRPOConfig, GRPOTrainer
from execution_reward import ExecutionRewardCalculator, SimplifiedRewardCalculator
from data_filter import ArcticDataFilter, DataSample, DataQualityChecker
from prompt_optimizer import ArcticPromptOptimizer, PromptConfig, ResponseParser

# Azure OpenAI 설정 (실제 사용시)
try:
    from openai import AzureOpenAI
    from dotenv import load_dotenv
    load_dotenv()
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    print("Azure OpenAI not available. Using mock implementation.")


@dataclass
class IntegrationConfig:
    """통합 설정"""
    # Model
    model_name: str = "gpt-4-turbo"
    temperature: float = 0.0  # Greedy for evaluation
    max_tokens: int = 1000

    # Data
    batch_size: int = 32
    max_execution_time: float = 5.0
    enable_filtering: bool = True

    # Training (if needed)
    enable_training: bool = False
    learning_rate: float = 1e-5

    # Prompt
    prompt_style: str = "arctic_style"  # "arctic_style", "omnisql_style", "direct"


class ArcticText2SQLPipeline:
    """
    Arctic-Text2SQL 전체 파이프라인

    주요 기능:
    1. 데이터 필터링 및 품질 관리
    2. 프롬프트 최적화
    3. SQL 생성 및 실행
    4. 보상 계산 및 평가
    5. GRPO 기반 학습 (선택적)
    """

    def __init__(self, config: IntegrationConfig = None):
        """
        Args:
            config: 통합 설정
        """
        self.config = config or IntegrationConfig()

        # 컴포넌트 초기화
        self._init_components()

        # Azure OpenAI 클라이언트 (사용 가능한 경우)
        self._init_azure_client()

        # 통계
        self.statistics = {
            "total_queries": 0,
            "successful_queries": 0,
            "syntax_valid_queries": 0,
            "failed_queries": 0,
            "avg_execution_time": 0.0
        }

    def _init_components(self):
        """컴포넌트 초기화"""
        # 데이터 필터
        self.data_filter = ArcticDataFilter(
            max_execution_time=self.config.max_execution_time
        )

        # 프롬프트 최적화기
        prompt_config = PromptConfig(
            use_chain_of_thought=True,
            use_think_tags=(self.config.prompt_style == "arctic_style")
        )
        self.prompt_optimizer = ArcticPromptOptimizer(prompt_config)

        # 응답 파서
        self.response_parser = ResponseParser()

        # 보상 계산기
        self.reward_calculator = SimplifiedRewardCalculator()  # CPU 환경용

        # GRPO 트레이너 (학습 모드시)
        if self.config.enable_training:
            grpo_config = GRPOConfig(batch_size=self.config.batch_size)
            self.trainer = GRPOTrainer(grpo_config)

        # 데이터 품질 체커
        self.quality_checker = DataQualityChecker()

    def _init_azure_client(self):
        """Azure OpenAI 클라이언트 초기화"""
        if AZURE_AVAILABLE and os.getenv("AZURE_OPENAI_API_KEY"):
            self.azure_client = AzureOpenAI(
                azure_endpoint=os.getenv("ENDPOINT_URL"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version="2025-01-01-preview"
            )
        else:
            self.azure_client = None

    def process_query(self,
                     question: str,
                     schema: str,
                     evidence: Optional[str] = None,
                     ground_truth_sql: Optional[str] = None) -> Dict[str, Any]:
        """
        단일 쿼리 처리

        Args:
            question: 자연어 질문
            schema: 데이터베이스 스키마
            evidence: 추가 정보
            ground_truth_sql: 정답 SQL (평가용)

        Returns:
            처리 결과
        """
        start_time = time.time()
        self.statistics["total_queries"] += 1

        # 1. 프롬프트 생성
        prompt = self.prompt_optimizer.generate_prompt(
            question=question,
            schema=schema,
            evidence=evidence
        )

        # 2. SQL 생성
        if self.azure_client:
            generated_sql = self._generate_with_azure(prompt)
        else:
            generated_sql = self._generate_mock(prompt)

        # 3. 응답 파싱
        parsed = self.response_parser.parse_response(generated_sql)
        sql_query = parsed.get("sql", generated_sql)

        # 4. 보상 계산
        reward = 0.0
        if ground_truth_sql:
            reward = self.reward_calculator.calculate_reward(
                sql_query,
                ground_truth_sql
            )

            # 통계 업데이트
            if reward == 1.0:
                self.statistics["successful_queries"] += 1
            elif reward == 0.1:
                self.statistics["syntax_valid_queries"] += 1
            else:
                self.statistics["failed_queries"] += 1

        execution_time = time.time() - start_time
        self._update_avg_execution_time(execution_time)

        return {
            "question": question,
            "generated_sql": sql_query,
            "reward": reward,
            "execution_time": execution_time,
            "reasoning": parsed.get("reasoning"),
            "success": reward == 1.0
        }

    def _generate_with_azure(self, prompt: str) -> str:
        """Azure OpenAI를 사용한 SQL 생성"""
        try:
            response = self.azure_client.chat.completions.create(
                model=self.config.model_name,
                messages=[
                    {"role": "system", "content": "You are a SQL expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Azure API error: {e}")
            return self._generate_mock(prompt)

    def _generate_mock(self, prompt: str) -> str:
        """Mock SQL 생성 (테스트용)"""
        # 간단한 패턴 매칭으로 SQL 생성
        if "top" in prompt.lower() or "limit" in prompt.lower():
            sql = "SELECT * FROM table ORDER BY column DESC LIMIT 5"
        elif "count" in prompt.lower():
            sql = "SELECT COUNT(*) FROM table"
        elif "join" in prompt.lower():
            sql = "SELECT t1.*, t2.* FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
        else:
            sql = "SELECT * FROM table WHERE condition = 'value'"

        # Arctic 스타일 응답 시뮬레이션
        if self.config.prompt_style == "arctic_style":
            response = f"""<think>
Analyzing the question and schema...
Need to generate appropriate SQL query.
</think>

<answer>
Generated SQL query based on the question.

```sql
{sql}
```
</answer>"""
        else:
            response = f"```sql\n{sql}\n```"

        return response

    def process_dataset(self,
                       samples: List[DataSample],
                       filter_data: bool = True) -> Dict[str, Any]:
        """
        데이터셋 처리

        Args:
            samples: 처리할 샘플들
            filter_data: 데이터 필터링 여부

        Returns:
            처리 결과 통계
        """
        # 1. 데이터 필터링
        if filter_data and self.config.enable_filtering:
            print("Filtering dataset...")
            samples = self.data_filter.filter_dataset(samples)
            print(f"Filtered to {len(samples)} samples")

        # 2. 품질 분석
        quality_metrics = self.quality_checker.analyze_dataset(samples)
        print(f"Dataset quality: {quality_metrics['sql_diversity']:.2f} diversity")

        # 3. 각 샘플 처리
        results = []
        for i, sample in enumerate(samples):
            if i % 10 == 0:
                print(f"Processing sample {i+1}/{len(samples)}...")

            result = self.process_query(
                question=sample.question,
                schema=sample.schema,
                evidence=sample.evidence,
                ground_truth_sql=sample.sql
            )
            results.append(result)

        # 4. 최종 통계
        success_rate = sum(1 for r in results if r["success"]) / len(results)
        avg_reward = sum(r["reward"] for r in results) / len(results)

        return {
            "total_samples": len(samples),
            "success_rate": success_rate,
            "average_reward": avg_reward,
            "quality_metrics": quality_metrics,
            "detailed_results": results
        }

    def train_with_grpo(self,
                       samples: List[DataSample],
                       epochs: int = 3) -> Dict[str, Any]:
        """
        GRPO를 사용한 학습 (시뮬레이션)

        Args:
            samples: 학습 샘플
            epochs: 학습 에포크

        Returns:
            학습 통계
        """
        if not self.config.enable_training:
            return {"error": "Training not enabled in config"}

        print("Starting GRPO training simulation...")
        training_stats = []

        for epoch in range(epochs):
            print(f"\nEpoch {epoch + 1}/{epochs}")

            # 샘플을 배치로 분할
            for i in range(0, len(samples), self.config.batch_size):
                batch = samples[i:i + self.config.batch_size]
                prompts = [s.question for s in batch]

                # 학습 스텝 (시뮬레이션)
                stats = self.trainer.train_step(
                    prompts=prompts,
                    model_fn=lambda p, t, m: (self._generate_mock(p), -1.0),
                    reward_fn=lambda sql, p: self.reward_calculator.calculate_reward(sql),
                    optimizer_step_fn=lambda loss: None  # No-op
                )

                training_stats.append(stats)

        return {
            "epochs": epochs,
            "final_reward": training_stats[-1]["mean_reward"],
            "training_history": training_stats
        }

    def _update_avg_execution_time(self, new_time: float):
        """평균 실행 시간 업데이트"""
        n = self.statistics["total_queries"]
        old_avg = self.statistics["avg_execution_time"]
        self.statistics["avg_execution_time"] = (old_avg * (n - 1) + new_time) / n

    def get_statistics(self) -> Dict[str, Any]:
        """통계 반환"""
        total = self.statistics["total_queries"]
        if total == 0:
            return self.statistics

        return {
            **self.statistics,
            "success_rate": self.statistics["successful_queries"] / total,
            "syntax_valid_rate": self.statistics["syntax_valid_queries"] / total,
            "failure_rate": self.statistics["failed_queries"] / total
        }


def create_sample_dataset() -> List[DataSample]:
    """샘플 데이터셋 생성"""
    samples = [
        DataSample(
            question="Find all customers who ordered products in 2024",
            sql="SELECT DISTINCT c.* FROM customers c JOIN orders o ON c.id = o.customer_id WHERE YEAR(o.order_date) = 2024",
            schema="""
                CREATE TABLE customers (id INT, name VARCHAR(100));
                CREATE TABLE orders (id INT, customer_id INT, order_date DATE);
            """,
            evidence="Use YEAR function for date filtering"
        ),
        DataSample(
            question="Get the top 5 products by sales amount",
            sql="SELECT p.name, SUM(o.amount) as total FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.id, p.name ORDER BY total DESC LIMIT 5",
            schema="""
                CREATE TABLE products (id INT, name VARCHAR(100));
                CREATE TABLE orders (id INT, product_id INT, amount DECIMAL);
            """
        ),
        DataSample(
            question="Count employees per department",
            sql="SELECT d.name, COUNT(e.id) as emp_count FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.id, d.name",
            schema="""
                CREATE TABLE departments (id INT, name VARCHAR(100));
                CREATE TABLE employees (id INT, name VARCHAR(100), dept_id INT);
            """
        )
    ]
    return samples


def run_integration_demo():
    """통합 데모 실행"""
    print("Arctic-Text2SQL Integration Demo")
    print("=" * 70)

    # 1. 파이프라인 초기화
    config = IntegrationConfig(
        prompt_style="arctic_style",
        enable_filtering=True,
        enable_training=False  # CPU 환경
    )
    pipeline = ArcticText2SQLPipeline(config)

    # 2. 샘플 데이터 준비
    samples = create_sample_dataset()
    print(f"\nPrepared {len(samples)} samples")

    # 3. 단일 쿼리 테스트
    print("\n" + "=" * 70)
    print("Single Query Test")
    print("-" * 70)

    result = pipeline.process_query(
        question="Find customers with orders above average",
        schema="""
            CREATE TABLE customers (id INT, name VARCHAR(100));
            CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL);
        """,
        evidence="Calculate average across all orders first",
        ground_truth_sql="SELECT c.* FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > (SELECT AVG(amount) FROM orders)"
    )

    print(f"Question: {result['question']}")
    print(f"Generated SQL: {result['generated_sql']}")
    print(f"Reward: {result['reward']}")
    print(f"Execution Time: {result['execution_time']:.3f}s")
    print(f"Success: {result['success']}")

    # 4. 데이터셋 처리
    print("\n" + "=" * 70)
    print("Dataset Processing")
    print("-" * 70)

    dataset_results = pipeline.process_dataset(samples, filter_data=False)
    print(f"\nResults:")
    print(f"  Success Rate: {dataset_results['success_rate']:.2%}")
    print(f"  Average Reward: {dataset_results['average_reward']:.3f}")
    print(f"  SQL Diversity: {dataset_results['quality_metrics']['sql_diversity']:.3f}")

    # 5. 최종 통계
    print("\n" + "=" * 70)
    print("Pipeline Statistics")
    print("-" * 70)

    stats = pipeline.get_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.3f}")
        else:
            print(f"  {key}: {value}")


def run_with_existing_src():
    """
    기존 src/ 코드와 통합 예제
    실제 사용시 이 함수를 참고하여 통합
    """
    print("Integration with existing src/ code")
    print("=" * 70)

    # 기존 src/ 경로 추가 (실제 경로로 변경 필요)
    src_path = Path(__file__).parent.parent / "src"
    if src_path.exists():
        sys.path.insert(0, str(src_path))

    # Arctic 파이프라인 초기화
    config = IntegrationConfig(
        prompt_style="omnisql_style",  # OmniSQL 스타일 사용
        enable_filtering=True,
        model_name="gpt-4-turbo"
    )
    pipeline = ArcticText2SQLPipeline(config)

    # 기존 데이터 로드 (예시)
    # from your_existing_module import load_data
    # data = load_data()

    # Arctic 모듈 적용
    # 1. 데이터 필터링
    # filtered_data = pipeline.data_filter.filter_dataset(data)

    # 2. 프롬프트 최적화
    # optimized_prompt = pipeline.prompt_optimizer.generate_prompt(...)

    # 3. 보상 계산
    # reward = pipeline.reward_calculator.calculate_reward(...)

    print("Ready to integrate with existing code!")
    print("Use pipeline components:")
    print("  - pipeline.data_filter: Data filtering")
    print("  - pipeline.prompt_optimizer: Prompt optimization")
    print("  - pipeline.reward_calculator: Reward calculation")
    print("  - pipeline.response_parser: Response parsing")


if __name__ == "__main__":
    # 메인 데모 실행
    run_integration_demo()

    # 기존 코드와 통합 예제 (선택적)
    print("\n" * 2)
    run_with_existing_src()