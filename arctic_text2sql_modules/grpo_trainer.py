"""
GRPO (Group Relative Policy Optimization) Trainer Module
Arctic-Text2SQL의 핵심 학습 방법론 구현
GPU 없이도 사용 가능한 경량화 버전
"""

from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
import numpy as np
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class GRPOConfig:
    """GRPO 학습 설정"""
    batch_size: int = 256  # Arctic paper: 256
    rollouts_per_prompt: int = 16  # Arctic paper: 16
    update_batch_size: int = 128  # Arctic paper: 128
    kl_penalty: float = 0.001  # β in paper
    clip_ratio: float = 0.2  # ε in paper
    temperature: float = 0.8  # Generation temperature
    max_length: int = 512
    learning_rate: float = 1e-5

@dataclass
class RolloutSample:
    """단일 rollout 샘플"""
    prompt: str
    generated_sql: str
    reward: float
    log_prob: float
    ref_log_prob: float  # Reference model의 log probability

class GRPOTrainer:
    """
    GRPO 기반 SQL 생성 모델 트레이너
    Arctic-Text2SQL-R1의 핵심 학습 방법론 구현

    주요 특징:
    1. Group-based relative optimization
    2. KL divergence penalty로 안정적 학습
    3. Advantage normalization
    """

    def __init__(self, config: GRPOConfig = None):
        """
        Args:
            config: GRPO 설정. None이면 기본값 사용
        """
        self.config = config or GRPOConfig()
        self.training_stats = defaultdict(list)

    def compute_advantages(self, rewards: List[float]) -> List[float]:
        """
        그룹 내 상대적 advantage 계산

        Args:
            rewards: 같은 prompt에 대한 N개 rollout의 보상값

        Returns:
            Normalized advantages
        """
        rewards = np.array(rewards)

        # Baseline: 그룹 평균
        baseline = np.mean(rewards)

        # Advantage 계산
        advantages = rewards - baseline

        # Normalization (안정성을 위해)
        if np.std(advantages) > 0:
            advantages = advantages / (np.std(advantages) + 1e-8)

        return advantages.tolist()

    def compute_grpo_loss(self,
                          rollouts: List[RolloutSample],
                          current_log_probs: List[float]) -> Dict[str, float]:
        """
        GRPO loss 계산

        Args:
            rollouts: 그룹 rollout 샘플들
            current_log_probs: 현재 정책의 log probabilities

        Returns:
            loss 및 통계 정보
        """
        # Advantages 계산
        rewards = [r.reward for r in rollouts]
        advantages = self.compute_advantages(rewards)

        # Likelihood ratios 계산
        ratios = []
        for i, rollout in enumerate(rollouts):
            ratio = np.exp(current_log_probs[i] - rollout.log_prob)
            ratios.append(ratio)

        # GRPO objective 계산
        policy_loss = 0.0
        kl_loss = 0.0

        for i, (rollout, advantage, ratio) in enumerate(zip(rollouts, advantages, ratios)):
            # Clipped objective
            clipped_ratio = np.clip(ratio,
                                   1 - self.config.clip_ratio,
                                   1 + self.config.clip_ratio)

            # Policy gradient loss
            policy_loss -= min(ratio * advantage, clipped_ratio * advantage)

            # KL penalty
            kl_div = rollout.log_prob - rollout.ref_log_prob
            kl_loss += self.config.kl_penalty * kl_div

        # 평균화
        n = len(rollouts)
        policy_loss /= n
        kl_loss /= n

        total_loss = policy_loss + kl_loss

        return {
            "total_loss": total_loss,
            "policy_loss": policy_loss,
            "kl_loss": kl_loss,
            "mean_reward": np.mean(rewards),
            "reward_std": np.std(rewards),
            "mean_advantage": np.mean(advantages)
        }

    def generate_rollouts(self,
                         prompt: str,
                         model_fn: callable,
                         reward_fn: callable) -> List[RolloutSample]:
        """
        하나의 prompt에 대해 N개의 rollout 생성

        Args:
            prompt: 입력 프롬프트
            model_fn: SQL 생성 함수
            reward_fn: 보상 계산 함수

        Returns:
            생성된 rollout 샘플들
        """
        rollouts = []

        for _ in range(self.config.rollouts_per_prompt):
            # SQL 생성 (실제 구현에서는 모델 호출)
            generated_sql, log_prob = model_fn(
                prompt,
                temperature=self.config.temperature,
                max_length=self.config.max_length
            )

            # 보상 계산
            reward = reward_fn(generated_sql, prompt)

            # Reference model log probability (실제로는 별도 모델)
            ref_log_prob = log_prob - np.random.randn() * 0.1  # Simplified

            rollouts.append(RolloutSample(
                prompt=prompt,
                generated_sql=generated_sql,
                reward=reward,
                log_prob=log_prob,
                ref_log_prob=ref_log_prob
            ))

        return rollouts

    def train_step(self,
                   prompts: List[str],
                   model_fn: callable,
                   reward_fn: callable,
                   optimizer_step_fn: callable) -> Dict[str, float]:
        """
        단일 학습 스텝 수행

        Args:
            prompts: 학습할 프롬프트들
            model_fn: SQL 생성 모델
            reward_fn: 보상 계산 함수
            optimizer_step_fn: 최적화 스텝 함수

        Returns:
            학습 통계
        """
        all_stats = defaultdict(list)

        # 각 prompt에 대해 rollout 생성 및 학습
        for prompt in prompts:
            # Rollout 생성
            rollouts = self.generate_rollouts(prompt, model_fn, reward_fn)

            # 현재 정책의 log probabilities 계산 (simplified)
            current_log_probs = [r.log_prob + np.random.randn() * 0.01
                               for r in rollouts]

            # Loss 계산
            stats = self.compute_grpo_loss(rollouts, current_log_probs)

            # 통계 저장
            for key, value in stats.items():
                all_stats[key].append(value)

            # 최적화 스텝 (실제로는 gradient 계산 및 업데이트)
            optimizer_step_fn(stats["total_loss"])

        # 평균 통계 반환
        avg_stats = {key: np.mean(values) for key, values in all_stats.items()}
        self.training_stats["train_steps"].append(avg_stats)

        return avg_stats

    def evaluate(self,
                eval_prompts: List[str],
                model_fn: callable,
                reward_fn: callable) -> Dict[str, float]:
        """
        모델 평가

        Args:
            eval_prompts: 평가용 프롬프트
            model_fn: SQL 생성 모델
            reward_fn: 보상 계산 함수

        Returns:
            평가 통계
        """
        total_rewards = []

        for prompt in eval_prompts:
            # Greedy decoding으로 생성
            generated_sql, _ = model_fn(
                prompt,
                temperature=0.0,  # Greedy
                max_length=self.config.max_length
            )

            # 보상 계산
            reward = reward_fn(generated_sql, prompt)
            total_rewards.append(reward)

        eval_stats = {
            "mean_reward": np.mean(total_rewards),
            "success_rate": np.mean([r > 0.5 for r in total_rewards]),
            "perfect_rate": np.mean([r == 1.0 for r in total_rewards])
        }

        self.training_stats["eval_steps"].append(eval_stats)
        return eval_stats


class MockModelWrapper:
    """
    실제 모델 없이 GRPO trainer를 테스트하기 위한 Mock 클래스
    """

    def generate(self, prompt: str, temperature: float, max_length: int) -> Tuple[str, float]:
        """Mock SQL generation"""
        # 실제로는 LLM 호출
        sql = f"SELECT * FROM table WHERE condition = '{prompt[:20]}'"
        log_prob = -np.random.random() * 10
        return sql, log_prob

    def compute_reward(self, sql: str, prompt: str) -> float:
        """Mock reward computation"""
        # 실제로는 execution 기반 보상
        if "SELECT" in sql:
            return np.random.choice([0.0, 0.1, 1.0], p=[0.2, 0.3, 0.5])
        return 0.0


def demo_grpo_training():
    """GRPO 학습 데모"""
    # 설정 초기화
    config = GRPOConfig()
    trainer = GRPOTrainer(config)

    # Mock 모델
    model = MockModelWrapper()

    # 샘플 프롬프트
    prompts = [
        "Find all customers who ordered products in 2024",
        "Get the top 5 products by sales",
        "List employees with salary above average"
    ]

    print("GRPO Training Demo")
    print("=" * 50)

    # 학습 시뮬레이션
    for epoch in range(3):
        stats = trainer.train_step(
            prompts=prompts,
            model_fn=model.generate,
            reward_fn=model.compute_reward,
            optimizer_step_fn=lambda loss: None  # No-op for demo
        )

        print(f"\nEpoch {epoch + 1}:")
        print(f"  Mean Reward: {stats['mean_reward']:.3f}")
        print(f"  Policy Loss: {stats['policy_loss']:.3f}")
        print(f"  KL Loss: {stats['kl_loss']:.3f}")

    # 평가
    eval_stats = trainer.evaluate(
        eval_prompts=prompts,
        model_fn=model.generate,
        reward_fn=model.compute_reward
    )

    print(f"\nEvaluation:")
    print(f"  Success Rate: {eval_stats['success_rate']:.2%}")
    print(f"  Perfect Rate: {eval_stats['perfect_rate']:.2%}")


if __name__ == "__main__":
    demo_grpo_training()