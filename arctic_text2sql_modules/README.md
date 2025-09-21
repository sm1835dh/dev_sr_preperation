# Arctic-Text2SQL Modules

Arctic-Text2SQL-R1 논문의 핵심 기술을 모듈화한 구현입니다.

## 📊 주요 성과
- **BIRD Test: 71.83%** (32B 모델)
- 7B 모델이 기존 70B 모델과 동등한 성능
- 단순한 보상 체계로 안정적 학습

## 🚀 핵심 모듈

### 1. GRPO Trainer (`grpo_trainer.py`)
- **Group Relative Policy Optimization** 구현
- CPU 환경에서 실행 가능한 경량화 버전
- 주요 설정:
  - Batch size: 256
  - Rollouts per prompt: 16
  - KL penalty (β): 0.001
  - Clip ratio (ε): 0.2

### 2. Execution Reward (`execution_reward.py`)
- **단순하지만 효과적인 보상 시스템**
- 보상 체계:
  - 1.0: 정확한 실행 결과
  - 0.1: 문법은 맞지만 결과 다름
  - 0.0: 문법 오류 또는 실행 불가
- SQLite 기반 실제 실행 검증

### 3. Data Filter (`data_filter.py`)
- **데이터 품질 관리**
- 필터링 기준:
  - Empty result 제거
  - 5초 초과 실행 쿼리 제거
  - 문법 오류 쿼리 제거
  - Model-based filtering (선택적)

### 4. Prompt Optimizer (`prompt_optimizer.py`)
- **프롬프트 최적화**
- Arctic/OmniSQL 스타일 프롬프트
- Chain-of-Thought with `<think>` tags
- 데이터베이스 스키마 직렬화 최적화

### 5. Integration Example (`integration_example.py`)
- **통합 파이프라인**
- 기존 src/ 코드와 연동 가능
- Azure OpenAI 지원 (선택적)

## 💻 사용 방법

### 기본 사용
```python
from arctic_text2sql_modules.integration_example import ArcticText2SQLPipeline, IntegrationConfig

# 파이프라인 초기화
config = IntegrationConfig(
    prompt_style="arctic_style",
    enable_filtering=True
)
pipeline = ArcticText2SQLPipeline(config)

# SQL 생성
result = pipeline.process_query(
    question="Find top 5 customers by orders",
    schema="CREATE TABLE customers ...",
    evidence="Consider only 2024 orders"
)
```

### 개별 모듈 사용
```python
# 데이터 필터링
from arctic_text2sql_modules.data_filter import ArcticDataFilter
filter = ArcticDataFilter(max_execution_time=5.0)
filtered_samples = filter.filter_dataset(samples)

# 프롬프트 최적화
from arctic_text2sql_modules.prompt_optimizer import ArcticPromptOptimizer
optimizer = ArcticPromptOptimizer()
prompt = optimizer.generate_prompt(question, schema)

# 보상 계산
from arctic_text2sql_modules.execution_reward import SimplifiedRewardCalculator
calculator = SimplifiedRewardCalculator()
reward = calculator.calculate_reward(generated_sql, ground_truth_sql)
```

## 🔧 설치 요구사항

```bash
pip install numpy
pip install openai  # Azure OpenAI 사용시
pip install python-dotenv  # 환경변수 관리
```

## 📈 성능 향상 팁

1. **프롬프트 스타일**: `arctic_style` > `omnisql_style` > `direct`
2. **데이터 필터링**: Empty result 제거로 약 15% 데이터 감소, 품질 향상
3. **Online RL**: Batch RL보다 더 나은 성능
4. **Simple Reward**: 복잡한 보상 함수보다 단순한 execution-based 보상이 효과적

## 🔍 주의사항

- GPU 없는 환경을 위해 실제 학습보다는 추론과 데이터 처리에 중점
- GRPO 학습은 시뮬레이션 모드로 제공
- 실제 데이터베이스 실행은 SQLite 기반

## 📚 참고문헌

Arctic-Text2SQL-R1: Simple Rewards, Strong Reasoning in Text-to-SQL (2025)