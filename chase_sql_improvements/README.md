# CHASE-SQL Improvements Sample Code

이 폴더는 CHASE-SQL 논문의 핵심 개선사항들을 샘플 코드로 구현한 것입니다.
기존 src/ 폴더와 독립적으로 검토 후 통합 가능하도록 구성했습니다.

## 폴더 구조

```
chase_sql_improvements/
├── README.md                          # 이 문서
├── selection_agent/                   # Selection Agent (핵심 개선)
│   ├── pairwise_selector.py          # Pairwise binary classifier
│   ├── training_data_generator.py    # Training data 생성
│   └── selection_algorithm.py        # Comparison matrix algorithm
├── multi_path_generation/             # 다중 경로 SQL 생성
│   ├── divide_conquer_cot.py        # Divide-and-Conquer CoT
│   ├── query_plan_cot.py            # Query Plan CoT
│   └── generator_ensemble.py         # 통합 generator
├── synthetic_examples/                # Instance-aware 예제 생성
│   ├── online_generator.py          # Test-time 예제 생성
│   └── feature_matcher.py           # SQL feature distribution matching
├── query_fixer/                      # Enhanced Query Fixer
│   └── self_reflection_fixer.py     # Self-reflection 기반 수정
└── integration_example.py            # 전체 통합 예제
```

## 주요 개선사항

### 1. Selection Agent (예상 +10-15%)
- Pairwise binary classification
- Fine-tuned model 사용
- Comparison matrix algorithm

### 2. Multi-path Generation (예상 +5-10%)
- 3가지 독립적인 generation 방법
- Diversity와 quality 균형

### 3. Instance-aware Examples (예상 +3-5%)
- Test-time synthetic generation
- Schema-specific examples

## 사용 방법

각 모듈은 독립적으로 테스트 가능하며, `integration_example.py`에서
전체 통합 예제를 확인할 수 있습니다.

```python
# 예제 실행
python integration_example.py
```

## 기존 시스템과의 통합

각 모듈은 기존 src/ 폴더의 구조와 호환되도록 설계되었습니다.
필요한 모듈만 선택적으로 통합 가능합니다.