# Text2SQL Evaluation API

Text2SQL 모델의 예측 결과를 평가하는 FastAPI 서비스입니다.

## 설치 및 실행

### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 환경 변수 설정
`.env` 파일에 PostgreSQL 연결 정보를 설정합니다:
```
PG_HOST=your_host
PG_PORT=5432
PG_DATABASE=your_database
PG_USER=your_user
PG_PASSWORD=your_password
```

### 3. API 서버 실행
```bash
python text2sql_api.py
```

서버는 기본적으로 `http://localhost:8000`에서 실행됩니다.

## API 엔드포인트

### 1. 단일 쿼리 평가
**POST** `/evaluate/single`

단일 쿼리에 대한 평가를 수행합니다.

**Request Body:**
```json
{
  "query_number": 1,
  "predicted_product_ids": ["P001", "P002", "P003"]
}
```

**Response:**
```json
{
  "status": "success",
  "query_metrics": {
    "query_number": 1,
    "category": "sales",
    "instruction": "최근 3개월간 가장 많이 팔린 상품...",
    "TP": 2,
    "FP": 1,
    "FN": 2,
    "Precision": 0.6667,
    "Recall": 0.5,
    "F1_Score": 0.5714,
    "Exact_Match": 0,
    "Jaccard_Similarity": 0.4,
    "Predicted_Count": 3,
    "Ground_Truth_Count": 4
  },
  "error": null
}
```

### 2. 여러 쿼리 일괄 평가
**POST** `/evaluate/batch`

여러 쿼리를 한 번에 평가하고 전체 요약 메트릭을 제공합니다.

**Request Body:**
```json
{
  "queries": [
    {"query_number": 1, "predicted_product_ids": ["P001", "P002"]},
    {"query_number": 2, "predicted_product_ids": ["P003", "P004", "P005"]},
    {"query_number": 3, "predicted_product_ids": ["P006"]}
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "total_queries_evaluated": 3,
  "query_results": [
    {
      "query_number": 1,
      "category": "sales",
      "instruction": "...",
      "TP": 2,
      "FP": 0,
      "FN": 2,
      "Precision": 1.0,
      "Recall": 0.5,
      "F1_Score": 0.6667,
      "Exact_Match": 0,
      "Jaccard_Similarity": 0.5,
      "Predicted_Count": 2,
      "Ground_Truth_Count": 4
    },
    ...
  ],
  "overall_metrics": {
    "total_queries": 3,
    "total_tp": 5,
    "total_fp": 3,
    "total_fn": 4,
    "micro_precision": 0.625,
    "micro_recall": 0.5556,
    "micro_f1": 0.5882,
    "macro_precision": 0.7222,
    "macro_recall": 0.5185,
    "macro_f1": 0.5873,
    "exact_match_rate": 0.0,
    "avg_jaccard": 0.4667,
    "category_metrics": {
      "sales": {
        "Precision": 0.8333,
        "Recall": 0.5,
        "F1_Score": 0.5833,
        "Exact_Match": 0.0,
        "Count": 2
      },
      "inventory": {
        "Precision": 0.5,
        "Recall": 0.5556,
        "F1_Score": 0.5263,
        "Exact_Match": 0.0,
        "Count": 1
      }
    }
  },
  "error": null
}
```

### 3. Health Check
**GET** `/health`

API 서버와 데이터베이스 연결 상태를 확인합니다.

**Response:**
```json
{
  "status": "healthy",
  "database": "connected"
}
```

### 4. 평가 요약 조회
**GET** `/evaluate/summary`

현재까지 수행된 모든 평가의 요약 정보를 조회합니다.

### 5. 평가 결과 초기화
**DELETE** `/evaluate/reset`

저장된 모든 평가 결과를 초기화합니다.

## 사용 예제 (Python)

### requests 라이브러리를 사용한 호출 예제

```python
import requests
import json

# API 서버 주소
BASE_URL = "http://localhost:8000"

# 1. 단일 쿼리 평가
def evaluate_single_query(query_number, predicted_ids):
    url = f"{BASE_URL}/evaluate/single"
    payload = {
        "query_number": query_number,
        "predicted_product_ids": predicted_ids
    }
    response = requests.post(url, json=payload)
    return response.json()

# 2. 배치 평가
def evaluate_batch_queries(queries):
    url = f"{BASE_URL}/evaluate/batch"
    payload = {"queries": queries}
    response = requests.post(url, json=payload)
    return response.json()

# 사용 예제
if __name__ == "__main__":
    # 단일 쿼리 평가
    result = evaluate_single_query(1, ["P001", "P002", "P003"])
    print("단일 쿼리 평가 결과:")
    print(f"F1 Score: {result['query_metrics']['F1_Score']:.2%}")

    # 배치 평가
    batch_queries = [
        {"query_number": 1, "predicted_product_ids": ["P001", "P002"]},
        {"query_number": 2, "predicted_product_ids": ["P003", "P004", "P005"]},
        {"query_number": 3, "predicted_product_ids": ["P006"]}
    ]

    batch_result = evaluate_batch_queries(batch_queries)
    print(f"\n배치 평가 완료: {batch_result['total_queries_evaluated']}개 쿼리")
    print(f"전체 Micro F1: {batch_result['overall_metrics']['micro_f1']:.2%}")
    print(f"전체 Macro F1: {batch_result['overall_metrics']['macro_f1']:.2%}")
```

## API 문서

FastAPI의 자동 문서 생성 기능을 사용하여 대화형 API 문서를 확인할 수 있습니다:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 평가 메트릭 설명

- **TP (True Positives)**: 예측과 정답 모두에 있는 product_id 수
- **FP (False Positives)**: 예측에만 있고 정답에는 없는 product_id 수
- **FN (False Negatives)**: 정답에만 있고 예측에는 없는 product_id 수
- **Precision**: TP / (TP + FP) - 예측한 것 중 정답 비율
- **Recall**: TP / (TP + FN) - 정답 중 예측한 비율
- **F1 Score**: 2 * (Precision * Recall) / (Precision + Recall)
- **Exact Match**: 예측과 정답이 완전히 일치하는지 여부 (0 또는 1)
- **Jaccard Similarity**: TP / (TP + FP + FN) - IoU (Intersection over Union)

### Micro vs Macro 평균

- **Micro-averaged**: 전체 TP, FP, FN을 합산한 후 계산
- **Macro-averaged**: 각 쿼리의 메트릭을 먼저 계산한 후 평균

## 주의사항

1. 정답 데이터는 `tc_check_table_20251015` 테이블에 있어야 합니다.
2. API는 동시에 여러 요청을 처리할 수 있지만, 각 요청은 독립적으로 처리됩니다.
3. 데이터베이스 연결이 끊어진 경우 자동으로 재연결을 시도합니다.