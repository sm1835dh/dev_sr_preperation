# Text-to-SQL System Implementation

이 프로젝트는 BIRD_Overall_2nd 논문 "Automatic Metadata Extraction for Text-to-SQL"을 기반으로 구현된 Text-to-SQL 시스템입니다.

## 구현된 컴포넌트

### 1. 데이터베이스 프로파일링 (Section 2)
- **파일**: `modules/profiler.py`
- **기능**: 데이터베이스 컬럼의 통계 정보 추출
- **구현 사항**:
  - NULL/비NULL 값 개수
  - 고유값 개수
  - 최소/최대값, 평균값
  - Top-K 빈도값
  - MinHash 시그니처 생성
  - 텍스트 패턴 분석

### 2. LLM 메타데이터 요약 (Section 2.1)
- **파일**: `modules/llm_summarizer.py`
- **기능**: 컬럼 정보를 자연어로 요약
- **구현 사항**:
  - 기계적 영어 설명 생성
  - 짧은 설명 (1-2 문장)
  - 긴 설명 (상세한 값 정보 포함)
  - Azure OpenAI 통합

### 3. 스키마 링킹 (Section 3)
- **파일**: `modules/schema_linker.py`
- **기능**: 질문과 관련된 테이블/컬럼 식별
- **구현 사항**:
  - LSH 인덱스 (리터럴 매칭)
  - FAISS 인덱스 (의미 유사도)
  - 다중 패스 알고리즘
  - 5가지 스키마/프로파일 조합
  - 리터럴 추출 및 매칭

### 4. SQL 생성 (Section 4)
- **파일**: `modules/sql_generator.py`
- **기능**: Few-shot 학습으로 SQL 쿼리 생성
- **구현 사항**:
  - Few-shot 예제 선택 (벡터 유사도)
  - 다중 SQL 후보 생성
  - SQL 문법 검증 (sqlglot)
  - 패턴 체크
  - 다수결 투표

### 5. 평가 프레임워크
- **파일**: `modules/evaluator.py`
- **기능**: 시스템 성능 평가
- **구현 사항**:
  - Exact Match 정확도
  - Execution 정확도
  - 스키마 링킹 F1 점수
  - Ablation Study
  - 효율성 측정

## 시스템 아키텍처

```
[BIRD Dataset] → [Database Profiling] → [LLM Summarization]
                                              ↓
[Question] → [Schema Linking] → [SQL Generation] → [Evaluation]
                ↓                      ↓
         [LSH + FAISS]          [Few-shot Learning]
```

## 설정 및 실행

### 환경 설정
1. `.env` 파일에 Azure OpenAI 설정:
```
ENDPOINT_URL=your_azure_endpoint
AZURE_OPENAI_API_KEY=your_api_key
BIRD_DATASET=/path/to/bird/dataset
```

2. 의존성 설치:
```bash
uv sync
```

### 실행 방법

#### 전체 파이프라인 실행 (PostgreSQL 필요)
```bash
uv run python src/main.py
```

#### 테스트 실행 (데이터베이스 불필요)
```bash
uv run python src/test_pipeline.py
```

## 파일 구조

```
src/
├── configs/
│   └── config.py           # 전체 설정
├── modules/
│   ├── database.py         # 데이터베이스 연결
│   ├── bird_loader.py      # BIRD 데이터셋 로더
│   ├── profiler.py         # 데이터베이스 프로파일링
│   ├── llm_summarizer.py   # LLM 메타데이터 요약
│   ├── schema_linker.py    # 스키마 링킹
│   ├── sql_generator.py    # SQL 생성
│   └── evaluator.py        # 평가 프레임워크
├── main.py                 # 메인 파이프라인
├── test_pipeline.py        # 테스트 모듈
└── README.md               # 이 파일
```

## 주요 특징

1. **모듈화된 설계**: 각 논문 섹션이 독립적인 모듈로 구현
2. **Azure OpenAI 통합**: 임베딩 및 텍스트 생성에 Azure OpenAI 사용
3. **BIRD 데이터셋 지원**: BIRD 데이터셋에서 랜덤 샘플 선택 및 로딩
4. **포괄적인 평가**: 다양한 메트릭과 ablation study 지원
5. **오류 처리**: 강건한 오류 처리 및 로깅

## 테스트 결과

모든 핵심 컴포넌트가 성공적으로 테스트됨:
- ✅ Schema Linking: PASS
- ✅ SQL Generation: PASS
- ✅ Evaluator: PASS
- ✅ Integration: PASS

## 향후 개선 사항

1. 더 정교한 SQL 파싱 및 검증
2. 추가적인 벡터 데이터베이스 지원
3. 실시간 스키마 업데이트
4. 더 많은 평가 메트릭
5. 웹 인터페이스 추가

## 라이센스

이 구현은 연구 목적으로 제작되었습니다.