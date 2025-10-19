# Text2SQL LangGraph 프로젝트 공통 요청사항

## 1. 개발 환경 및 도구

### 1.1 Python 환경
- **Python 버전**: 3.12
- **패키지 관리자**: uv (pip 대신 사용)
- **가상환경**: uv를 통한 가상환경 구축

### 1.2 코드 품질 관리
- **코드 포맷터**: Black
  - 라인 길이: 88자 (Black 기본값)
  - 자동 포맷팅 설정
  - pre-commit hook 연동
  
- **코드 검사 도구**: Pylint
  - 코드 스타일 준수
  - 잠재적 버그 검출
  - 복잡도 체크

### 1.3 프로젝트 구조 원칙
```
text2sql-langgraph/
├── .env                    # 환경 변수 (git 제외)
├── .env.example           # 환경 변수 템플릿
├── .gitignore            
├── pyproject.toml         # 프로젝트 의존성 및 설정
├── Makefile              # 자동화 스크립트
├── README.md             
├── src/
│   ├── __init__.py
│   ├── config/           # 설정 관리
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   └── constants.py
│   ├── connections/      # 외부 서비스 연결 관리
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── postgresql.py
│   │   ├── mongodb.py
│   │   ├── azure_openai.py
│   │   └── azure_search.py
│   ├── modules/          # 핵심 처리 모듈
│   │   ├── __init__.py
│   │   ├── question_input.py
│   │   ├── intent_classifier.py
│   │   ├── entity_extractor.py
│   │   ├── kv_extractor.py
│   │   ├── sql_generator.py
│   │   └── sql_executor.py
│   ├── workflow/         # LangGraph 워크플로우
│   │   ├── __init__.py
│   │   ├── state.py
│   │   ├── graph.py
│   │   └── orchestrator.py
│   ├── evaluation/       # 평가 시스템
│   │   ├── __init__.py
│   │   ├── tc_manager.py
│   │   └── metrics.py
│   ├── utils/           # 유틸리티
│   │   ├── __init__.py
│   │   ├── decorators.py
│   │   ├── prompts.py
│   │   └── logger.py
│   ├── cli.py          # CLI 인터페이스
│   └── main.py         # 메인 진입점
├── tests/              # 테스트 코드
│   ├── __init__.py
│   ├── unit/
│   ├── integration/
│   └── fixtures/
└── data/              # 데이터 및 테스트 케이스
    ├── test_cases/
    ├── schemas/
    └── examples/
```

## 2. 코딩 표준 및 원칙

### 2.1 Decorator 활용 원칙
- **반복 코드 최소화**: 공통 기능은 decorator로 추상화
- **관심사 분리**: 핵심 로직과 부가 기능 분리
- **가독성 우선**: 코드 라인 수 감소를 위한 decorator 적극 활용

### 2.2 필수 Decorator 목록
```python
# 연결 관리
@retry(max_attempts=3, backoff=2)
@connection_pool(max_size=10)
@transaction

# 검증 및 캐싱
@validate_input
@cache_result(ttl=3600)
@schema_validation

# 에러 처리 및 모니터링
@error_handler
@log_execution
@monitor_performance
@rate_limit(calls=100, period=60)

# 비즈니스 로직
@extract_entities
@sql_validation
@benchmark
```

### 2.3 모듈화 원칙
- **단일 책임 원칙**: 각 모듈은 하나의 명확한 책임만 가짐
- **의미 단위 분리**: 비즈니스 로직 기준으로 모듈 분리
- **느슨한 결합**: 모듈 간 의존성 최소화
- **인터페이스 기반 설계**: 추상 클래스 활용

### 2.4 에러 처리 원칙
- **Graceful Degradation**: 장애 시에도 최소 기능 보장
- **명시적 에러 처리**: try-except 블록 명확히 정의
- **에러 로깅**: 모든 에러 상황 로깅
- **재시도 전략**: 일시적 장애에 대한 자동 재시도

## 3. 외부 서비스 연동

### 3.1 사용할 서비스
- **Azure OpenAI**: LLM 기반 처리 (Intent 분류, SQL 생성 등)
- **PostgreSQL**: 메인 데이터베이스
- **MongoDB**: NoSQL 데이터 저장
- **Azure AI Search**: RAG 및 벡터 검색

### 3.2 Connection 관리 원칙
- **Connection Pooling**: 모든 DB 연결에 풀링 적용
- **환경별 설정 분리**: dev/staging/prod 환경 구분
- **Timeout 설정**: 모든 연결에 적절한 timeout 설정
- **Health Check**: 주기적 연결 상태 확인

### 3.3 보안 원칙
- **환경 변수 사용**: 모든 credentials는 .env 파일로 관리
- **SQL Injection 방지**: Parameterized queries 사용
- **Input Validation**: 모든 사용자 입력 검증
- **최소 권한 원칙**: 필요한 최소 권한만 부여

## 4. 성능 최적화 가이드라인

### 4.1 캐싱 전략
- **결과 캐싱**: 동일 질문에 대한 결과 캐싱
- **Schema 캐싱**: DB 스키마 정보 캐싱
- **Prompt 캐싱**: 자주 사용되는 프롬프트 템플릿 캐싱

### 4.2 비동기 처리
- **Async/Await 활용**: I/O 바운드 작업 비동기 처리
- **Concurrent 실행**: 독립적인 작업 병렬 처리
- **LangGraph 병렬 노드**: 가능한 경우 노드 병렬 실행

### 4.3 리소스 관리
- **메모리 관리**: 대용량 데이터 처리 시 스트리밍 사용
- **Connection 재사용**: 연결 풀 통한 리소스 재사용
- **Batch 처리**: 대량 데이터 배치 단위 처리

## 5. 테스트 및 평가

### 5.1 테스트 전략
- **단위 테스트**: 모든 모듈별 단위 테스트 작성
- **통합 테스트**: 워크플로우 전체 통합 테스트
- **성능 테스트**: 응답 시간 및 처리량 테스트
- **커버리지 목표**: 80% 이상 코드 커버리지

### 5.2 평가 메트릭
- **SQL 정확도**: 생성된 SQL의 정확성
- **Intent 분류 정확도**: 의도 분류 성공률
- **Entity 추출 정확도**: 엔티티 추출 성공률
- **응답 시간**: End-to-end 처리 시간
- **에러율**: 실패 쿼리 비율

### 5.3 TC (Test Case) 관리
- **독립 실행**: TC 평가는 별도 명령으로 실행
- **배치 처리**: 여러 TC 동시 실행 지원
- **결과 리포팅**: Markdown/HTML 형식 리포트 생성
- **버전 관리**: TC 버전 관리 및 이력 추적

## 6. 로깅 및 모니터링

### 6.1 로깅 레벨
```python
# 로깅 레벨별 사용 기준
DEBUG: 상세 디버깅 정보
INFO: 일반 정보성 메시지
WARNING: 경고 메시지
ERROR: 에러 발생 시
CRITICAL: 시스템 중단 위험 상황
```

### 6.2 로깅 포맷
```python
# 표준 로깅 포맷
{
    "timestamp": "2024-01-01T00:00:00",
    "level": "INFO",
    "module": "module_name",
    "function": "function_name",
    "message": "log message",
    "extra": {...}
}
```

### 6.3 메트릭 수집
- **실행 시간**: 각 모듈별 실행 시간
- **API 호출 횟수**: 외부 API 호출 통계
- **DB 쿼리 횟수**: 데이터베이스 쿼리 통계
- **캐시 히트율**: 캐싱 효율성 측정

## 7. 문서화 표준

### 7.1 코드 문서화
- **Docstring**: 모든 클래스/함수에 Google Style docstring
- **Type Hints**: 모든 함수 파라미터와 리턴값에 타입 힌트
- **주석**: 복잡한 로직에 대한 인라인 주석

### 7.2 프로젝트 문서
- **README.md**: 프로젝트 개요 및 시작 가이드
- **API 문서**: 자동 생성 (Sphinx/MkDocs)
- **아키텍처 문서**: 시스템 구조 다이어그램
- **사용 예제**: 실제 사용 시나리오별 예제

## 8. 배포 및 운영

### 8.1 환경 구분
```yaml
# 환경별 설정
development:
  - 디버그 모드 활성화
  - 상세 로깅
  - Mock 데이터 사용 가능

staging:
  - 프로덕션과 동일 구성
  - 테스트 데이터 사용
  - 성능 모니터링 활성화

production:
  - 최적화 모드
  - 에러 로깅만
  - 실제 데이터 사용
```

### 8.2 CI/CD 파이프라인
- **자동 테스트**: Push/PR 시 자동 테스트 실행
- **코드 품질 체크**: Black, Pylint 자동 실행
- **보안 스캔**: 의존성 취약점 검사
- **자동 배포**: 테스트 통과 시 자동 배포

## 9. 의존성 패키지 버전 관리

```toml
# pyproject.toml 주요 패키지 버전
[tool.poetry.dependencies]
python = "^3.12"
langgraph = "^0.2.0"
langchain = "^0.3.0"
langchain-openai = "^0.2.0"
sqlalchemy = "^2.0.0"
psycopg2-binary = "^2.9.0"
pymongo = "^4.0.0"
azure-search-documents = "^11.0.0"
azure-identity = "^1.0.0"
python-dotenv = "^1.0.0"
pydantic = "^2.0.0"
fastapi = "^0.100.0"  # Optional
streamlit = "^1.30.0"  # Optional
click = "^8.0.0"       # CLI용

[tool.poetry.group.dev.dependencies]
black = "^24.0.0"
pylint = "^3.0.0"
pytest = "^8.0.0"
pytest-asyncio = "^0.23.0"
pytest-cov = "^4.0.0"
pre-commit = "^3.0.0"
```

## 10. Makefile 명령어

```makefile
# 개발 환경 설정
setup:
	uv venv
	uv pip install -e .

# 코드 품질 관리
format:
	black src/ tests/

lint:
	pylint src/

# 테스트
test:
	pytest tests/

test-coverage:
	pytest --cov=src tests/

# 실행
run:
	python -m src.main

run-cli:
	python -m src.cli

# TC 평가
evaluate:
	python -m src.cli evaluate --tc-dir data/test_cases/

# 청소
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
```