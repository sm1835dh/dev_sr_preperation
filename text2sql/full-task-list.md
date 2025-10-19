# Text2SQL LangGraph 프로젝트 전체 Task List

## 프로젝트 개요
Text2SQL 문제를 해결하기 위한 LangGraph 기반 시스템 구축. 자연어 질문을 SQL로 변환하고 실행하여 결과를 반환하는 end-to-end 파이프라인.

## 기술 스택
- **언어**: Python 3.12
- **프레임워크**: LangGraph, LangChain
- **데이터베이스**: PostgreSQL, MongoDB
- **AI 서비스**: Azure OpenAI, Azure AI Search
- **개발 도구**: uv, Black, Pylint

---

## 📝 Phase 0: 프로젝트 초기화 [2시간]

### Task 0.1: 개발 환경 구축
- [ ] uv 설치 및 Python 3.12 환경 설정
- [ ] 프로젝트 디렉토리 생성
- [ ] Git 저장소 초기화
- [ ] .gitignore 파일 생성

### Task 0.2: 프로젝트 구조 생성
- [ ] 디렉토리 구조 생성 (src, tests, data 등)
- [ ] __init__.py 파일 배치
- [ ] README.md 기본 템플릿 작성

### Task 0.3: 의존성 관리 설정
- [ ] pyproject.toml 생성
- [ ] 필수 패키지 정의
- [ ] 개발 패키지 정의
- [ ] 패키지 설치 및 검증

### Task 0.4: 코드 품질 도구 설정
- [ ] Black 설정 (pyproject.toml)
- [ ] Pylint 설정 (.pylintrc)
- [ ] pre-commit 설정
- [ ] Makefile 생성

---

## 📝 Phase 1: 기본 인프라 모듈 [3시간]

### Task 1.1: Logger 모듈
- [ ] CustomLogger 클래스 구현
- [ ] 로그 레벨별 색상 설정
- [ ] 파일/콘솔 동시 출력
- [ ] @log_execution decorator
- [ ] 단위 테스트 작성

### Task 1.2: Decorator 컬렉션
- [ ] @retry decorator 구현
- [ ] @cache_result decorator 구현
- [ ] @validate_input decorator 구현
- [ ] @monitor_performance decorator 구현
- [ ] @rate_limit decorator 구현
- [ ] @transaction decorator 구현
- [ ] 각 decorator 테스트

### Task 1.3: Configuration Manager
- [ ] Settings 클래스 (Pydantic)
- [ ] 환경 변수 로드
- [ ] 환경별 설정 분리
- [ ] 설정 검증 메서드
- [ ] 테스트 작성

### Task 1.4: 유틸리티 모듈
- [ ] 공통 헬퍼 함수
- [ ] 상수 정의
- [ ] 예외 클래스 정의

---

## 📝 Phase 2: Connection 관리 [4시간]

### Task 2.1: Base Connection Manager
- [ ] AbstractConnectionManager 추상 클래스
- [ ] ConnectionPool 구현
- [ ] 연결 재시도 로직
- [ ] Health check 메서드
- [ ] 테스트 작성

### Task 2.2: PostgreSQL Connection
- [ ] PostgreSQLConnection 클래스
- [ ] SQLAlchemy 엔진 설정
- [ ] 쿼리 실행 메서드
- [ ] 트랜잭션 관리
- [ ] 스키마 조회 메서드
- [ ] Connection pool 설정
- [ ] 테스트 작성

### Task 2.3: MongoDB Connection
- [ ] MongoDBConnection 클래스
- [ ] CRUD 메서드 구현
- [ ] Aggregation 지원
- [ ] 인덱스 관리
- [ ] 테스트 작성

### Task 2.4: Azure OpenAI Connection
- [ ] AzureOpenAIConnection 클래스
- [ ] Chat completion 메서드
- [ ] Embedding 메서드
- [ ] Rate limiting 처리
- [ ] 비용 추적
- [ ] 테스트 작성

### Task 2.5: Azure AI Search Connection
- [ ] AzureSearchConnection 클래스
- [ ] 검색 메서드 구현
- [ ] Vector search 지원
- [ ] 인덱싱 메서드
- [ ] 테스트 작성

---

## 📝 Phase 3: 핵심 처리 모듈 [6시간]

### Task 3.1: Question Input Module
- [ ] QuestionInputNode 클래스
- [ ] 입력 전처리 (정규화, 특수문자 처리)
- [ ] 입력 검증 (@validate_input)
- [ ] 질문 타입 분석
- [ ] 컨텍스트 추출
- [ ] 테스트 작성

### Task 3.2: Intent Classification Module
- [ ] IntentClassifierNode 클래스
- [ ] Intent 카테고리 정의
- [ ] LLM 기반 분류 구현
- [ ] Few-shot 프롬프트 템플릿
- [ ] 추가 질문 필요 판단
- [ ] 결과 캐싱 (@cache_result)
- [ ] 테스트 작성

### Task 3.3: Entity Extraction Module
- [ ] EntityExtractorNode 클래스
- [ ] Entity 타입 정의
- [ ] LLM 기반 추출
- [ ] Entity 검증 (DB/RAG)
- [ ] Entity 정규화
- [ ] 관계 분석
- [ ] 테스트 작성

### Task 3.4: Key-Value Extraction Module
- [ ] KeyValueExtractorNode 클래스
- [ ] 스키마 정의 및 로드
- [ ] Key-Value 매핑
- [ ] RAG 기반 Key 검색
- [ ] 조건절 파싱
- [ ] Value 정규화
- [ ] 테스트 작성

### Task 3.5: SQL Generation Module
- [ ] SQLGeneratorNode 클래스
- [ ] DB 스키마 자동 로드
- [ ] SQL 템플릿 관리
- [ ] LLM 기반 SQL 생성
- [ ] SQL 검증 (@sql_validation)
- [ ] SQL 최적화
- [ ] Product ID 쿼리 생성
- [ ] 테스트 작성

### Task 3.6: SQL Execution Module
- [ ] SQLExecutorNode 클래스
- [ ] SQL 실행 메서드
- [ ] 재시도 메커니즘 (n회)
- [ ] 에러 분석 및 수정
- [ ] 결과 후처리
- [ ] Product ID 추출
- [ ] 성능 모니터링
- [ ] 테스트 작성

---

## 📝 Phase 4: LangGraph Workflow [3시간]

### Task 4.1: Workflow State
- [ ] WorkflowState 모델 정의
- [ ] State 업데이트 로직
- [ ] State 전환 검증
- [ ] State 저장/복구
- [ ] 테스트 작성

### Task 4.2: Workflow Graph
- [ ] Text2SQLWorkflow 클래스
- [ ] Graph 구조 정의
- [ ] 노드 추가 및 연결
- [ ] 조건부 엣지 설정
- [ ] 병렬 실행 설정
- [ ] 에러 처리 플로우
- [ ] 테스트 작성

### Task 4.3: Workflow Orchestrator
- [ ] WorkflowOrchestrator 클래스
- [ ] 실행 메서드 구현
- [ ] 배치 실행 지원
- [ ] 모니터링 통합
- [ ] 결과 캐싱
- [ ] 이벤트 처리
- [ ] 테스트 작성

---

## 📝 Phase 5: 평가 시스템 [3시간]

### Task 5.1: Test Case Manager
- [ ] TestCase 모델 정의
- [ ] TC 파일 로드 (JSON/YAML)
- [ ] TC 실행 메서드
- [ ] 배치 실행 지원
- [ ] 결과 비교 로직
- [ ] 테스트 작성

### Task 5.2: Evaluation Metrics
- [ ] MetricsCalculator 클래스
- [ ] 정확도 메트릭 계산
- [ ] 성능 메트릭 계산
- [ ] 에러 분석
- [ ] 리포트 생성 (Markdown/HTML)
- [ ] 실시간 모니터링
- [ ] 테스트 작성

---

## 📝 Phase 6: CLI 및 애플리케이션 [2시간]

### Task 6.1: CLI Interface
- [ ] Click 기반 CLI 구조
- [ ] run 명령어 (단일 질문)
- [ ] batch 명령어 (배치 처리)
- [ ] evaluate 명령어 (TC 평가)
- [ ] schema 명령어 (DB 스키마)
- [ ] interactive 모드
- [ ] 테스트 작성

### Task 6.2: Main Application
- [ ] FastAPI 앱 구현 (선택)
- [ ] Streamlit UI (선택)
- [ ] 엔드포인트 정의
- [ ] Health check
- [ ] 테스트 작성

---

## 📝 Phase 7: 문서화 [2시간]

### Task 7.1: 코드 문서화
- [ ] 모든 클래스/함수 docstring
- [ ] Type hints 추가
- [ ] 인라인 주석

### Task 7.2: 프로젝트 문서
- [ ] README.md 완성
- [ ] API 문서 생성
- [ ] 아키텍처 다이어그램
- [ ] 사용 예제 작성

### Task 7.3: 사용자 가이드
- [ ] 설치 가이드
- [ ] 설정 가이드
- [ ] CLI 사용법
- [ ] 트러블슈팅

---

## 📝 Phase 8: 배포 및 운영 [2시간]

### Task 8.1: Docker 설정
- [ ] Dockerfile 작성
- [ ] docker-compose.yml 작성
- [ ] .dockerignore 설정
- [ ] 빌드 및 테스트

### Task 8.2: CI/CD Pipeline
- [ ] GitHub Actions workflow
- [ ] 자동 테스트 설정
- [ ] 코드 품질 체크
- [ ] 자동 배포 설정

### Task 8.3: 모니터링 설정
- [ ] 로깅 설정
- [ ] 메트릭 수집
- [ ] 알람 설정
- [ ] 대시보드 구성

---

## 📊 진행 상황 추적

### 완료된 Task
- 총 Task: 0 / 108
- 진행률: 0%

### 현재 진행 중
- [ ] (현재 작업 중인 task 기록)

### 다음 우선순위
1. Phase 0: 프로젝트 초기화
2. Phase 1: 기본 인프라 모듈
3. Phase 2: Connection 관리

---

## 🎯 마일스톤

### M1: MVP (최소 기능 제품)
- **목표**: 기본적인 Text2SQL 변환 및 실행
- **포함**: Phase 0-4 필수 기능
- **예상 시간**: 18시간

### M2: 평가 시스템
- **목표**: TC 기반 평가 및 메트릭 수집
- **포함**: Phase 5
- **예상 시간**: 3시간

### M3: 사용자 인터페이스
- **목표**: CLI 및 웹 인터페이스 제공
- **포함**: Phase 6
- **예상 시간**: 2시간

### M4: 프로덕션 준비
- **목표**: 문서화 및 배포 준비
- **포함**: Phase 7-8
- **예상 시간**: 4시간

---

## 📝 참고사항

### 개발 원칙
1. **모듈화**: 의미 단위로 모듈 분리
2. **Decorator 활용**: 코드 중복 최소화
3. **에러 처리**: 모든 예외 상황 고려
4. **테스트**: 모든 모듈에 단위 테스트
5. **문서화**: 명확한 docstring과 주석

### 주의사항
- SQL Injection 방지 철저
- Connection pool 관리 중요
- API Rate limiting 고려
- 캐싱 전략 수립
- 성능 모니터링 필수

### 리소스
- [LangGraph Documentation](https://github.com/langchain-ai/langgraph)
- [Azure OpenAI API Reference](https://docs.microsoft.com/azure/cognitive-services/openai/)
- [PostgreSQL Best Practices](https://www.postgresql.org/docs/current/)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)

---

## 🔄 업데이트 이력

| 날짜 | 버전 | 변경사항 | 작성자 |
|------|------|---------|--------|
| 2024-01-XX | 1.0 | 초기 작성 | - |

---

## 📞 연락처

프로젝트 관련 문의사항이 있으시면 아래로 연락주세요:
- **이메일**: (프로젝트 담당자 이메일)
- **Slack**: #text2sql-project
- **GitHub Issues**: (저장소 링크)