# Text2SQL LangGraph 모듈별 개발 Task

## 📦 Phase 1: Base Infrastructure Modules

### Task 1.1: Logger Module (`src/utils/logger.py`)
```python
구현 사항:
1. CustomLogger 클래스 구현
   - 싱글톤 패턴 적용
   - 로그 레벨별 색상 지정 (colorlog 사용)
   - 파일 및 콘솔 동시 출력
   - 로그 로테이션 설정 (일별/크기별)

2. @log_execution decorator 구현
   - 함수 진입/종료 로깅
   - 실행 시간 측정
   - 파라미터 및 리턴값 로깅 (선택적)
   - 에러 발생 시 자동 로깅

3. 구조화된 로깅 포맷
   - JSON 형식 로그 출력 옵션
   - 컨텍스트 정보 포함 (request_id, user_id 등)
   - 성능 메트릭 자동 포함

테스트 케이스:
- 로그 레벨별 출력 테스트
- 로그 로테이션 동작 테스트
- Decorator 적용 테스트
```

### Task 1.2: Decorators Collection (`src/utils/decorators.py`)
```python
구현할 Decorator 목록:

1. @retry decorator
   - 파라미터: max_attempts, backoff_factor, exceptions
   - 지수 백오프 알고리즘
   - 재시도 간 대기 시간 로깅
   - 특정 예외만 재시도 옵션
   
2. @cache_result decorator
   - 파라미터: ttl, cache_key_func, cache_backend
   - Redis/In-memory 캐시 지원
   - 캐시 키 커스터마이징
   - 캐시 무효화 메서드
   
3. @validate_input decorator
   - Pydantic 모델 기반 검증
   - 타입 힌트 자동 파싱
   - 에러 메시지 커스터마이징
   - 검증 실패 시 상세 에러 정보
   
4. @monitor_performance decorator
   - 실행 시간 측정
   - 메모리 사용량 추적
   - CPU 사용률 모니터링
   - Prometheus 메트릭 export
   
5. @rate_limit decorator
   - 파라미터: calls, period, scope
   - Token bucket 알고리즘
   - 사용자/IP별 제한 옵션
   - 제한 초과 시 대기 시간 반환
   
6. @transaction decorator
   - DB 트랜잭션 자동 관리
   - 롤백 처리
   - 중첩 트랜잭션 지원
   - 격리 수준 설정 옵션

테스트 케이스:
- 각 decorator 독립 테스트
- Decorator 조합 테스트
- 성능 영향 측정 테스트
```

### Task 1.3: Configuration Manager (`src/config/settings.py`)
```python
구현 사항:

1. Settings 클래스 (Pydantic BaseSettings 상속)
   - 환경 변수 자동 로드
   - 타입 검증 및 변환
   - 기본값 설정
   - 환경별 오버라이드 (.env.dev, .env.prod)

2. 설정 카테고리:
   # Database Settings
   - POSTGRES_HOST, PORT, DB, USER, PASSWORD
   - POSTGRES_POOL_SIZE, MAX_OVERFLOW
   - MONGODB_URI, DATABASE, COLLECTION
   
   # Azure Settings  
   - AZURE_OPENAI_KEY, ENDPOINT, DEPLOYMENT
   - AZURE_SEARCH_KEY, ENDPOINT, INDEX
   - AZURE_API_VERSION
   
   # Application Settings
   - LOG_LEVEL, LOG_FORMAT
   - CACHE_TTL, CACHE_BACKEND
   - MAX_RETRIES, RETRY_BACKOFF
   - REQUEST_TIMEOUT
   
   # LangGraph Settings
   - WORKFLOW_MAX_STEPS
   - PARALLEL_EXECUTION
   - STATE_PERSISTENCE

3. 설정 검증 메서드
   - validate_database_connection()
   - validate_azure_credentials()
   - check_required_settings()

4. 동적 설정 리로드
   - 파일 변경 감지
   - Hot reload 지원
   - 설정 변경 이벤트 발행

테스트 케이스:
- 환경 변수 로드 테스트
- 설정 검증 테스트
- 환경별 오버라이드 테스트
```

## 📦 Phase 2: Connection Management Modules

### Task 2.1: Base Connection Manager (`src/connections/base.py`)
```python
구현 사항:

1. AbstractConnectionManager 추상 클래스
   @abstractmethod:
   - connect()
   - disconnect()
   - health_check()
   - get_connection()
   - execute()

2. ConnectionPool 클래스
   - 최대 연결 수 관리
   - 연결 재사용 로직
   - 유휴 연결 정리
   - 연결 상태 모니터링
   
3. 공통 기능:
   - 연결 재시도 로직
   - 연결 타임아웃 처리
   - 연결 상태 로깅
   - 메트릭 수집

테스트 케이스:
- 연결 풀 동작 테스트
- 동시성 테스트
- 연결 실패 처리 테스트
```

### Task 2.2: PostgreSQL Connection (`src/connections/postgresql.py`)
```python
구현 사항:

1. PostgreSQLConnection 클래스
   - SQLAlchemy Engine 생성
   - Session factory 설정
   - Connection pool 설정
     * pool_size=20
     * max_overflow=10
     * pool_timeout=30
     * pool_recycle=3600

2. 쿼리 실행 메서드:
   def execute_query(sql: str, params: dict = None) -> List[Dict]:
       - Parameterized query 실행
       - 결과 Dictionary 변환
       - 에러 처리 및 로깅
   
   def execute_many(sql: str, data: List[Dict]) -> int:
       - Bulk insert/update
       - 트랜잭션 처리
       - 영향받은 행 수 반환
   
   def get_table_schema(table_name: str) -> Dict:
       - 테이블 컬럼 정보 조회
       - 인덱스 정보 포함
       - 제약조건 정보 포함

3. 트랜잭션 관리:
   @contextmanager
   def transaction():
       - 자동 commit/rollback
       - Savepoint 지원
       - 격리 수준 설정

4. 성능 최적화:
   - Prepared statement 캐싱
   - 연결 keep-alive 설정
   - Query plan 캐싱

테스트 케이스:
- CRUD 작업 테스트
- 트랜잭션 롤백 테스트
- 대용량 데이터 처리 테스트
- 동시성 처리 테스트
```

### Task 2.3: MongoDB Connection (`src/connections/mongodb.py`)
```python
구현 사항:

1. MongoDBConnection 클래스
   - MongoClient 설정
     * maxPoolSize=50
     * minPoolSize=10
     * serverSelectionTimeoutMS=5000
   - Database/Collection 접근
   - Connection string 파싱

2. CRUD 메서드:
   def insert_one(collection: str, document: Dict) -> str:
       - 문서 삽입
       - ObjectId 반환
   
   def find(collection: str, filter: Dict, projection: Dict = None) -> List[Dict]:
       - 조회 쿼리
       - 페이지네이션 지원
       - 정렬 옵션
   
   def update_many(collection: str, filter: Dict, update: Dict) -> int:
       - 대량 업데이트
       - Upsert 옵션
   
   def aggregate(collection: str, pipeline: List[Dict]) -> List[Dict]:
       - Aggregation pipeline 실행
       - 스트리밍 옵션

3. 인덱스 관리:
   - 인덱스 생성/삭제
   - 복합 인덱스 지원
   - 텍스트 인덱스 설정

4. GridFS 지원:
   - 대용량 파일 저장
   - 청크 단위 스트리밍

테스트 케이스:
- 기본 CRUD 테스트
- Aggregation 테스트
- 대용량 데이터 테스트
- 연결 복구 테스트
```

### Task 2.4: Azure OpenAI Connection (`src/connections/azure_openai.py`)
```python
구현 사항:

1. AzureOpenAIConnection 클래스
   - Azure OpenAI 클라이언트 초기화
   - API 버전 관리
   - 엔드포인트/키 관리
   - 모델 배포 이름 관리

2. 모델 호출 메서드:
   async def chat_completion(
       messages: List[Dict],
       model: str = "gpt-4o",
       temperature: float = 0,
       max_tokens: int = None,
       tools: List[Dict] = None
   ) -> Dict:
       - 스트리밍/비스트리밍 옵션
       - Function calling 지원
       - 재시도 로직 (429 에러)
       - 토큰 사용량 추적

   async def embeddings(
       texts: List[str],
       model: str = "text-embedding-3"
   ) -> List[List[float]]:
       - 배치 임베딩 생성
       - 차원 축소 옵션

3. 프롬프트 관리:
   - 프롬프트 템플릿 캐싱
   - Few-shot examples 관리
   - System prompt 버전 관리

4. Rate limiting:
   - TPM/RPM 제한 관리
   - 백오프 전략
   - 요청 큐잉

5. 비용 추적:
   - 토큰 사용량 로깅
   - 모델별 비용 계산
   - 일별/월별 집계

테스트 케이스:
- API 호출 테스트
- 스트리밍 응답 테스트
- Rate limit 처리 테스트
- Function calling 테스트
```

### Task 2.5: Azure AI Search Connection (`src/connections/azure_search.py`)
```python
구현 사항:

1. AzureSearchConnection 클래스
   - SearchClient 초기화
   - 인덱스 관리 클라이언트
   - 다중 인덱스 지원

2. 검색 메서드:
   def search(
       query: str,
       filter: str = None,
       select: List[str] = None,
       top: int = 10,
       skip: int = 0,
       semantic_config: str = None
   ) -> SearchResults:
       - 키워드 검색
       - 필터링 및 패싯
       - 하이라이팅
       - Semantic search

   def vector_search(
       vector: List[float],
       k: int = 10,
       filter: str = None
   ) -> List[Dict]:
       - 벡터 유사도 검색
       - Hybrid search (키워드 + 벡터)
       - Re-ranking 옵션

3. 인덱싱 메서드:
   def index_documents(
       documents: List[Dict],
       batch_size: int = 100
   ) -> Dict:
       - 배치 인덱싱
       - 병합/업데이트 옵션
       - 실패 문서 처리

4. 인덱스 관리:
   - 스키마 생성/수정
   - Analyzer 설정
   - Scoring profile 관리
   - Suggester 설정

5. 모니터링:
   - 검색 쿼리 로깅
   - 성능 메트릭
   - 인덱스 통계

테스트 케이스:
- 검색 정확도 테스트
- 벡터 검색 테스트
- 대량 인덱싱 테스트
- 필터링 테스트
```

## 📦 Phase 3: Core Processing Modules

### Task 3.1: Question Input Module (`src/modules/question_input.py`)
```python
구현 사항:

1. QuestionInputNode 클래스:
   class QuestionInputNode(BaseNode):
       def __init__(self):
           - 입력 검증 규칙 로드
           - 전처리 파이프라인 초기화
           - 정규화 규칙 설정

2. 입력 전처리 메서드:
   def preprocess(self, question: str) -> str:
       - 공백 정규화 (연속 공백 제거)
       - 특수문자 처리
       - 한글 자모 분리 방지
       - 이모지 제거/유지 옵션
       - 대소문자 정규화

3. 입력 검증:
   @validate_input
   def validate_question(self, question: str) -> ValidationResult:
       - 최소/최대 길이 체크
       - SQL Injection 패턴 검사
       - 금지어 필터링
       - 언어 감지
       - 질문 형식 검증

4. 질문 분석:
   def analyze_question_type(self, question: str) -> QuestionType:
       - 의문사 추출 (무엇, 어떤, 얼마나 등)
       - 질문 패턴 분류
       - 복잡도 평가
       - 다중 질문 감지

5. 컨텍스트 추출:
   def extract_context(self, question: str) -> Dict:
       - 시간 표현 추출 (오늘, 이번달, 작년 등)
       - 비교 표현 감지 (더, 가장, ~보다 등)
       - 조건절 추출
       - 집계 함수 힌트 추출

테스트 케이스:
- 다양한 질문 형식 테스트
- 악의적 입력 방어 테스트
- 전처리 정확도 테스트
- 성능 벤치마크
```

### Task 3.2: Intent Classification Module (`src/modules/intent_classifier.py`)
```python
구현 사항:

1. IntentClassifierNode 클래스:
   class IntentClassifierNode(BaseNode):
       INTENT_CATEGORIES = [
           "price_inquiry",      # 가격 문의
           "comparison",         # 비교
           "benefit_check",      # 혜택 확인
           "availability",       # 재고/가용성
           "specification",      # 사양 조회
           "recommendation",     # 추천
           "calculation",        # 계산
           "aggregation"        # 집계
       ]

2. Intent 분류 메서드:
   async def classify_intent(
       self, 
       question: str, 
       context: Dict = None
   ) -> IntentResult:
       - Few-shot 프롬프트 구성
       - Azure OpenAI 호출
       - 신뢰도 점수 계산
       - 다중 intent 처리
       - 결과 후처리

3. 프롬프트 템플릿:
   CLASSIFICATION_PROMPT = """
   질문을 다음 카테고리 중 하나로 분류하세요:
   - price_inquiry: 가격 관련 질문
   - comparison: 두 개 이상 항목 비교
   - benefit_check: 할인, 혜택, 프로모션
   ...
   
   Examples:
   Q: "갤럭시 S24 가격이 얼마인가요?"
   A: {"intent": "price_inquiry", "confidence": 0.95}
   
   Q: "{question}"
   A:
   """

4. 추가 질문 판단:
   def check_clarification_needed(
       self, 
       question: str, 
       intent: str
   ) -> ClarificationResult:
       - 모호성 검사
       - 필수 정보 누락 체크
       - 추가 질문 생성
       - 우선순위 설정

5. Intent 캐싱:
   @cache_result(ttl=3600)
   def get_cached_intent(self, question_hash: str) -> Optional[IntentResult]:
       - 질문 해시 생성
       - 캐시 조회
       - 유효성 검증

6. Intent 학습 데이터 관리:
   def update_training_examples(
       self, 
       question: str, 
       intent: str, 
       is_correct: bool
   ):
       - 피드백 수집
       - Few-shot 예제 업데이트
       - 성능 메트릭 갱신

테스트 케이스:
- 각 Intent별 분류 정확도
- 모호한 질문 처리
- 다중 Intent 케이스
- 캐싱 효과 측정
```

### Task 3.3: Entity Extraction Module (`src/modules/entity_extractor.py`)
```python
구현 사항:

1. EntityExtractorNode 클래스:
   class EntityExtractorNode(BaseNode):
       ENTITY_TYPES = {
           "product": "제품명",
           "model": "모델명", 
           "capacity": "용량/옵션",
           "color": "색상",
           "price_range": "가격대",
           "date_range": "기간",
           "location": "지역/매장",
           "quantity": "수량"
       }

2. Entity 추출 메서드:
   async def extract_entities(
       self, 
       question: str,
       intent: str = None
   ) -> EntityResult:
       - NER 프롬프트 구성
       - 패턴 매칭 (정규식)
       - LLM 기반 추출
       - Entity 정규화
       - 신뢰도 점수 할당

3. Entity 검증:
   async def validate_entities(
       self,
       entities: List[Entity],
       use_db: bool = True,
       use_rag: bool = True
   ) -> List[ValidatedEntity]:
       - DB 조회로 유효성 확인
       - RAG 검색으로 보완
       - 유사 Entity 제안
       - 오타 교정

4. 프롬프트 예시:
   EXTRACTION_PROMPT = """
   다음 질문에서 엔티티를 추출하세요:
   
   질문: "갤럭시 S25 512GB 블루 색상 가격이 얼마예요?"
   
   추출할 엔티티 유형:
   - product: 제품명
   - capacity: 용량
   - color: 색상
   
   응답 형식:
   {
       "entities": [
           {"type": "product", "value": "갤럭시 S25", "confidence": 0.95},
           {"type": "capacity", "value": "512GB", "confidence": 1.0},
           {"type": "color", "value": "블루", "confidence": 0.9}
       ]
   }
   """

5. Entity 관계 분석:
   def analyze_entity_relations(
       self,
       entities: List[Entity]
   ) -> EntityRelations:
       - Entity 간 관계 파악
       - 계층 구조 분석
       - 의존성 확인

6. Entity 정규화:
   def normalize_entities(self, entities: List[Entity]) -> List[Entity]:
       - 동의어 통합
       - 단위 변환 (GB, TB 등)
       - 날짜 형식 통일
       - 대소문자 정규화

테스트 케이스:
- 다양한 Entity 타입 추출
- 복합 Entity 처리
- 오타 포함 케이스
- Entity 관계 분석
```

### Task 3.4: Key-Value Extraction Module (`src/modules/kv_extractor.py`)
```python
구현 사항:

1. KeyValueExtractorNode 클래스:
   class KeyValueExtractorNode(BaseNode):
       def __init__(self):
           - 스키마 정의 로드
           - Key 설명 사전 초기화
           - 매핑 규칙 설정

2. 스키마 정의:
   SCHEMA_DEFINITIONS = {
       "product": {
           "name": "제품명",
           "price": "가격",
           "discount_rate": "할인율",
           "stock_quantity": "재고수량",
           "release_date": "출시일"
       },
       "order": {
           "order_id": "주문번호",
           "total_amount": "총금액",
           "status": "주문상태"
       }
   }

3. Key-Value 추출:
   async def extract_key_values(
       self,
       question: str,
       entities: List[Entity],
       schema: str = None
   ) -> KeyValueResult:
       - 스키마 기반 매핑
       - RAG 검색으로 Key 확인
       - Value 타입 추론
       - 조건절 파싱

4. RAG 기반 Key 검색:
   async def search_key_descriptions(
       self,
       query: str,
       top_k: int = 5
   ) -> List[KeyDescription]:
       - Vector search 실행
       - Semantic 유사도 계산
       - 컨텍스트 기반 순위 조정

5. 조건절 파싱:
   def parse_conditions(
       self,
       question: str,
       key_values: Dict
   ) -> List[Condition]:
       """
       예시:
       "가격이 100만원 이상인" -> {"key": "price", "operator": ">=", "value": 1000000}
       "2024년 이후 출시된" -> {"key": "release_date", "operator": ">", "value": "2024-01-01"}
       """
       - 비교 연산자 추출
       - 범위 조건 처리
       - AND/OR 조건 파싱

6. Value 정규화:
   def normalize_values(
       self,
       key_values: Dict,
       schema: Dict
   ) -> Dict:
       - 데이터 타입 변환
       - 단위 통일
       - NULL 처리
       - 기본값 적용

테스트 케이스:
- 스키마 매핑 테스트
- 조건절 파싱 테스트
- RAG 검색 정확도
- 복합 조건 처리
```

### Task 3.5: SQL Generation Module (`src/modules/sql_generator.py`)
```python
구현 사항:

1. SQLGeneratorNode 클래스:
   class SQLGeneratorNode(BaseNode):
       def __init__(self):
           - DB 스키마 로드
           - SQL 템플릿 초기화
           - 방언별 생성기 설정

2. 스키마 자동 로드:
   async def load_database_schema(self) -> DatabaseSchema:
       - 테이블 목록 조회
       - 컬럼 정보 수집
       - 관계(FK) 정보 파악
       - 인덱스 정보 로드
       - 스키마 캐싱

3. SQL 생성 메인 메서드:
   async def generate_sql(
       self,
       intent: str,
       entities: List[Entity],
       key_values: Dict,
       conditions: List[Condition]
   ) -> SQLResult:
       - 테이블 선택
       - JOIN 전략 결정
       - WHERE 절 구성
       - GROUP BY/ORDER BY 처리
       - SQL 최적화

4. 프롬프트 템플릿:
   SQL_GENERATION_PROMPT = """
   Database Schema:
   {schema}
   
   Question: {question}
   Intent: {intent}
   Entities: {entities}
   Conditions: {conditions}
   
   Generate SQL query following these rules:
   1. Use proper JOINs for related tables
   2. Apply appropriate WHERE conditions
   3. Include necessary GROUP BY for aggregations
   4. Add ORDER BY for better readability
   5. Use column aliases for clarity
   
   SQL:
   """

5. SQL 템플릿 관리:
   SQL_TEMPLATES = {
       "price_inquiry": """
           SELECT 
               p.product_name,
               p.price,
               p.discount_rate,
               p.price * (1 - p.discount_rate) as final_price
           FROM products p
           WHERE {conditions}
       """,
       "comparison": """
           SELECT 
               {columns}
           FROM {tables}
           WHERE {conditions}
           ORDER BY {order_by}
       """
   }

6. SQL 검증:
   @sql_validation
   def validate_sql(self, sql: str) -> ValidationResult:
       - 문법 검사 (sqlparse)
       - 위험 쿼리 패턴 검사
       - 테이블/컬럼 존재 확인
       - 예상 실행 계획 분석

7. SQL 최적화:
   def optimize_sql(self, sql: str) -> str:
       - 불필요한 JOIN 제거
       - 인덱스 활용 확인
       - 서브쿼리 -> JOIN 변환
       - LIMIT 자동 추가

8. Product ID 추출 쿼리:
   def generate_product_id_query(
       self,
       main_sql: str
   ) -> str:
       """
       메인 쿼리에서 product_id만 추출하는 쿼리 생성
       """
       - CTE 활용
       - DISTINCT 처리
       - 결과 제한

테스트 케이스:
- 각 Intent별 SQL 생성
- 복잡한 JOIN 케이스
- 집계 함수 처리
- SQL Injection 방어
- 성능 최적화 검증
```

### Task 3.6: SQL Execution Module (`src/modules/sql_executor.py`)
```python
구현 사항:

1. SQLExecutorNode 클래스:
   class SQLExecutorNode(BaseNode):
       def __init__(self):
           - DB 연결 풀 초기화
           - 실행 통계 초기화
           - 에러 핸들러 설정

2. SQL 실행 메서드:
   async def execute_sql(
       self,
       sql: str,
       params: Dict = None,
       timeout: int = 30
   ) -> ExecutionResult:
       - Parameterized query 실행
       - 타임아웃 처리
       - 결과 페치
       - 메타데이터 수집

3. 재시도 메커니즘:
   @retry_on_failure(max_attempts=3)
   async def execute_with_retry(
       self,
       sql: str,
       error_history: List[Error] = None
   ) -> ExecutionResult:
       try:
           return await self.execute_sql(sql)
       except SQLExecutionError as e:
           # 에러 분석
           error_type = self.analyze_error(e)
           
           if error_type == "SYNTAX_ERROR":
               # SQL 수정 시도
               fixed_sql = await self.fix_syntax_error(sql, e)
               return await self.execute_sql(fixed_sql)
           
           elif error_type == "MISSING_COLUMN":
               # 컬럼 대체 시도
               fixed_sql = await self.fix_column_error(sql, e)
               return await self.execute_sql(fixed_sql)
           
           elif error_type == "TIMEOUT":
               # 쿼리 최적화 시도
               optimized_sql = await self.optimize_for_performance(sql)
               return await self.execute_sql(optimized_sql)
           
           raise

4. 에러 분석 및 수정:
   async def analyze_error(self, error: Exception) -> str:
       - 에러 메시지 파싱
       - 에러 타입 분류
       - 수정 가능 여부 판단
   
   async def fix_syntax_error(
       self,
       sql: str,
       error: Exception
   ) -> str:
       - LLM을 통한 SQL 수정
       - 문법 오류 교정
       - 재검증

   async def fix_column_error(
       self,
       sql: str,
       error: Exception  
   ) -> str:
       - 스키마 재확인
       - 유사 컬럼 검색
       - 컬럼명 교체

5. 결과 후처리:
   def process_results(
       self,
       raw_results: List[Tuple],
       columns: List[str]
   ) -> ProcessedResult:
       - Dictionary 변환
       - NULL 처리
       - 데이터 타입 변환
       - 포맷팅

6. Product ID 별도 추출:
   async def extract_product_ids(
       self,
       sql: str
   ) -> List[int]:
       """
       실행된 SQL에서 product_id 목록 추출
       """
       - Product ID 쿼리 생성
       - 별도 실행
       - 중복 제거
       - 결과 반환

7. 성능 모니터링:
   def collect_execution_metrics(
       self,
       sql: str,
       execution_time: float,
       row_count: int
   ) -> ExecutionMetrics:
       - 실행 시간 기록
       - 행 수 기록
       - 쿼리 플랜 수집
       - 리소스 사용량

8. 트랜잭션 관리:
   @transaction
   async def execute_in_transaction(
       self,
       queries: List[str]
   ) -> List[ExecutionResult]:
       - 다중 쿼리 실행
       - 원자성 보장
       - 부분 롤백 지원

테스트 케이스:
- 정상 실행 테스트
- 에러 복구 테스트
- 타임아웃 처리
- 대용량 결과 처리
- 트랜잭션 테스트
```

## 📦 Phase 4: LangGraph Workflow

### Task 4.1: Workflow State (`src/workflow/state.py`)
```python
구현 사항:

1. WorkflowState 정의:
   from pydantic import BaseModel
   from typing import Optional, List, Dict, Any
   from enum import Enum
   
   class WorkflowStatus(Enum):
       PENDING = "pending"
       PROCESSING = "processing"
       COMPLETED = "completed"
       FAILED = "failed"
       RETRY = "retry"
   
   class WorkflowState(BaseModel):
       # 입력 데이터
       question: str
       request_id: str
       user_id: Optional[str] = None
       
       # 처리 단계별 결과
       preprocessed_question: Optional[str] = None
       intent: Optional[str] = None
       intent_confidence: Optional[float] = None
       entities: Optional[List[Dict]] = None
       key_values: Optional[Dict] = None
       conditions: Optional[List[Dict]] = None
       
       # SQL 관련
       generated_sql: Optional[str] = None
       sql_validation_result: Optional[Dict] = None
       execution_result: Optional[Dict] = None
       product_ids: Optional[List[int]] = None
       
       # 에러 및 재시도
       errors: List[Dict] = []
       retry_count: int = 0
       max_retries: int = 3
       
       # 추가 정보
       clarification_needed: bool = False
       clarification_questions: Optional[List[str]] = None
       
       # 메타데이터
       status: WorkflowStatus = WorkflowStatus.PENDING
       start_time: Optional[datetime] = None
       end_time: Optional[datetime] = None
       execution_time: Optional[float] = None
       
       # 설정
       use_cache: bool = True
       parallel_execution: bool = False
       debug_mode: bool = False

2. State 업데이트 메서드:
   class StateManager:
       def update_state(
           self,
           state: WorkflowState,
           updates: Dict[str, Any]
       ) -> WorkflowState:
           - 필드 검증
           - 상태 전환 규칙 확인
           - 타임스탬프 업데이트
           - 이력 기록

3. State 검증:
   def validate_state_transition(
       self,
       current_status: WorkflowStatus,
       new_status: WorkflowStatus
   ) -> bool:
       """
       유효한 상태 전환 규칙:
       PENDING -> PROCESSING
       PROCESSING -> COMPLETED/FAILED/RETRY
       RETRY -> PROCESSING
       FAILED -> (종료)
       COMPLETED -> (종료)
       """

4. State 저장/복구:
   class StatePersistence:
       async def save_state(
           self,
           state: WorkflowState
       ) -> None:
           - Redis/MongoDB 저장
           - 직렬화 처리
           - TTL 설정
       
       async def load_state(
           self,
           request_id: str
       ) -> Optional[WorkflowState]:
           - 상태 조회
           - 역직렬화
           - 유효성 검증

테스트 케이스:
- State 생성/업데이트
- 상태 전환 검증
- 직렬화/역직렬화
- 동시성 처리
```

### Task 4.2: Workflow Graph (`src/workflow/graph.py`)
```python
구현 사항:

1. Graph 정의:
   from langgraph.graph import StateGraph, END
   from langgraph.prebuilt import ToolExecutor
   
   class Text2SQLWorkflow:
       def __init__(self):
           self.graph = StateGraph(WorkflowState)
           self._build_graph()
       
       def _build_graph(self):
           # 노드 추가
           self.graph.add_node("input", self.process_input)
           self.graph.add_node("classify_intent", self.classify_intent)
           self.graph.add_node("extract_entities", self.extract_entities)
           self.graph.add_node("extract_kv", self.extract_key_values)
           self.graph.add_node("generate_sql", self.generate_sql)
           self.graph.add_node("execute_sql", self.execute_sql)
           self.graph.add_node("handle_error", self.handle_error)
           
           # 엣지 추가
           self.graph.add_edge("input", "classify_intent")
           
           # 조건부 엣지
           self.graph.add_conditional_edges(
               "classify_intent",
               self.check_clarification,
               {
                   "needs_clarification": END,
                   "continue": "extract_entities"
               }
           )
           
           # 병렬 실행 설정
           self.graph.add_parallel_edges(
               ["extract_entities", "extract_kv"],
               "generate_sql"
           )
           
           # 에러 처리 엣지
           self.graph.add_conditional_edges(
               "execute_sql",
               self.check_execution_result,
               {
                   "success": END,
                   "retry": "handle_error",
                   "fail": END
               }
           )

2. 노드 구현:
   async def process_input(self, state: WorkflowState) -> WorkflowState:
       - QuestionInputNode 호출
       - 상태 업데이트
       - 로깅
   
   async def classify_intent(self, state: WorkflowState) -> WorkflowState:
       - IntentClassifierNode 호출
       - 캐시 확인
       - 결과 저장
   
   # ... 각 노드별 구현

3. 조건부 라우팅:
   def check_clarification(self, state: WorkflowState) -> str:
       if state.clarification_needed:
           return "needs_clarification"
       return "continue"
   
   def check_execution_result(self, state: WorkflowState) -> str:
       if state.execution_result and not state.errors:
           return "success"
       elif state.retry_count < state.max_retries:
           return "retry"
       return "fail"

4. 에러 처리 노드:
   async def handle_error(self, state: WorkflowState) -> WorkflowState:
       - 에러 분석
       - 재시도 전략 결정
       - SQL 수정
       - 상태 업데이트

5. 병렬 실행 관리:
   async def parallel_execution(
       self,
       state: WorkflowState,
       nodes: List[str]
   ) -> WorkflowState:
       - asyncio.gather 활용
       - 결과 병합
       - 에러 처리

테스트 케이스:
- 전체 워크플로우 실행
- 조건부 분기 테스트
- 병렬 실행 테스트
- 에러 복구 테스트
```

### Task 4.3: Workflow Orchestrator (`src/workflow/orchestrator.py`)
```python
구현 사항:

1. WorkflowOrchestrator 클래스:
   class WorkflowOrchestrator:
       def __init__(self):
           self.workflow = Text2SQLWorkflow()
           self.executor = WorkflowExecutor()
           self.monitor = WorkflowMonitor()

2. 실행 메서드:
   async def run(
       self,
       question: str,
       user_id: str = None,
       config: Dict = None
   ) -> WorkflowResult:
       # 상태 초기화
       state = self.initialize_state(question, user_id, config)
       
       # 모니터링 시작
       with self.monitor.track(state.request_id):
           # 워크플로우 실행
           result = await self.workflow.graph.ainvoke(state)
           
           # 결과 후처리
           return self.process_result(result)

3. 배치 실행:
   async def run_batch(
       self,
       questions: List[str],
       batch_size: int = 10
   ) -> List[WorkflowResult]:
       - 배치 분할
       - 동시 실행 관리
       - 결과 집계
       - 실패 처리

4. 모니터링:
   @monitor_performance
   async def track_execution(
       self,
       state: WorkflowState
   ) -> None:
       - 단계별 시간 측정
       - 리소스 사용량
       - 성공/실패율
       - 병목 지점 파악

5. 결과 캐싱:
   class ResultCache:
       async def get(self, question_hash: str) -> Optional[WorkflowResult]:
           - 캐시 조회
           - 유효성 검증
           
       async def set(
           self,
           question_hash: str,
           result: WorkflowResult,
           ttl: int = 3600
       ) -> None:
           - 결과 저장
           - TTL 설정

6. 이벤트 처리:
   class EventHandler:
       async def on_node_start(self, node: str, state: WorkflowState):
           - 로깅
           - 메트릭 수집
       
       async def on_node_complete(self, node: str, state: WorkflowState):
           - 결과 검증
           - 다음 단계 준비
       
       async def on_error(self, error: Exception, state: WorkflowState):
           - 에러 로깅
           - 알림 전송

테스트 케이스:
- 단일 실행 테스트
- 배치 실행 테스트
- 캐싱 효과 측정
- 모니터링 정확도
```

## 📦 Phase 5: Evaluation System

### Task 5.1: Test Case Manager (`src/evaluation/tc_manager.py`)
```python
구현 사항:

1. TestCase 모델:
   class TestCase(BaseModel):
       id: str
       question: str
       expected_intent: str
       expected_entities: List[Dict]
       expected_sql: Optional[str]
       expected_result: Optional[Dict]
       tags: List[str] = []
       difficulty: str = "medium"
       created_at: datetime
       
2. TestCaseManager 클래스:
   class TestCaseManager:
       def __init__(self, tc_dir: str):
           self.tc_dir = Path(tc_dir)
           self.test_cases = self.load_test_cases()

3. TC 로드:
   def load_test_cases(self) -> List[TestCase]:
       """
       JSON/YAML 파일에서 TC 로드
       """
       - 파일 탐색
       - 포맷 파싱
       - 검증
       - 정렬/필터링

4. TC 실행:
   @benchmark
   async def run_test_case(
       self,
       tc: TestCase,
       orchestrator: WorkflowOrchestrator
   ) -> TestResult:
       - TC 실행
       - 결과 비교
       - 메트릭 수집
       - 상세 로그

5. 배치 실행:
   async def run_batch(
       self,
       test_cases: List[TestCase],
       parallel: int = 5
   ) -> BatchResult:
       - 동시 실행
       - 진행률 추적
       - 결과 집계

6. 결과 비교:
   def compare_results(
       self,
       actual: Dict,
       expected: Dict,
       comparison_type: str
   ) -> ComparisonResult:
       - SQL 비교 (정규화 후)
       - 결과 집합 비교
       - 부분 일치 허용

테스트 케이스:
- TC 파일 로드
- 개별 TC 실행
- 배치 실행
- 결과 비교 정확도
```

### Task 5.2: Evaluation Metrics (`src/evaluation/metrics.py`)
```python
구현 사항:

1. MetricsCalculator 클래스:
   class MetricsCalculator:
       def __init__(self):
           self.metrics = {}

2. 정확도 메트릭:
   def calculate_accuracy_metrics(
       self,
       results: List[TestResult]
   ) -> AccuracyMetrics:
       return {
           "intent_accuracy": self.intent_accuracy(results),
           "entity_precision": self.entity_precision(results),
           "entity_recall": self.entity_recall(results),
           "sql_exact_match": self.sql_exact_match(results),
           "sql_semantic_match": self.sql_semantic_match(results),
           "result_accuracy": self.result_accuracy(results)
       }

3. 성능 메트릭:
   def calculate_performance_metrics(
       self,
       results: List[TestResult]  
   ) -> PerformanceMetrics:
       return {
           "avg_response_time": self.avg_response_time(results),
           "p50_latency": self.percentile_latency(results, 50),
           "p95_latency": self.percentile_latency(results, 95),
           "p99_latency": self.percentile_latency(results, 99),
           "throughput": self.calculate_throughput(results)
       }

4. 에러 분석:
   def analyze_errors(
       self,
       results: List[TestResult]
   ) -> ErrorAnalysis:
       - 에러 타입별 분류
       - 빈도 분석
       - 패턴 파악
       - 개선 제안

5. 리포트 생성:
   class ReportGenerator:
       def generate_markdown_report(
           self,
           metrics: Dict,
           output_path: str
       ) -> None:
           """
           # Test Results Report
           
           ## Summary
           - Total Test Cases: X
           - Success Rate: Y%
           
           ## Accuracy Metrics
           | Metric | Value |
           |--------|-------|
           | Intent Accuracy | 95% |
           ...
           """
       
       def generate_html_report(
           self,
           metrics: Dict,
           output_path: str
       ) -> None:
           - 차트 생성 (plotly)
           - 테이블 포맷팅
           - 인터랙티브 뷰

6. 실시간 모니터링:
   class MetricsMonitor:
       def track_metric(self, name: str, value: float):
           - Prometheus 전송
           - 시계열 저장
           - 알림 트리거

테스트 케이스:
- 메트릭 계산 정확도
- 리포트 생성
- 에러 분석
- 실시간 추적
```

## 📦 Phase 6: CLI and Main Application

### Task 6.1: CLI Interface (`src/cli.py`)
```python
구현 사항:

1. CLI 명령어 구조:
   import click
   
   @click.group()
   def cli():
       """Text2SQL LangGraph CLI"""
       pass
   
   @cli.command()
   @click.option('--question', '-q', help='Question to process')
   @click.option('--debug', is_flag=True, help='Enable debug mode')
   def run(question: str, debug: bool):
       """Run single question"""
       - 단일 질문 처리
       - 결과 출력
       - 디버그 정보
   
   @cli.command()
   @click.option('--file', '-f', help='Input file path')
   @click.option('--output', '-o', help='Output file path')
   def batch(file: str, output: str):
       """Process batch questions"""
       - 파일 읽기
       - 배치 처리
       - 결과 저장
   
   @cli.command()
   @click.option('--tc-dir', default='data/test_cases')
   @click.option('--report', default='report.md')
   def evaluate(tc_dir: str, report: str):
       """Run evaluation"""
       - TC 실행
       - 메트릭 계산
       - 리포트 생성
   
   @cli.command()
   def schema():
       """Show database schema"""
       - 스키마 조회
       - 테이블 정보
       - 관계 표시

2. Interactive 모드:
   @cli.command()
   def interactive():
       """Start interactive session"""
       while True:
           question = input("Question> ")
           if question.lower() in ['exit', 'quit']:
               break
           # 처리 및 출력

3. 설정 관리:
   @cli.command()
   @click.option('--env', type=click.Choice(['dev', 'staging', 'prod']))
   def config(env: str):
       """Manage configuration"""
       - 환경 전환
       - 설정 확인
       - 검증

테스트 케이스:
- 각 명령어 테스트
- 파라미터 검증
- 에러 처리
```

### Task 6.2: Main Entry Point (`src/main.py`)
```python
구현 사항:

1. FastAPI 앱 (선택사항):
   from fastapi import FastAPI, HTTPException
   from pydantic import BaseModel
   
   app = FastAPI(title="Text2SQL API")
   
   class QuestionRequest(BaseModel):
       question: str
       user_id: Optional[str]
       options: Optional[Dict]
   
   @app.post("/process")
   async def process_question(request: QuestionRequest):
       - 요청 처리
       - 워크플로우 실행
       - 응답 반환
   
   @app.get("/health")
   async def health_check():
       - DB 연결 확인
       - 서비스 상태
   
   @app.post("/evaluate")
   async def run_evaluation(tc_ids: List[str]):
       - TC 실행
       - 결과 반환

2. Streamlit 앱 (선택사항):
   import streamlit as st
   
   st.title("Text2SQL Demo")
   
   question = st.text_input("Enter your question:")
   
   if st.button("Process"):
       with st.spinner("Processing..."):
           result = process_question(question)
           st.json(result)

3. 메인 실행:
   if __name__ == "__main__":
       import uvicorn
       uvicorn.run(app, host="0.0.0.0", port=8000)

테스트 케이스:
- API 엔드포인트 테스트
- 동시 요청 처리
- 에러 응답 테스트
```

## 📋 개발 체크리스트

### 필수 구현 (MVP)
- [ ] 프로젝트 구조 설정
- [ ] 환경 설정 및 의존성 관리
- [ ] PostgreSQL Connection
- [ ] Azure OpenAI Connection
- [ ] Question Input Module
- [ ] Intent Classifier Module
- [ ] Entity Extractor Module
- [ ] SQL Generator Module
- [ ] SQL Executor Module
- [ ] Basic Workflow Graph
- [ ] CLI 기본 명령어

### 추가 기능
- [ ] MongoDB Connection
- [ ] Azure AI Search Connection
- [ ] Key-Value Extractor Module
- [ ] Retry 메커니즘
- [ ] TC Manager
- [ ] Evaluation Metrics
- [ ] FastAPI/Streamlit UI
- [ ] 성능 모니터링
- [ ] 배치 처리

### 최적화 및 개선
- [ ] 캐싱 시스템
- [ ] 병렬 처리
- [ ] 에러 복구 개선
- [ ] SQL 최적화
- [ ] 문서화
- [ ] Docker 지원
- [ ] CI/CD 설정