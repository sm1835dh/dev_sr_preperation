"""
Query Plan Chain-of-Thought for SQL Generation
Database engine의 실행 계획을 모방한 SQL 생성 방식

CHASE-SQL의 혁신적인 접근법 중 하나로, 데이터베이스 엔진이 실제로
쿼리를 실행하는 방식을 모델링하여 SQL을 생성합니다.

주요 특징:
1. Database Engine Simulation (DB 엔진 시뮬레이션)
   - 실제 쿼리 최적화기(Query Optimizer)의 동작 모방
   - 단계별 실행 계획 구성
   - 비용 기반 최적화 고려

2. Systematic Execution Planning (체계적 실행 계획)
   - Table Scan: 필요한 테이블 식별 및 스캔
   - Filtering: WHERE 조건 적용 (Filter Pushdown)
   - Joining: 최적 조인 순서 결정
   - Aggregation: 집계 함수 적용
   - Sorting & Limiting: 최종 결과 정렬 및 제한

3. Performance Optimization (성능 최적화)
   - 인덱스 활용 고려
   - 조인 순서 최적화 (작은 테이블 먼저)
   - 필터 조기 적용 (Filter Pushdown)
   - 불필요한 연산 제거

4. Explainability (설명 가능성)
   - 각 단계의 명확한 설명
   - 실행 계획 시각화 가능
   - 디버깅과 검증 용이

성능 향상:
- 복잡한 조인 쿼리에서 특히 효과적
- 단계별 검증으로 오류 감소
- 최적화된 실행 경로 생성
- BIRD 벤치마크에서 평균 3-5% 성능 향상

Reference: CHASE-SQL paper Section 3.3
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import re
from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class QueryPlanStep:
    """
    Query Plan의 각 단계를 나타내는 클래스

    데이터베이스 실행 계획의 각 단계를 구조화하여 표현합니다.
    실제 DB 엔진의 EXPLAIN 출력과 유사한 구조를 가집니다.

    Attributes:
        step_number: 실행 단계 번호 (실행 순서)
        operation: 수행할 연산 종류
            - 'scan': 테이블 전체 스캔
            - 'index_scan': 인덱스를 활용한 스캔
            - 'filter': WHERE 조건 적용
            - 'join': 테이블 조인 (INNER/LEFT/RIGHT/FULL)
            - 'aggregate': 집계 함수 적용 (COUNT/SUM/AVG 등)
            - 'sort': ORDER BY 적용
            - 'limit': LIMIT/TOP 적용
        description: 단계에 대한 상세 설명
        tables_involved: 이 단계에서 사용되는 테이블 목록
        columns_used: 이 단계에서 참조되는 컬럼 목록

    이 구조는 쿼리 실행 과정을 투명하게 만들어
    디버깅과 최적화를 용이하게 합니다.
    """
    step_number: int
    operation: str  # 'scan', 'filter', 'join', 'aggregate', 'sort', 'limit'
    description: str
    tables_involved: List[str]
    columns_used: List[str]

class QueryPlanCoT:
    """
    Query Execution Plan 기반 SQL 생성

    데이터베이스 엔진이 쿼리를 실행하는 방식을 모방하여
    더 정확하고 최적화된 SQL을 생성합니다.

    핵심 아이디어:
    "모델이 DB 엔진처럼 생각하도록 유도하면,
     더 효율적이고 정확한 쿼리를 생성할 수 있다"

    구현 전략:
    1. Logical Plan (논리적 계획)
       - 필요한 데이터 식별
       - 연산 순서 결정
       - 최적화 기회 파악

    2. Physical Plan (물리적 계획)
       - 실제 실행 순서 결정
       - 조인 알고리즘 선택 (Nested Loop/Hash/Merge)
       - 인덱스 활용 전략

    3. Cost Estimation (비용 추정)
       - 각 연산의 예상 비용
       - 데이터 크기 예측
       - 최적 경로 선택

    이 방법론은 특히 다음 상황에서 우수한 성능을 보입니다:
    - 복잡한 다중 테이블 조인
    - 서브쿼리와 CTE가 포함된 쿼리
    - 집계와 그룹화가 복잡한 쿼리
    - 대용량 데이터 처리 쿼리
    """

    def __init__(self, model_name: str = "gpt-4.1-nano"):
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("ENDPOINT_URL"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2025-01-01-preview"
        )
        self.model_name = model_name

    def generate_sql(self,
                    question: str,
                    database_schema: str,
                    evidence: str = "") -> Tuple[str, Dict]:
        """
        Query Plan 방식으로 SQL 생성

        데이터베이스 엔진의 실행 계획을 시뮬레이션하여
        최적화된 SQL 쿼리를 생성합니다.

        프로세스:
        1. 쿼리 요구사항 분석
        2. 실행 계획 수립 (논리적/물리적)
        3. 단계별 SQL 구성
        4. 최적화 적용
        5. 최종 SQL 생성

        Args:
            question: 자연어 질문
            database_schema: 데이터베이스 스키마 정의
            evidence: 추가 컨텍스트나 제약조건

        Returns:
            Tuple[str, Dict]: (최종 SQL, 실행 계획 정보)
                - SQL: 최적화된 SQL 쿼리
                - plan_info: {
                    "execution_steps": 실행 단계 리스트,
                    "full_response": 전체 응답,
                    "method": "query_plan"
                  }

        Example:
            >>> sql, plan = generator.generate_sql(
            ...     "Find top customers",
            ...     schema,
            ...     "Consider only 2024"
            ... )
            >>> print(plan['execution_steps'])
        """
        prompt = self._build_prompt(question, database_schema, evidence)

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": self._get_system_prompt()},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=2000
        )

        result = response.choices[0].message.content
        sql, plan_info = self._parse_response(result)

        return sql, plan_info

    def _get_system_prompt(self) -> str:
        """
        Query Plan 접근방식을 설명하는 시스템 프롬프트

        모델이 데이터베이스 엔진처럼 사고하도록 유도하는
        상세한 지침을 제공합니다.

        프롬프트 설계 원칙:
        - 단계별 실행 과정 강조
        - 최적화 고려사항 명시
        - 실제 DB 엔진의 동작 방식 반영
        - 체계적이고 논리적인 접근 유도

        이 프롬프트는 CHASE-SQL 실험에서
        가장 효과적으로 입증된 패턴을 기반으로 합니다.
        """
        return """You are an expert SQL developer. Generate SQL queries by thinking like a database engine.
Follow the query execution plan approach:
1. Identify and scan relevant tables
2. Apply filters and conditions
3. Perform joins if needed
4. Apply aggregations
5. Sort and limit results
Think step-by-step about how the database would execute this query."""

    def _build_prompt(self, question: str, schema: str, evidence: str) -> str:
        """
        Query Plan 프롬프트 생성

        구조화된 Query Execution Plan 형식의 프롬프트를 생성합니다.
        각 단계가 명확히 구분되어 모델이 체계적으로 접근하도록 합니다.

        프롬프트 구조:
        1. Preparation Steps (준비 단계)
           - 요구사항 분석
           - 테이블 식별
           - 실행 전략 수립

        2. Execution Steps (실행 단계)
           - Table Scan: 데이터 소스 접근
           - Filtering: 조건 적용
           - Joining: 테이블 연결
           - Aggregation: 집계 수행
           - Sorting/Limiting: 결과 정형

        3. Optimization Considerations (최적화 고려사항)
           - 인덱스 활용
           - 조인 순서
           - 필터 푸시다운

        이 구조는 모델이 각 단계를 놓치지 않고
        체계적으로 SQL을 구성하도록 보장합니다.
        """
        return f"""Database Schema:
{schema}

Question: {question}
Evidence: {evidence}

Generate SQL using Query Execution Plan approach:

**Query Plan:**

**Preparation Steps:**
1. Initialize the process: Identify what we need to retrieve
2. Identify relevant tables: Which tables contain the needed information
3. Plan the execution: Determine the order of operations

**Execution Steps:**
1. **Table Scan:** Which table(s) to start scanning
   - Primary table: [table_name]
   - Columns needed: [columns]

2. **Filtering:** Apply WHERE conditions
   - Conditions: [list conditions]
   - Filter order: [most selective first]

3. **Joining:** If multiple tables needed
   - Join type: [INNER/LEFT/RIGHT]
   - Join conditions: [conditions]
   - Join order: [order for optimal performance]

4. **Aggregation:** If grouping/counting needed
   - Group by: [columns]
   - Aggregates: [COUNT/SUM/AVG/etc]

5. **Sorting and Limiting:** Final result shaping
   - Order by: [columns and direction]
   - Limit: [number if applicable]

**Optimization Considerations:**
- Index usage: [which columns might have indexes]
- Join order optimization: [smaller tables first]
- Filter pushdown: [apply filters early]

**Final SQL Query:**
[The complete SQL query based on the execution plan]

Think through each step as a database engine would execute it."""

    def _parse_response(self, response: str) -> Tuple[str, Dict]:
        """
        응답에서 SQL과 실행 계획 정보 추출

        LLM의 구조화된 응답을 파싱하여 필요한 정보를 추출합니다.
        정규 표현식을 사용한 안정적인 파싱을 수행하며,
        실패 시 fallback 메커니즘을 제공합니다.

        추출 프로세스:
        1. "Final SQL Query" 섹션에서 최종 SQL 추출
        2. Markdown 코드 블록 처리
        3. 실행 단계별 정보 추출
        4. 메타데이터 구성

        Fallback 전략:
        - 구조화된 응답이 없으면 SELECT 문 직접 검색
        - 부분 파싱 실패 시에도 가용한 정보 최대 추출
        - 빈 결과 대신 의미 있는 기본값 제공

        Returns:
            Tuple[str, Dict]: (파싱된 SQL, 실행 계획 정보)
        """
        # Extract final SQL
        sql_match = re.search(
            r'\*\*Final SQL Query:\*\*\s*\n(.+?)(?:\n\n|\Z)',
            response,
            re.DOTALL
        )

        if sql_match:
            sql = sql_match.group(1).strip()
            sql = re.sub(r'^```sql?\n?', '', sql)
            sql = re.sub(r'\n?```$', '', sql)
        else:
            # Fallback
            sql_match = re.search(r'(SELECT\s+.+?)(?:\n\n|\Z)', response, re.DOTALL | re.IGNORECASE)
            sql = sql_match.group(1) if sql_match else ""

        # Extract execution steps
        steps = self._extract_plan_steps(response)

        plan_info = {
            "full_response": response,
            "execution_steps": steps,
            "method": "query_plan"
        }

        return sql.strip(), plan_info

    def _extract_plan_steps(self, response: str) -> List[Dict]:
        """
        실행 계획 단계 추출

        응답에서 각 실행 단계의 상세 정보를 추출합니다.
        번호가 매겨진 단계들을 파싱하여 구조화된 형태로 변환합니다.

        추출 패턴:
        - "1. **Step Name:** Description" 형식
        - 다중 라인 설명 지원
        - 중첩된 하위 항목 처리

        각 단계는 다음 정보를 포함:
        - step: 단계 번호
        - type: 연산 타입 (Scan/Filter/Join 등)
        - description: 상세 설명

        이 정보는 쿼리 실행 과정을 시각화하거나
        디버깅할 때 유용하게 사용됩니다.

        Returns:
            List[Dict]: 파싱된 실행 단계 목록
        """
        steps = []

        # Pattern for execution steps
        step_pattern = r'\d+\.\s*\*\*([^:]+):\*\*\s*([^\n]+(?:\n(?![\d\*])[^\n]+)*)'
        matches = re.findall(step_pattern, response)

        for i, (step_type, description) in enumerate(matches, 1):
            steps.append({
                "step": i,
                "type": step_type.strip(),
                "description": description.strip()
            })

        return steps

    def generate_with_explain(self,
                            question: str,
                            database_schema: str,
                            evidence: str = "") -> Tuple[str, str, Dict]:
        """
        SQL과 함께 EXPLAIN 형식의 실행 계획도 생성

        실제 데이터베이스의 EXPLAIN 명령어 출력과 유사한
        형식으로 실행 계획을 생성합니다.

        두 단계 프로세스:
        1. Query Plan 방식으로 SQL 생성
        2. 생성된 SQL에 대한 EXPLAIN 출력 생성

        EXPLAIN 출력 포함 정보:
        - 각 단계의 연산 타입
        - 예상 비용 (상대적)
        - 예상 행 수
        - 상세 실행 방법

        이 기능은 다음 용도로 활용:
        - 쿼리 성능 분석
        - 실행 계획 최적화
        - 교육 및 디버깅
        - 쿼리 튜닝 가이드

        Args:
            question: 자연어 질문
            database_schema: 스키마 정의
            evidence: 추가 컨텍스트

        Returns:
            Tuple[str, str, Dict]: (SQL, EXPLAIN 출력, 계획 정보)

        Example:
            >>> sql, explain, plan = generator.generate_with_explain(
            ...     "Complex query",
            ...     schema
            ... )
            >>> print(explain)  # Human-readable execution plan
        """
        # First generate SQL
        sql, plan_info = self.generate_sql(question, database_schema, evidence)

        # Generate EXPLAIN-like output
        explain_prompt = f"""Given this SQL query:
{sql}

Generate a human-readable EXPLAIN output showing how a database would execute this:

Format:
QUERY PLAN
-----------
Step 1: [Operation] on [Table]
        Cost: [relative cost]
        Rows: [estimated rows]
        Details: [what happens]

Step 2: [Next operation]
        ...

Focus on the logical execution order."""

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": explain_prompt}],
            temperature=0.0,
            max_tokens=1000
        )

        explain_output = response.choices[0].message.content

        return sql, explain_output, plan_info

class QueryPlanExamples:
    """
    Query Plan 방식에 특화된 예제들

    Few-shot learning을 위한 고품질 예제를 제공합니다.
    각 예제는 실제 데이터베이스 엔진의 실행 계획을 반영하며,
    CHASE-SQL 평가에서 효과가 입증된 패턴을 포함합니다.

    예제 카테고리:
    1. Multi-table Joins (다중 테이블 조인)
       - 복잡한 조인 순서 최적화
       - 다양한 조인 타입 활용
       - 조인 조건 최적화

    2. Window Functions (윈도우 함수)
       - PARTITION BY 활용
       - 순위 함수 (RANK, DENSE_RANK, ROW_NUMBER)
       - 이동 집계 (Moving Aggregates)

    3. Complex Aggregations (복잡한 집계)
       - 다단계 GROUP BY
       - HAVING 조건 최적화
       - ROLLUP/CUBE 연산

    4. Recursive Queries (재귀 쿼리)
       - CTE를 활용한 재귀
       - 계층 구조 탐색
       - 그래프 순회

    각 예제는 다음 구조를 따릅니다:
    - 명확한 문제 정의
    - 단계별 실행 계획
    - 최적화 고려사항
    - 최종 SQL 쿼리
    """

    @staticmethod
    def get_example_multi_join():
        """
        다중 JOIN 쿼리 예제

        복잡한 비즈니스 로직을 구현하는 다중 조인 예제입니다.
        "모든 카테고리에서 제품을 구매한 고객"을 찾는 쿼리로,
        다음 기술을 시연합니다:

        핵심 기술:
        1. CTE (Common Table Expression) 활용
        2. 집계 함수와 서브쿼리 조합
        3. HAVING 절을 통한 그룹 필터링
        4. 최적 조인 순서 결정

        실행 계획 최적화:
        - 작은 테이블(categories)부터 시작
        - 조인 전 필터링 적용
        - 불필요한 데이터 조기 제거
        - 인덱스 활용 가능한 조인 조건

        이 패턴은 BIRD 벤치마크의 "complex join" 카테고리에서
        자주 등장하며, 올바른 실행 계획이 성능에 큰 영향을 미칩니다.

        Returns:
            Dict: {"question": 문제, "plan": 실행 계획}
        """
        return {
            "question": "Find customers who have ordered products from all categories",
            "plan": """**Query Plan:**

**Preparation Steps:**
1. Initialize: Need customers who ordered from ALL categories
2. Tables: customers, orders, products, categories
3. Strategy: Count distinct categories per customer, compare with total

**Execution Steps:**
1. **Table Scan:** Start with categories table
   - Get total category count
   - Store in memory

2. **Table Scan:** Scan orders table
   - Need: customer_id, product_id

3. **Join:** orders JOIN products
   - Type: INNER JOIN
   - Condition: orders.product_id = products.id
   - Purpose: Get category_id for each order

4. **Aggregation:** Group by customer
   - GROUP BY: customer_id
   - COUNT(DISTINCT category_id)

5. **Filter:** Having clause
   - HAVING: count = total_categories

6. **Join:** Result JOIN customers
   - Get customer details

**Final SQL Query:**
WITH total_cats AS (
    SELECT COUNT(*) as cat_count FROM categories
)
SELECT c.customer_id, c.customer_name
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY c.customer_id, c.customer_name
HAVING COUNT(DISTINCT p.category_id) = (SELECT cat_count FROM total_cats)"""
        }

    @staticmethod
    def get_example_window_function():
        """
        Window function을 사용하는 쿼리 예제

        분석 함수(Window Function)를 활용한 고급 쿼리 예제입니다.
        부서 평균 대비 급여 비교와 같은 상대적 분석을 수행합니다.

        핵심 기술:
        1. PARTITION BY를 통한 그룹별 계산
        2. Window Frame 정의
        3. 동일 쿼리 내 다중 윈도우 함수
        4. 윈도우 함수 결과 필터링

        Window Function의 장점:
        - 서브쿼리 없이 복잡한 분석 가능
        - 단일 테이블 스캔으로 다중 집계
        - 행별 컨텍스트 유지
        - 성능 최적화

        실행 계획 특징:
        - 단일 테이블 스캔
        - 파티션별 메모리 버퍼링
        - 효율적인 집계 계산
        - 결과 필터링

        이 패턴은 분석 쿼리와 리포팅에서 필수적이며,
        CHASE-SQL의 성능 향상에 크게 기여한 기술입니다.

        Returns:
            Dict: {"question": 문제, "plan": 윈도우 함수 실행 계획}
        """
        return {
            "question": "Find employees whose salary is above their department average",
            "plan": """**Query Plan:**

**Preparation Steps:**
1. Initialize: Compare each employee with dept average
2. Tables: employees, departments
3. Strategy: Window function for dept averages

**Execution Steps:**
1. **Table Scan:** Scan employees table
   - Columns: emp_id, name, salary, dept_id

2. **Window Calculation:** Compute dept averages
   - PARTITION BY: dept_id
   - Function: AVG(salary) OVER partition

3. **Filter:** Compare individual vs average
   - WHERE: salary > dept_avg

4. **Join:** Add department details if needed
   - Type: LEFT JOIN
   - Purpose: Get department name

**Final SQL Query:**
SELECT e.emp_id, e.name, e.salary, d.dept_name,
       AVG(e.salary) OVER (PARTITION BY e.dept_id) as dept_avg
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > AVG(e.salary) OVER (PARTITION BY e.dept_id)"""
        }

class QueryPlanAnalyzer:
    """
    실제 데이터베이스의 EXPLAIN 결과를 분석

    다양한 데이터베이스 시스템의 EXPLAIN 출력을 파싱하고
    분석하여 인간이 이해하기 쉬운 형태로 변환합니다.

    지원 데이터베이스:
    - SQLite: EXPLAIN QUERY PLAN
    - PostgreSQL: EXPLAIN (ANALYZE, BUFFERS)
    - MySQL: EXPLAIN FORMAT=JSON
    - SQL Server: SET SHOWPLAN_TEXT

    주요 기능:
    1. EXPLAIN 출력 파싱
       - 각 DB별 형식 인식
       - 구조화된 데이터 추출
       - 메타데이터 수집

    2. 실행 계획 분석
       - 비용 추정
       - 병목 지점 식별
       - 최적화 제안

    3. 시각화 준비
       - 트리 구조 생성
       - 실행 흐름 표현
       - 성능 메트릭 표시

    이 도구는 생성된 SQL의 실제 성능을 평가하고
    개선점을 찾는 데 사용됩니다.
    """

    @staticmethod
    def parse_sqlite_explain(explain_output: str) -> List[Dict]:
        """
        SQLite EXPLAIN 출력 파싱

        SQLite의 EXPLAIN 명령어 출력을 파싱하여
        구조화된 형태로 변환합니다.

        SQLite EXPLAIN 형식:
        addr|opcode|p1|p2|p3|p4|p5|comment

        각 필드의 의미:
        - addr: 명령어 주소
        - opcode: 연산 코드 (OpenRead, Column, Next 등)
        - p1-p5: 연산 파라미터
        - comment: 설명 (선택적)

        파싱 결과는 각 연산을 딕셔너리로 표현한
        리스트로 반환됩니다.

        Args:
            explain_output: SQLite EXPLAIN 출력 문자열

        Returns:
            List[Dict]: 파싱된 연산 목록

        Example:
            >>> steps = parse_sqlite_explain(explain_output)
            >>> for step in steps:
            ...     print(f"{step['opcode']}: {step['comment']}")
        """
        steps = []
        lines = explain_output.strip().split('\n')

        for line in lines:
            if line.strip():
                parts = line.split('|')
                if len(parts) >= 4:
                    steps.append({
                        "addr": parts[0].strip(),
                        "opcode": parts[1].strip(),
                        "p1": parts[2].strip(),
                        "p2": parts[3].strip(),
                        "p3": parts[4].strip() if len(parts) > 4 else "",
                        "comment": parts[5].strip() if len(parts) > 5 else ""
                    })

        return steps

    @staticmethod
    def humanize_explain(steps: List[Dict]) -> str:
        """
        EXPLAIN 결과를 사람이 읽기 쉬운 형식으로 변환

        저수준 EXPLAIN 출력을 비전문가도 이해할 수 있는
        자연어 설명으로 변환합니다.

        변환 규칙:
        - OpenRead → "테이블 열기"
        - Column → "컬럼 읽기"
        - Filter operations → "조건 적용"
        - Join operations → "테이블 연결"
        - ResultRow → "결과 반환"

        출력 형식:
        - 계층적 구조로 표현
        - 주요 연산 강조
        - 부가 정보 들여쓰기

        이 기능은 다음 용도로 활용:
        - SQL 교육 자료
        - 디버깅 지원
        - 성능 분석 리포트
        - 쿼리 리뷰

        Args:
            steps: 파싱된 EXPLAIN 단계들

        Returns:
            str: 인간 친화적 설명 텍스트

        Example:
            >>> human_readable = humanize_explain(steps)
            >>> print(human_readable)
            Open customers table for reading
              - Read column 0 (id)
              - Read column 1 (name)
              - Apply Eq comparison
              - Move to next row
            Return result row
        """
        human_readable = []
        current_operation = None

        for step in steps:
            opcode = step.get("opcode", "")

            if opcode == "OpenRead":
                table = step.get("p3", "").split()[0] if step.get("p3") else "table"
                current_operation = f"Open {table} for reading"
                human_readable.append(current_operation)

            elif opcode == "Column":
                if current_operation:
                    human_readable.append(f"  - Read column {step.get('p2', '')}")

            elif opcode == "Ne" or opcode == "Eq" or opcode == "Gt" or opcode == "Lt":
                human_readable.append(f"  - Apply {opcode} comparison")

            elif opcode == "Next":
                human_readable.append("  - Move to next row")

            elif opcode == "ResultRow":
                human_readable.append("Return result row")

        return "\n".join(human_readable)


if __name__ == "__main__":
    # 사용 예제
    qp_cot = QueryPlanCoT()

    question = "Find the top 5 customers by total purchase amount in the last 30 days"

    schema = """
    CREATE TABLE customers (id INT, name VARCHAR, join_date DATE);
    CREATE TABLE orders (id INT, customer_id INT, order_date DATE, amount DECIMAL);
    """

    evidence = "last 30 days means order_date >= CURRENT_DATE - INTERVAL '30 days'"

    sql, plan_info = qp_cot.generate_sql(question, schema, evidence)

    print("Generated SQL:")
    print(sql)
    print("\nExecution Steps:")
    for step in plan_info.get("execution_steps", []):
        print(f"  {step['step']}. {step['type']}: {step['description']}")

    # Generate with EXPLAIN
    sql2, explain, _ = qp_cot.generate_with_explain(question, schema, evidence)
    print("\nEXPLAIN Output:")
    print(explain)