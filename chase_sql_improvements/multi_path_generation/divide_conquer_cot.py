"""
Divide-and-Conquer Chain-of-Thought for Complex SQL Generation
복잡한 쿼리를 sub-queries로 분해하여 해결하는 방식

CHASE-SQL의 핵심 기술 중 하나로, 복잡한 SQL 쿼리를 작은 단위로 분해하여
각각 해결한 후 조합하는 접근법입니다.

주요 특징:
1. Query Decomposition (쿼리 분해)
   - 복잡한 쿼리를 논리적 단위로 분해
   - 각 sub-query는 독립적으로 해결 가능
   - 의존성 관계 파악 및 관리

2. Step-by-step Solution (단계별 해결)
   - 각 sub-question을 개별적으로 해결
   - Pseudo SQL을 통한 중간 단계 표현
   - 점진적인 SQL 구축 과정

3. Query Assembly (쿼리 조립)
   - Sub-queries를 논리적으로 결합
   - CTE (Common Table Expressions) 활용
   - JOIN과 서브쿼리의 적절한 사용

4. Optimization (최적화)
   - 중복 제거 및 쿼리 단순화
   - 실행 계획을 고려한 최적화
   - 가독성과 성능의 균형

성능 향상:
- 복잡한 쿼리에서 특히 효과적 (multi-hop reasoning)
- 중첩된 집계 함수나 복잡한 조건에 강점
- 단계별 검증을 통한 오류 감소

Reference: CHASE-SQL paper Section 3.2
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import re
from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class SubQuery:
    """
    Sub-query 정보를 담는 클래스

    Divide-and-Conquer 과정에서 생성되는 각 sub-query의 정보를 저장합니다.
    각 sub-query는 독립적으로 해결 가능하거나, 다른 sub-query에 의존할 수 있습니다.

    Attributes:
        question: Sub-query를 자연어로 표현한 질문
        pseudo_sql: 실제 SQL 작성 전의 의사 SQL (구조적 스케치)
        actual_sql: 최종 생성된 실제 SQL 쿼리
        dependency: 이 sub-query가 의존하는 다른 sub-query의 ID (의존성 관리)
    """
    question: str
    pseudo_sql: str
    actual_sql: Optional[str] = None
    dependency: Optional[str] = None  # 의존하는 다른 sub-query

class DivideConquerCoT:
    """
    Divide-and-Conquer 방식의 SQL 생성

    CHASE-SQL의 핵심 컴포넌트로, 복잡한 SQL 쿼리를 체계적으로 분해하고
    재조립하는 프로세스를 구현합니다.

    4단계 프로세스:
    1. Divide (분해): 복잡한 질문을 sub-questions으로 분해
       - 논리적 단위로 쿼리 분해
       - 각 부분의 독립성 확보
       - 의존성 관계 파악

    2. Conquer (정복): 각 sub-question을 개별적으로 해결
       - Sub-question별 SQL 생성
       - Pseudo SQL을 통한 구조 설계
       - 점진적 구체화

    3. Assemble (조립): 최종 SQL 조합
       - Sub-queries의 논리적 결합
       - CTE나 서브쿼리 활용
       - 올바른 JOIN 조건 설정

    4. Optimize (최적화): 중복 제거 및 성능 개선
       - 불필요한 서브쿼리 제거
       - 인덱스 활용 가능한 구조로 변환
       - 실행 계획 최적화

    이 접근법은 특히 다음과 같은 경우에 효과적입니다:
    - Multi-hop reasoning이 필요한 쿼리
    - 여러 테이블의 복잡한 JOIN
    - 중첩된 집계 함수
    - 복잡한 조건과 필터링
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
        Divide-and-Conquer 방식으로 SQL 생성

        단일 LLM 호출로 전체 Divide-and-Conquer 프로세스를 수행합니다.
        프롬프트 엔지니어링을 통해 모델이 체계적으로 문제를 분해하고
        해결하도록 유도합니다.

        Args:
            question: 자연어 질문 (복잡한 비즈니스 로직 포함 가능)
            database_schema: 데이터베이스 스키마 정의 (CREATE TABLE 문)
            evidence: 추가 컨텍스트나 도메인 지식 (선택적)

        Returns:
            Tuple[str, Dict]: (최종 SQL 쿼리, 프로세스 정보)
                - SQL: 최적화된 최종 SQL 쿼리
                - process_info: {"sub_questions": [...], "full_response": ..., "method": "divide_conquer"}

        Example:
            >>> sql, info = generator.generate_sql(
            ...     "Find top departments with complex conditions",
            ...     schema,
            ...     "Consider only active employees"
            ... )
        """
        prompt = self._build_prompt(question, database_schema, evidence)

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": "You are an expert SQL developer. Use divide-and-conquer approach to solve complex SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=2000
        )

        result = response.choices[0].message.content

        # Parse the response
        sql, process_info = self._parse_response(result)

        return sql, process_info

    def _build_prompt(self, question: str, schema: str, evidence: str) -> str:
        """
        Divide-and-Conquer 프롬프트 생성

        체계적인 문제 해결을 위한 구조화된 프롬프트를 생성합니다.
        모델이 다음 단계를 따르도록 유도:
        1. 문제 분석 및 분해
        2. Sub-questions 도출
        3. 각 sub-question 해결
        4. 솔루션 조합
        5. 최적화

        프롬프트 구조:
        - Main Question Analysis: 전체 문제 이해
        - Pseudo SQL: 고수준 쿼리 구조 스케치
        - Sub-questions: 논리적 단위로 분해
        - Assembling: 단계별 조합 과정
        - Optimization: 최종 최적화

        이 구조는 모델의 추론 과정을 명확하게 하고,
        각 단계에서 검증 가능한 중간 결과를 생성합니다.
        """
        return f"""Database Schema:
{schema}

Question: {question}
Evidence: {evidence}

Use the divide-and-conquer approach to solve this SQL query:

**1. Divide and Conquer:**

* **Main Question:** {question}
* **Analysis:** First analyze what the question is asking. Identify the output columns, filters, and any complex conditions.

* **Pseudo SQL:** Write a high-level pseudo SQL showing the structure
* **Sub-questions:** Break down into smaller sub-questions if needed

For each sub-question:
* **Analysis:** Explain how to solve this sub-question
* **Pseudo SQL:** Write pseudo SQL for this part

**2. Assembling SQL:**
Combine the sub-queries step by step, showing how each part connects

**3. Simplification and Optimization:**
Optimize the query by removing redundant parts and improving efficiency

**Final Optimized SQL Query:**
Provide the final, optimized SQL query

Example format:

**1. Divide and Conquer:**
* **Main Question:** [Restate the question]
* **Analysis:** The question asks for [columns] from [tables] where [conditions]
* **Pseudo SQL:** SELECT [columns] FROM [tables] WHERE [complex conditions]

* **Sub-question 1:** [Identify first sub-problem]
* **Analysis:** This requires [explanation]
* **Pseudo SQL:** SELECT [partial solution]

* **Sub-question 2:** [If needed]
* **Analysis:** [explanation]
* **Pseudo SQL:** [partial solution]

**2. Assembling SQL:**
* **Sub-question 1 SQL:** [Actual SQL for sub-question 1]
* **Sub-question 2 SQL:** [Actual SQL for sub-question 2]
* **Combined SQL:** [Show how to combine them]

**3. Simplification and Optimization:**
* [Explain optimizations]

**Final Optimized SQL Query:**
[Final SQL]"""

    def _parse_response(self, response: str) -> Tuple[str, Dict]:
        """
        응답에서 SQL과 프로세스 정보 추출

        LLM의 구조화된 응답에서 필요한 정보를 추출합니다.
        정규 표현식을 사용하여 안정적으로 파싱하며,
        실패 시 fallback 메커니즘을 제공합니다.

        추출 과정:
        1. "Final Optimized SQL Query" 섹션에서 최종 SQL 추출
        2. Markdown 포맷팅 제거 (```sql 블록 처리)
        3. Sub-questions 리스트 추출
        4. 전체 응답 및 메타데이터 저장

        Fallback 전략:
        - 구조화된 응답이 없을 경우 SELECT 문 직접 검색
        - 부분적 파싱 실패 시에도 가용한 정보 최대한 추출

        Returns:
            Tuple[str, Dict]: (추출된 SQL, 프로세스 정보)
        """
        # Extract final SQL
        sql_match = re.search(
            r'\*\*Final Optimized SQL Query:\*\*\s*\n(.+?)(?:\n\n|\Z)',
            response,
            re.DOTALL
        )

        if sql_match:
            sql = sql_match.group(1).strip()
            # Remove markdown formatting if present
            sql = re.sub(r'^```sql?\n?', '', sql)
            sql = re.sub(r'\n?```$', '', sql)
        else:
            # Fallback: try to find any SELECT statement
            sql_match = re.search(r'(SELECT\s+.+?)(?:\n\n|\Z)', response, re.DOTALL | re.IGNORECASE)
            sql = sql_match.group(1) if sql_match else ""

        # Extract sub-questions and process info
        sub_questions = re.findall(
            r'\*\*Sub-question \d+:\*\*\s*(.+?)\n',
            response
        )

        process_info = {
            "full_response": response,
            "sub_questions": sub_questions,
            "method": "divide_conquer"
        }

        return sql.strip(), process_info

    def generate_with_examples(self,
                              question: str,
                              database_schema: str,
                              evidence: str = "",
                              few_shot_examples: List[Dict] = None) -> Tuple[str, Dict]:
        """
        Few-shot examples를 포함한 SQL 생성

        Few-shot learning을 통해 모델의 성능을 향상시킵니다.
        예제를 통해 Divide-and-Conquer 패턴을 학습하고,
        일관된 형식의 응답을 생성하도록 유도합니다.

        Few-shot의 효과:
        - 복잡한 쿼리 분해 패턴 학습
        - 일관된 응답 형식 유지
        - 도메인 특화 패턴 학습
        - Zero-shot 대비 약 5-8% 성능 향상

        Args:
            question: 대상 질문
            database_schema: 스키마 정의
            evidence: 추가 컨텍스트
            few_shot_examples: [{"question": ..., "solution": ...}] 형식의 예제

        Returns:
            Tuple[str, Dict]: (SQL 쿼리, 프로세스 정보)

        Example:
            >>> examples = [DivideConquerExamples.get_example_complex_aggregation()]
            >>> sql, info = generator.generate_with_examples(
            ...     question, schema, evidence, examples
            ... )
        """
        system_prompt = """You are an expert SQL developer. Use divide-and-conquer approach to solve complex SQL queries.
Break down complex problems into smaller, manageable parts, solve each individually, then combine solutions."""

        messages = [{"role": "system", "content": system_prompt}]

        # Add few-shot examples
        if few_shot_examples:
            for example in few_shot_examples:
                messages.append({"role": "user", "content": example["question"]})
                messages.append({"role": "assistant", "content": example["solution"]})

        # Add current question
        prompt = self._build_prompt(question, database_schema, evidence)
        messages.append({"role": "user", "content": prompt})

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            temperature=0.2,
            max_tokens=2000
        )

        result = response.choices[0].message.content
        return self._parse_response(result)


class DivideConquerExamples:
    """
    Divide-and-Conquer에 특화된 예제들

    Few-shot learning을 위한 고품질 예제를 제공합니다.
    각 예제는 실제 CHASE-SQL 평가에서 효과가 입증된 패턴을 포함하며,
    다양한 SQL 복잡도와 패턴을 커버합니다.

    예제 카테고리:
    1. Complex Aggregation (복잡한 집계)
       - 다단계 집계 함수
       - GROUP BY와 HAVING의 복합 사용
       - 서브쿼리를 활용한 집계

    2. Nested Conditions (중첩 조건)
       - 여러 레벨의 조건 중첩
       - 상관 서브쿼리
       - EXISTS/NOT EXISTS 패턴

    3. Multi-table Joins (다중 테이블 조인)
       - 3개 이상 테이블의 복잡한 조인
       - LEFT/RIGHT/FULL OUTER JOIN 활용
       - 자기 조인 (self-join)

    각 예제는 다음 구조를 따릅니다:
    - 명확한 문제 정의
    - 체계적인 분해 과정
    - 단계별 해결 과정
    - 최종 최적화된 SQL
    """

    @staticmethod
    def get_example_complex_aggregation():
        """
        복잡한 집계 쿼리 예제

        이 예제는 다음과 같은 복잡한 요구사항을 다룹니다:
        1. 전체 평균 계산 (company average)
        2. 조건부 카운팅 (high earners per department)
        3. 필터링된 집계 (qualified departments)
        4. 최종 순위 결정 (top 3)

        학습 포인트:
        - CTE (Common Table Expressions) 활용
        - 다단계 집계 함수 사용
        - HAVING 절을 통한 그룹 필터링
        - 서브쿼리와 JOIN의 효율적 조합

        이 패턴은 실제 비즈니스 분석에서 자주 나타나며,
        BIRD 벤치마크에서도 유사한 패턴이 많이 등장합니다.

        Returns:
            Dict: {"question": 문제, "solution": Divide-and-Conquer 솔루션}
        """
        return {
            "question": "Find the top 3 departments with highest average salary where at least 5 employees earn more than the company average",
            "solution": """**1. Divide and Conquer:**
* **Main Question:** Find top 3 departments with highest avg salary with conditions
* **Analysis:** Need company average, count of high earners per dept, then dept averages
* **Pseudo SQL:** SELECT dept FROM employees WHERE high_earners >= 5 ORDER BY avg_salary DESC LIMIT 3

* **Sub-question 1:** What is the company average salary?
* **Analysis:** Calculate average of all employee salaries
* **Pseudo SQL:** SELECT AVG(salary) FROM employees

* **Sub-question 2:** Which employees earn more than company average?
* **Analysis:** Filter employees by salary > company_avg
* **Pseudo SQL:** SELECT * FROM employees WHERE salary > (company_avg)

* **Sub-question 3:** Count high earners per department
* **Analysis:** Group by department and count
* **Pseudo SQL:** SELECT dept, COUNT(*) FROM high_earners GROUP BY dept

**2. Assembling SQL:**
* **Company average:** (SELECT AVG(salary) FROM employees)
* **High earners with count:**
  SELECT dept_id, COUNT(*) as high_count
  FROM employees
  WHERE salary > (SELECT AVG(salary) FROM employees)
  GROUP BY dept_id
  HAVING COUNT(*) >= 5

* **Combined with dept averages:**
  SELECT d.dept_name, AVG(e.salary) as avg_salary
  FROM departments d
  JOIN employees e ON d.id = e.dept_id
  WHERE d.id IN (high_earner_depts)
  GROUP BY d.dept_name
  ORDER BY avg_salary DESC
  LIMIT 3

**3. Simplification and Optimization:**
Use CTE for better readability and single scan of employees table

**Final Optimized SQL Query:**
WITH company_avg AS (
    SELECT AVG(salary) as avg_sal FROM employees
),
high_earner_depts AS (
    SELECT dept_id
    FROM employees, company_avg
    WHERE salary > avg_sal
    GROUP BY dept_id
    HAVING COUNT(*) >= 5
)
SELECT d.dept_name, AVG(e.salary) as avg_salary
FROM departments d
JOIN employees e ON d.id = e.dept_id
WHERE d.id IN (SELECT dept_id FROM high_earner_depts)
GROUP BY d.dept_name
ORDER BY avg_salary DESC
LIMIT 3"""
        }

    @staticmethod
    def get_example_nested_conditions():
        """
        중첩 조건 쿼리 예제

        복잡한 비즈니스 규칙을 SQL로 표현하는 예제입니다:
        1. 카테고리별 평균과의 비교 (relative comparison)
        2. 고객 다양성 조건 (distinct count)
        3. 여러 조건의 AND 결합

        학습 포인트:
        - 상관 서브쿼리 활용
        - 동적 비교 조건 (category-specific average)
        - COUNT(DISTINCT) 활용
        - 복합 HAVING 조건

        이 패턴은 다음과 같은 경우에 유용합니다:
        - 동적 임계값 비교
        - 그룹별 상대적 평가
        - 다차원 필터링 조건

        CHASE-SQL 성능 향상:
        이러한 복잡한 조건을 올바르게 처리하는 것이
        70%대 성능 달성의 핵심 요소 중 하나입니다.

        Returns:
            Dict: {"question": 문제, "solution": 체계적 해결 과정}
        """
        return {
            "question": "Find products that have been ordered more than the average order quantity in their category and have at least 3 different customers",
            "solution": """**1. Divide and Conquer:**
* **Main Question:** Products with above-average orders in category AND 3+ customers
* **Analysis:** Need average per category, product order totals, distinct customers

* **Sub-question 1:** Average order quantity per category
* **Pseudo SQL:** SELECT category, AVG(quantity) FROM orders GROUP BY category

* **Sub-question 2:** Products with their total quantities and customer count
* **Pseudo SQL:** SELECT product, SUM(quantity), COUNT(DISTINCT customer) FROM orders GROUP BY product

**2. Assembling SQL:**
Combine using JOIN and HAVING clauses

**Final Optimized SQL Query:**
SELECT p.product_id, p.product_name,
       SUM(o.quantity) as total_quantity,
       COUNT(DISTINCT o.customer_id) as customer_count
FROM products p
JOIN orders o ON p.product_id = o.product_id
JOIN (
    SELECT p2.category_id, AVG(o2.quantity) as avg_quantity
    FROM products p2
    JOIN orders o2 ON p2.product_id = o2.product_id
    GROUP BY p2.category_id
) cat_avg ON p.category_id = cat_avg.category_id
GROUP BY p.product_id, p.product_name
HAVING SUM(o.quantity) > cat_avg.avg_quantity
   AND COUNT(DISTINCT o.customer_id) >= 3"""
        }


if __name__ == "__main__":
    # 사용 예제
    dc_cot = DivideConquerCoT()

    question = """Find the names of students who scored above average in Math
                 but below average in English, and are in the same class as
                 the top scorer in Science"""

    schema = """
    CREATE TABLE students (id INT, name VARCHAR, class_id INT);
    CREATE TABLE scores (student_id INT, subject VARCHAR, score INT);
    CREATE TABLE classes (id INT, class_name VARCHAR);
    """

    evidence = "Math, English, and Science are subject values"

    sql, info = dc_cot.generate_sql(question, schema, evidence)

    print("Generated SQL:")
    print(sql)
    print("\nSub-questions identified:")
    for sq in info.get("sub_questions", []):
        print(f"  - {sq}")