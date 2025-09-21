"""
Query Plan Chain-of-Thought for SQL Generation
Database engine의 실행 계획을 모방한 SQL 생성 방식
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
    """Query Plan의 각 단계를 나타내는 클래스"""
    step_number: int
    operation: str  # 'scan', 'filter', 'join', 'aggregate', 'sort', 'limit'
    description: str
    tables_involved: List[str]
    columns_used: List[str]

class QueryPlanCoT:
    """
    Query Execution Plan 기반 SQL 생성
    데이터베이스 엔진이 쿼리를 실행하는 방식을 모방
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
        """Query Plan 접근방식을 설명하는 시스템 프롬프트"""
        return """You are an expert SQL developer. Generate SQL queries by thinking like a database engine.
Follow the query execution plan approach:
1. Identify and scan relevant tables
2. Apply filters and conditions
3. Perform joins if needed
4. Apply aggregations
5. Sort and limit results
Think step-by-step about how the database would execute this query."""

    def _build_prompt(self, question: str, schema: str, evidence: str) -> str:
        """Query Plan 프롬프트 생성"""
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
        """응답에서 SQL과 실행 계획 정보 추출"""
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
        """실행 계획 단계 추출"""
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
    """Query Plan 방식에 특화된 예제들"""

    @staticmethod
    def get_example_multi_join():
        """다중 JOIN 쿼리 예제"""
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
        """Window function을 사용하는 쿼리 예제"""
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
    """실제 데이터베이스의 EXPLAIN 결과를 분석"""

    @staticmethod
    def parse_sqlite_explain(explain_output: str) -> List[Dict]:
        """SQLite EXPLAIN 출력 파싱"""
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
        """EXPLAIN 결과를 사람이 읽기 쉬운 형식으로 변환"""
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