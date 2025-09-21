"""
Divide-and-Conquer Chain-of-Thought for Complex SQL Generation
복잡한 쿼리를 sub-queries로 분해하여 해결하는 방식
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
    """Sub-query 정보를 담는 클래스"""
    question: str
    pseudo_sql: str
    actual_sql: Optional[str] = None
    dependency: Optional[str] = None  # 의존하는 다른 sub-query

class DivideConquerCoT:
    """
    Divide-and-Conquer 방식의 SQL 생성
    1. Divide: 복잡한 질문을 sub-questions으로 분해
    2. Conquer: 각 sub-question을 해결
    3. Assemble: 최종 SQL 조합
    4. Optimize: 중복 제거 및 최적화
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
        단일 LLM 호출로 전체 프로세스 수행
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
        """Divide-and-Conquer 프롬프트 생성"""
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
        """응답에서 SQL과 프로세스 정보 추출"""
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
        Few-shot examples를 포함한 생성
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
    """Divide-and-Conquer에 특화된 예제들"""

    @staticmethod
    def get_example_complex_aggregation():
        """복잡한 집계 쿼리 예제"""
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
        """중첩 조건 쿼리 예제"""
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