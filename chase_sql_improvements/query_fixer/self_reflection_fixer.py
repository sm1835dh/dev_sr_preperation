"""
Self-Reflection based Query Fixer
Syntax error 및 empty result를 self-reflection으로 수정
"""

from typing import Optional, Dict, List
from dataclasses import dataclass
import re
import sqlglot
from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class FixAttempt:
    """수정 시도 정보"""
    attempt_number: int
    original_query: str
    fixed_query: str
    error_type: str
    error_message: str
    success: bool

class SelfReflectionFixer:
    """
    Self-reflection 기반 Query Fixer
    - Syntax error 감지 및 수정
    - Empty result 처리
    - 최대 3회 반복 수정 (β=3)
    """

    def __init__(self, model_name: str = "gpt-4.1-nano", max_attempts: int = 3):
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("ENDPOINT_URL"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2025-01-01-preview"
        )
        self.model_name = model_name
        self.max_attempts = max_attempts

    def fix_query(self,
                 query: str,
                 schema: str,
                 question: str,
                 evidence: str = "",
                 execution_result: Optional[str] = None) -> str:
        """
        Query를 수정하는 메인 메소드
        """
        attempts = []
        current_query = query

        for attempt_num in range(1, self.max_attempts + 1):
            # Check for errors
            error_type, error_msg = self._check_query_errors(current_query, execution_result)

            if error_type is None:
                # No errors detected
                return current_query

            # Attempt to fix
            fixed_query = self._reflect_and_fix(
                current_query, schema, question, evidence,
                error_type, error_msg, attempts
            )

            attempts.append(FixAttempt(
                attempt_number=attempt_num,
                original_query=current_query,
                fixed_query=fixed_query,
                error_type=error_type,
                error_message=error_msg,
                success=(fixed_query != current_query)
            ))

            # Check if fix was successful
            new_error_type, _ = self._check_query_errors(fixed_query, None)
            if new_error_type is None:
                return fixed_query

            current_query = fixed_query

        # Return best attempt if max attempts reached
        return current_query

    def _check_query_errors(self,
                           query: str,
                           execution_result: Optional[str]) -> tuple[Optional[str], str]:
        """
        쿼리 에러 체크
        Returns: (error_type, error_message)
        """
        # 1. Syntax validation using sqlglot
        try:
            parsed = sqlglot.parse_one(query, dialect="sqlite")
            if parsed is None:
                return "syntax", "Failed to parse SQL query"
        except Exception as e:
            return "syntax", str(e)

        # 2. Check for common issues
        query_upper = query.upper()

        # Missing FROM clause
        if 'SELECT' in query_upper and 'FROM' not in query_upper:
            return "missing_clause", "Missing FROM clause"

        # Unbalanced parentheses
        if query.count('(') != query.count(')'):
            return "syntax", "Unbalanced parentheses"

        # 3. Check execution result
        if execution_result is not None:
            if execution_result == "[]" or execution_result == "empty":
                return "empty_result", "Query returned no results"
            elif "error" in execution_result.lower():
                return "execution_error", execution_result

        return None, ""

    def _reflect_and_fix(self,
                        query: str,
                        schema: str,
                        question: str,
                        evidence: str,
                        error_type: str,
                        error_msg: str,
                        previous_attempts: List[FixAttempt]) -> str:
        """
        Self-reflection을 통한 쿼리 수정
        """
        prompt = self._build_reflection_prompt(
            query, schema, question, evidence,
            error_type, error_msg, previous_attempts
        )

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[
                {"role": "system", "content": "You are an expert SQL debugger. Fix SQL queries based on errors."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            max_tokens=1000
        )

        fixed_sql = self._extract_sql_from_response(response.choices[0].message.content)
        return fixed_sql if fixed_sql else query

    def _build_reflection_prompt(self,
                                query: str,
                                schema: str,
                                question: str,
                                evidence: str,
                                error_type: str,
                                error_msg: str,
                                previous_attempts: List[FixAttempt]) -> str:
        """Self-reflection 프롬프트 생성"""
        prompt = f"""Fix the SQL query based on the error encountered.

Database Schema:
{schema}

Original Question: {question}
Evidence: {evidence}

Current SQL Query:
{query}

Error Type: {error_type}
Error Message: {error_msg}

"""

        if previous_attempts:
            prompt += "Previous Fix Attempts:\n"
            for attempt in previous_attempts:
                prompt += f"Attempt {attempt.attempt_number}: {attempt.error_type} - "
                prompt += f"{'Fixed' if attempt.success else 'Failed'}\n"

        prompt += """
Analyze the error and provide a corrected SQL query.
Consider:
1. Table and column names must match the schema exactly
2. JOIN conditions must be properly specified
3. WHERE conditions should filter appropriately
4. GROUP BY must include all non-aggregated columns in SELECT
5. Check for typos in table/column names

If the error is "empty_result", consider:
- The WHERE conditions might be too restrictive
- JOINs might be filtering out all rows
- The data might not exist for the given conditions

Provide ONLY the corrected SQL query without explanation:"""

        return prompt

    def _extract_sql_from_response(self, response: str) -> Optional[str]:
        """응답에서 SQL 추출"""
        # Remove markdown if present
        response = re.sub(r'^```sql?\n?', '', response.strip())
        response = re.sub(r'\n?```$', '', response)

        # Find SELECT statement
        sql_match = re.search(r'(SELECT\s+.+?)(?:\n\n|\Z)', response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()

        # Return full response if it looks like SQL
        if response.upper().startswith('SELECT'):
            return response.strip()

        return None

    def batch_fix(self, queries: List[str], schema: str, question: str) -> List[str]:
        """여러 쿼리를 배치로 수정"""
        fixed_queries = []
        for query in queries:
            fixed = self.fix_query(query, schema, question)
            fixed_queries.append(fixed)
        return fixed_queries

class ErrorPatternAnalyzer:
    """
    에러 패턴 분석 및 일반적인 수정 방법 제공
    """

    COMMON_FIXES = {
        'missing_table_alias': {
            'pattern': r'ambiguous column',
            'fix': 'Add table aliases to ambiguous columns'
        },
        'invalid_group_by': {
            'pattern': r'not in GROUP BY',
            'fix': 'Add all non-aggregated columns to GROUP BY'
        },
        'missing_join_condition': {
            'pattern': r'missing join condition',
            'fix': 'Add ON clause to JOIN'
        },
        'type_mismatch': {
            'pattern': r'type mismatch',
            'fix': 'Cast values to matching types'
        }
    }

    @classmethod
    def suggest_fix(cls, error_message: str) -> Optional[str]:
        """에러 메시지 기반 수정 제안"""
        error_lower = error_message.lower()

        for fix_type, fix_info in cls.COMMON_FIXES.items():
            if re.search(fix_info['pattern'], error_lower):
                return fix_info['fix']

        return None

    @classmethod
    def apply_common_fixes(cls, query: str) -> str:
        """일반적인 자동 수정 적용"""
        fixed = query

        # Add missing AS keywords for aliases
        fixed = re.sub(r'FROM\s+(\w+)\s+(\w+)(?!\s+AS)',
                      r'FROM \1 AS \2', fixed, flags=re.IGNORECASE)

        # Fix common typos
        fixed = fixed.replace('FORM', 'FROM')
        fixed = fixed.replace('WEHRE', 'WHERE')
        fixed = fixed.replace('GROPU', 'GROUP')

        # Ensure proper spacing
        fixed = re.sub(r'([A-Z]+)([A-Z]+)', r'\1 \2', fixed)

        return fixed


if __name__ == "__main__":
    # 사용 예제
    fixer = SelfReflectionFixer()

    # Example with syntax error
    broken_query = """
    SELECT customer_name, COUNT(*)
    FORM customers
    WHERE age > 20
    GROUP BY customer_id
    """

    schema = """
    CREATE TABLE customers (
        customer_id INT,
        customer_name VARCHAR(100),
        age INT
    );
    """

    question = "Find customer names and their order counts for customers over 20"

    print("Original Query:")
    print(broken_query)
    print("\nFixing...")

    fixed = fixer.fix_query(broken_query, schema, question)

    print("\nFixed Query:")
    print(fixed)

    # Test error pattern analyzer
    print("\n" + "="*50)
    print("Error Pattern Analysis")
    print("="*50)

    error_msg = "Column customer_name not in GROUP BY clause"
    suggestion = ErrorPatternAnalyzer.suggest_fix(error_msg)
    print(f"Error: {error_msg}")
    print(f"Suggestion: {suggestion}")

    # Apply common fixes
    query_with_typos = "SELECT * FORM users WEHRE id > 10 GROPU BY name"
    auto_fixed = ErrorPatternAnalyzer.apply_common_fixes(query_with_typos)
    print(f"\nOriginal: {query_with_typos}")
    print(f"Auto-fixed: {auto_fixed}")