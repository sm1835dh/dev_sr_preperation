"""
Prompt Optimization Module
Arctic-Text2SQL의 프롬프트 최적화 시스템
OmniSQL 스타일 프롬프트 및 Chain-of-Thought 구현
"""

from typing import Optional, Dict, List, Any
from dataclasses import dataclass
import re
import json

@dataclass
class PromptConfig:
    """프롬프트 설정"""
    use_chain_of_thought: bool = True
    use_think_tags: bool = True
    max_think_tokens: int = 4000
    max_answer_tokens: int = 1000
    include_examples: bool = False
    database_serialization: str = "detailed"  # "detailed", "simple", "compressed"


class ArcticPromptOptimizer:
    """
    Arctic-Text2SQL의 프롬프트 최적화 시스템

    핵심 개선사항:
    1. OmniSQL 스타일 프롬프트 사용 (57.4% → 65.1% 성능 향상)
    2. Chain-of-Thought with <think> tags
    3. 데이터베이스 스키마 직렬화 최적화
    4. Step-by-step reasoning 유도
    """

    def __init__(self, config: PromptConfig = None):
        """
        Args:
            config: 프롬프트 설정
        """
        self.config = config or PromptConfig()
        self.template_cache = {}

    def generate_prompt(self,
                       question: str,
                       schema: str,
                       evidence: Optional[str] = None,
                       examples: Optional[List[Dict]] = None) -> str:
        """
        최적화된 프롬프트 생성

        Args:
            question: 자연어 질문
            schema: 데이터베이스 스키마
            evidence: 추가 정보/힌트
            examples: Few-shot 예제

        Returns:
            최적화된 프롬프트
        """
        # 시스템 프롬프트
        system_prompt = self._get_system_prompt()

        # 데이터베이스 스키마 직렬화
        serialized_schema = self._serialize_schema(schema)

        # 질문 포맷팅
        formatted_question = self._format_question(question, evidence)

        # Few-shot 예제 (있는 경우)
        examples_text = ""
        if self.config.include_examples and examples:
            examples_text = self._format_examples(examples)

        # 최종 프롬프트 조립
        if self.config.use_chain_of_thought:
            return self._build_cot_prompt(
                system_prompt,
                serialized_schema,
                formatted_question,
                examples_text
            )
        else:
            return self._build_direct_prompt(
                system_prompt,
                serialized_schema,
                formatted_question,
                examples_text
            )

    def _get_system_prompt(self) -> str:
        """시스템 프롬프트 생성"""
        return """You are a data science expert. Below, you are provided with a database schema and a natural language question. Your task is to understand the schema and generate a valid SQL query to answer the question.

Instructions:
- Make sure you only output the information that is asked in the question
- If the question asks for a specific column, only include that column in the SELECT clause
- The generated query should return all information asked without missing or extra data
- Pay careful attention to table relationships and join conditions
- Use appropriate aggregate functions when needed
- Ensure proper GROUP BY when using aggregates"""

    def _serialize_schema(self, schema: str) -> str:
        """
        데이터베이스 스키마 직렬화 최적화

        Args:
            schema: 원본 스키마

        Returns:
            최적화된 스키마
        """
        if self.config.database_serialization == "detailed":
            return self._serialize_detailed(schema)
        elif self.config.database_serialization == "compressed":
            return self._serialize_compressed(schema)
        else:
            return schema

    def _serialize_detailed(self, schema: str) -> str:
        """상세 스키마 직렬화"""
        lines = []
        lines.append("Database Schema:")
        lines.append("=" * 50)

        # 테이블별로 파싱 및 포맷팅
        tables = self._parse_schema(schema)

        for table_name, table_info in tables.items():
            lines.append(f"\nTable: {table_name}")
            lines.append("-" * 30)

            # 컬럼 정보
            lines.append("Columns:")
            for col in table_info['columns']:
                col_line = f"  - {col['name']} ({col['type']})"
                if col.get('primary_key'):
                    col_line += " [PRIMARY KEY]"
                if col.get('foreign_key'):
                    col_line += f" [FK -> {col['foreign_key']}]"
                lines.append(col_line)

            # 관계 정보
            if table_info.get('relationships'):
                lines.append("Relationships:")
                for rel in table_info['relationships']:
                    lines.append(f"  - {rel}")

        return "\n".join(lines)

    def _serialize_compressed(self, schema: str) -> str:
        """압축된 스키마 직렬화"""
        # 불필요한 공백 제거
        schema = re.sub(r'\s+', ' ', schema)
        # 주석 제거
        schema = re.sub(r'--.*?$', '', schema, flags=re.MULTILINE)
        return f"Schema: {schema}"

    def _parse_schema(self, schema: str) -> Dict[str, Dict]:
        """스키마 파싱"""
        tables = {}

        # CREATE TABLE 문 추출
        table_pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);'
        matches = re.finditer(table_pattern, schema, re.IGNORECASE | re.DOTALL)

        for match in matches:
            table_name = match.group(1)
            table_def = match.group(2)

            # 컬럼 파싱
            columns = []
            column_defs = table_def.split(',')

            for col_def in column_defs:
                col_def = col_def.strip()
                if not col_def:
                    continue

                # 컬럼 정보 추출
                col_match = re.match(r'(\w+)\s+(\w+(?:\([^)]+\))?)', col_def)
                if col_match:
                    col_info = {
                        'name': col_match.group(1),
                        'type': col_match.group(2)
                    }

                    # PRIMARY KEY 체크
                    if 'PRIMARY KEY' in col_def.upper():
                        col_info['primary_key'] = True

                    # FOREIGN KEY 체크
                    fk_match = re.search(r'REFERENCES\s+(\w+\.\w+|\w+)', col_def, re.IGNORECASE)
                    if fk_match:
                        col_info['foreign_key'] = fk_match.group(1)

                    columns.append(col_info)

            tables[table_name] = {
                'columns': columns,
                'relationships': []
            }

        return tables

    def _format_question(self, question: str, evidence: Optional[str]) -> str:
        """질문 포맷팅"""
        if evidence:
            return f"""Question: {question}
Additional Information: {evidence}"""
        return f"Question: {question}"

    def _format_examples(self, examples: List[Dict]) -> str:
        """Few-shot 예제 포맷팅"""
        lines = ["Examples:", "=" * 50]

        for i, example in enumerate(examples, 1):
            lines.append(f"\nExample {i}:")
            lines.append(f"Q: {example['question']}")
            lines.append(f"SQL: {example['sql']}")

        lines.append("=" * 50)
        return "\n".join(lines)

    def _build_cot_prompt(self,
                         system: str,
                         schema: str,
                         question: str,
                         examples: str) -> str:
        """Chain-of-Thought 프롬프트 생성"""
        template = f"""{system}

{schema}

{examples}

{question}

Please provide a detailed chain-of-thought reasoning process."""

        if self.config.use_think_tags:
            template += f"""

Output Format:
<think>
Step-by-step reasoning, including self-reflection and corrections if necessary.
[Limited by {self.config.max_think_tokens} tokens]
</think>

<answer>
Summary of the thought process leading to the final SQL query.
[Limited by {self.config.max_answer_tokens} tokens]

```sql
-- Your SQL query here
```
</answer>"""
        else:
            template += """

Think through this step-by-step:
1. Identify the tables needed
2. Determine the columns to select
3. Identify join conditions
4. Add WHERE conditions
5. Check for GROUP BY requirements
6. Add ORDER BY if needed
7. Verify the query answers the question

SQL Query:
```sql"""

        return template

    def _build_direct_prompt(self,
                           system: str,
                           schema: str,
                           question: str,
                           examples: str) -> str:
        """직접 프롬프트 생성 (CoT 없이)"""
        return f"""{system}

{schema}

{examples}

{question}

Generate the SQL query:
```sql"""


class ResponseParser:
    """
    모델 응답 파싱
    """

    def __init__(self):
        self.parsing_stats = {
            "total_parsed": 0,
            "with_think_tags": 0,
            "with_answer_tags": 0,
            "extraction_failures": 0
        }

    def parse_response(self, response: str) -> Dict[str, Any]:
        """
        모델 응답 파싱

        Args:
            response: 모델 응답

        Returns:
            파싱된 결과
        """
        self.parsing_stats["total_parsed"] += 1

        result = {
            "sql": None,
            "reasoning": None,
            "answer_summary": None
        }

        # <think> 태그 추출
        think_match = re.search(r'<think>(.*?)</think>', response, re.DOTALL)
        if think_match:
            result["reasoning"] = think_match.group(1).strip()
            self.parsing_stats["with_think_tags"] += 1

        # <answer> 태그 추출
        answer_match = re.search(r'<answer>(.*?)</answer>', response, re.DOTALL)
        if answer_match:
            answer_content = answer_match.group(1)
            result["answer_summary"] = self._extract_summary(answer_content)
            self.parsing_stats["with_answer_tags"] += 1

        # SQL 추출
        sql = self._extract_sql(response)
        if sql:
            result["sql"] = sql
        else:
            self.parsing_stats["extraction_failures"] += 1

        return result

    def _extract_sql(self, text: str) -> Optional[str]:
        """SQL 쿼리 추출"""
        # ```sql 블록 추출
        sql_match = re.search(r'```sql\s*(.*?)\s*```', text, re.DOTALL)
        if sql_match:
            return sql_match.group(1).strip()

        # SELECT로 시작하는 문장 추출 (fallback)
        select_match = re.search(r'(SELECT\s+.*?)(?:\n\n|$)', text, re.DOTALL | re.IGNORECASE)
        if select_match:
            return select_match.group(1).strip()

        return None

    def _extract_summary(self, answer_content: str) -> str:
        """답변 요약 추출"""
        # SQL 블록 제거
        summary = re.sub(r'```sql.*?```', '', answer_content, flags=re.DOTALL)
        return summary.strip()


class PromptTemplateLibrary:
    """
    다양한 프롬프트 템플릿 라이브러리
    """

    TEMPLATES = {
        "arctic_style": {
            "name": "Arctic-Text2SQL Style",
            "description": "Arctic 논문의 최적화된 프롬프트",
            "use_cot": True,
            "use_think_tags": True
        },
        "omnisql_style": {
            "name": "OmniSQL Style",
            "description": "OmniSQL의 프롬프트 스타일",
            "use_cot": True,
            "use_think_tags": False
        },
        "direct": {
            "name": "Direct SQL",
            "description": "CoT 없이 직접 SQL 생성",
            "use_cot": False,
            "use_think_tags": False
        },
        "few_shot": {
            "name": "Few-shot Learning",
            "description": "예제 기반 학습",
            "use_cot": False,
            "use_think_tags": False,
            "include_examples": True
        }
    }

    @classmethod
    def get_template_config(cls, template_name: str) -> PromptConfig:
        """템플릿 설정 반환"""
        if template_name not in cls.TEMPLATES:
            raise ValueError(f"Unknown template: {template_name}")

        template = cls.TEMPLATES[template_name]
        return PromptConfig(
            use_chain_of_thought=template.get("use_cot", True),
            use_think_tags=template.get("use_think_tags", True),
            include_examples=template.get("include_examples", False)
        )

    @classmethod
    def list_templates(cls) -> List[Dict]:
        """사용 가능한 템플릿 목록"""
        return [
            {
                "name": name,
                "info": info
            }
            for name, info in cls.TEMPLATES.items()
        ]


def demo_prompt_optimization():
    """프롬프트 최적화 데모"""
    print("Arctic-Text2SQL Prompt Optimization Demo")
    print("=" * 50)

    # 샘플 데이터
    question = "Find the top 5 customers by total order amount"
    schema = """
    CREATE TABLE customers (
        customer_id INT PRIMARY KEY,
        customer_name VARCHAR(100),
        join_date DATE
    );
    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        amount DECIMAL(10,2),
        order_date DATE,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );
    """
    evidence = "Consider only orders from 2024"

    # 1. Arctic 스타일 프롬프트
    print("\n1. Arctic Style Prompt:")
    print("-" * 30)
    arctic_config = PromptTemplateLibrary.get_template_config("arctic_style")
    optimizer = ArcticPromptOptimizer(arctic_config)
    arctic_prompt = optimizer.generate_prompt(question, schema, evidence)
    print(arctic_prompt[:500] + "..." if len(arctic_prompt) > 500 else arctic_prompt)

    # 2. Direct 스타일 프롬프트
    print("\n2. Direct Style Prompt:")
    print("-" * 30)
    direct_config = PromptTemplateLibrary.get_template_config("direct")
    optimizer = ArcticPromptOptimizer(direct_config)
    direct_prompt = optimizer.generate_prompt(question, schema, evidence)
    print(direct_prompt[:500] + "..." if len(direct_prompt) > 500 else direct_prompt)

    # 3. 응답 파싱 테스트
    print("\n3. Response Parsing:")
    print("-" * 30)
    sample_response = """
<think>
First, I need to identify the tables: customers and orders.
To find top 5 customers by total order amount, I need to:
1. Join customers and orders tables
2. Sum the order amounts per customer
3. Order by total amount descending
4. Limit to 5 results
</think>

<answer>
The query joins customers and orders tables, groups by customer,
sums the order amounts, orders by total descending, and limits to top 5.

```sql
SELECT c.customer_id, c.customer_name, SUM(o.amount) as total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE YEAR(o.order_date) = 2024
GROUP BY c.customer_id, c.customer_name
ORDER BY total_amount DESC
LIMIT 5
```
</answer>
    """

    parser = ResponseParser()
    parsed = parser.parse_response(sample_response)
    print(f"Extracted SQL: {parsed['sql']}")
    print(f"Has reasoning: {'Yes' if parsed['reasoning'] else 'No'}")
    print(f"Has summary: {'Yes' if parsed['answer_summary'] else 'No'}")

    # 4. 템플릿 목록
    print("\n4. Available Templates:")
    print("-" * 30)
    for template in PromptTemplateLibrary.list_templates():
        print(f"- {template['name']}: {template['info']['description']}")


if __name__ == "__main__":
    demo_prompt_optimization()