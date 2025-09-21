"""
Instance-aware Synthetic Example Generation
Test-time에 실시간으로 관련 예제를 생성하는 시스템
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import random
import re
from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class SyntheticExample:
    """합성 예제 데이터 클래스"""
    question: str
    sql: str
    sql_features: List[str]  # ['join', 'aggregate', 'subquery', etc.]
    tables_used: List[str]
    complexity: str  # 'simple', 'medium', 'complex'

class OnlineSyntheticGenerator:
    """
    Instance-aware synthetic example generation
    Test-time에 현재 질문과 스키마에 맞춤형 예제 생성
    """

    # SQL Feature categories (from BIRD distribution)
    SQL_FEATURES = {
        'basic': ['equality', 'inequality', 'like', 'in', 'between'],
        'aggregation': ['count', 'sum', 'avg', 'max', 'min'],
        'grouping': ['group_by', 'having'],
        'join': ['inner_join', 'left_join', 'multiple_join'],
        'subquery': ['in_subquery', 'exists', 'scalar_subquery'],
        'ordering': ['order_by', 'limit'],
        'advanced': ['case_when', 'window_function', 'cte']
    }

    def __init__(self, model_name: str = "gpt-4.1-nano"):
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("ENDPOINT_URL"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2025-01-01-preview"
        )
        self.model_name = model_name

    def generate_examples(self,
                         question: str,
                         database_schema: str,
                         filtered_columns: List[str] = None,
                         n_examples: int = 5) -> List[SyntheticExample]:
        """
        현재 질문과 스키마에 맞춤형 예제 생성
        두 가지 가이드라인 사용:
        1. Rf: Common SQL features
        2. Rt: Filtered schema specific
        """
        examples = []

        # Generate with common SQL features (Rf)
        rf_examples = self._generate_with_features(
            database_schema,
            n_examples=n_examples // 2 + 1
        )
        examples.extend(rf_examples)

        # Generate with filtered schema (Rt)
        if filtered_columns:
            rt_examples = self._generate_with_filtered_schema(
                database_schema,
                filtered_columns,
                question,
                n_examples=n_examples // 2
            )
            examples.extend(rt_examples)

        # Mix and deduplicate
        examples = self._mix_and_deduplicate(examples, n_examples)

        return examples

    def _generate_with_features(self,
                               schema: str,
                               n_examples: int) -> List[SyntheticExample]:
        """Rf: Common SQL features 기반 예제 생성"""
        prompt = f"""You are a SQL expert. Generate {n_examples} diverse SQL examples for the given schema.
Each example should showcase different SQL features from the BIRD dataset distribution.

Database Schema:
{schema}

Generate examples that include:
• Simple SELECT with WHERE (equality, inequality)
• Aggregation functions (COUNT, SUM, AVG)
• GROUP BY with HAVING
• INNER JOIN between tables
• Complex JOIN with multiple tables
• Subqueries (IN, EXISTS)
• ORDER BY with LIMIT

Format each example as:
Question: [natural language question]
SQL: [the SQL query]
Features: [list of SQL features used]

Generate {n_examples} diverse examples:"""

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=2000
        )

        examples = self._parse_examples(response.choices[0].message.content)
        return examples

    def _generate_with_filtered_schema(self,
                                      full_schema: str,
                                      filtered_columns: List[str],
                                      original_question: str,
                                      n_examples: int) -> List[SyntheticExample]:
        """Rt: Filtered schema specific 예제 생성"""
        # Extract relevant tables from filtered columns
        relevant_tables = self._extract_tables_from_columns(filtered_columns)

        prompt = f"""You are a SQL expert. Generate {n_examples} SQL examples similar in style to this question:
"{original_question}"

Use these specific tables and columns:
{', '.join(filtered_columns)}

Database Schema:
{full_schema}

Generate examples that:
1. Use similar query patterns to the original question
2. Focus on the specified tables and columns
3. Vary in complexity but maintain relevance

Format each example as:
Question: [natural language question]
SQL: [the SQL query]
Features: [list of SQL features used]

Generate {n_examples} examples:"""

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,  # Lower temperature for more focused examples
            max_tokens=1500
        )

        examples = self._parse_examples(response.choices[0].message.content)
        return examples

    def _parse_examples(self, response: str) -> List[SyntheticExample]:
        """응답에서 예제 파싱"""
        examples = []

        # Pattern to extract examples
        pattern = r'Question:\s*(.+?)\s*SQL:\s*(.+?)\s*Features:\s*(.+?)(?=Question:|$)'
        matches = re.findall(pattern, response, re.DOTALL | re.IGNORECASE)

        for question, sql, features in matches:
            # Clean up
            question = question.strip()
            sql = sql.strip()
            if sql.startswith('```'):
                sql = re.sub(r'^```sql?\n?', '', sql)
                sql = re.sub(r'\n?```$', '', sql)

            # Parse features
            feature_list = [f.strip() for f in features.split(',')]

            # Extract tables from SQL
            tables = self._extract_tables_from_sql(sql)

            # Determine complexity
            complexity = self._determine_complexity(sql)

            examples.append(SyntheticExample(
                question=question,
                sql=sql,
                sql_features=feature_list,
                tables_used=tables,
                complexity=complexity
            ))

        return examples

    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """SQL에서 테이블 이름 추출"""
        tables = []
        sql_upper = sql.upper()

        # FROM clause
        from_match = re.search(r'FROM\s+(\w+)', sql_upper)
        if from_match:
            tables.append(from_match.group(1))

        # JOIN clauses
        join_matches = re.findall(r'JOIN\s+(\w+)', sql_upper)
        tables.extend(join_matches)

        return list(set(tables))

    def _extract_tables_from_columns(self, columns: List[str]) -> List[str]:
        """컬럼 리스트에서 테이블 이름 추출"""
        tables = set()
        for col in columns:
            if '.' in col:
                table = col.split('.')[0]
                tables.add(table)
        return list(tables)

    def _determine_complexity(self, sql: str) -> str:
        """SQL 복잡도 판단"""
        sql_upper = sql.upper()

        # Count features
        feature_count = 0
        if 'JOIN' in sql_upper:
            feature_count += sql_upper.count('JOIN')
        if 'GROUP BY' in sql_upper:
            feature_count += 1
        if 'HAVING' in sql_upper:
            feature_count += 1
        if 'SUBQUERY' in sql_upper or '(SELECT' in sql_upper:
            feature_count += 2
        if 'CASE' in sql_upper:
            feature_count += 1

        if feature_count == 0:
            return 'simple'
        elif feature_count <= 2:
            return 'medium'
        else:
            return 'complex'

    def _mix_and_deduplicate(self,
                            examples: List[SyntheticExample],
                            target_count: int) -> List[SyntheticExample]:
        """예제 믹싱 및 중복 제거"""
        # Remove exact duplicates
        seen_sqls = set()
        unique_examples = []

        for ex in examples:
            sql_normalized = ' '.join(ex.sql.split()).upper()
            if sql_normalized not in seen_sqls:
                seen_sqls.add(sql_normalized)
                unique_examples.append(ex)

        # Ensure diversity in complexity
        simple = [e for e in unique_examples if e.complexity == 'simple']
        medium = [e for e in unique_examples if e.complexity == 'medium']
        complex = [e for e in unique_examples if e.complexity == 'complex']

        # Mix to get target count
        result = []
        # Try to get balanced distribution
        for examples_list in [simple, medium, complex]:
            if examples_list and len(result) < target_count:
                count = min(len(examples_list), (target_count - len(result)))
                result.extend(random.sample(examples_list, count))

        # Fill remaining if needed
        while len(result) < target_count and len(unique_examples) > len(result):
            remaining = [e for e in unique_examples if e not in result]
            if remaining:
                result.append(random.choice(remaining))
            else:
                break

        return result[:target_count]

class FeatureDistributionMatcher:
    """
    BIRD dataset의 SQL feature distribution과 매칭
    """

    # BIRD dataset feature distribution (approximate)
    BIRD_DISTRIBUTION = {
        'simple_select': 0.25,
        'aggregation': 0.20,
        'join': 0.30,
        'group_by': 0.15,
        'subquery': 0.08,
        'advanced': 0.02
    }

    @classmethod
    def match_distribution(cls,
                          examples: List[SyntheticExample],
                          target_count: int = 10) -> List[SyntheticExample]:
        """BIRD distribution에 맞춰 예제 선택"""
        categorized = cls._categorize_examples(examples)
        result = []

        for category, ratio in cls.BIRD_DISTRIBUTION.items():
            target_num = int(target_count * ratio)
            if category in categorized and categorized[category]:
                selected = random.sample(
                    categorized[category],
                    min(len(categorized[category]), target_num)
                )
                result.extend(selected)

        # Fill remaining slots
        while len(result) < target_count:
            remaining = [e for e in examples if e not in result]
            if remaining:
                result.append(random.choice(remaining))
            else:
                break

        return result[:target_count]

    @classmethod
    def _categorize_examples(cls, examples: List[SyntheticExample]) -> Dict[str, List]:
        """예제를 카테고리별로 분류"""
        categories = {
            'simple_select': [],
            'aggregation': [],
            'join': [],
            'group_by': [],
            'subquery': [],
            'advanced': []
        }

        for ex in examples:
            features_lower = [f.lower() for f in ex.sql_features]

            if any('join' in f for f in features_lower):
                categories['join'].append(ex)
            elif any(f in ['count', 'sum', 'avg', 'max', 'min'] for f in features_lower):
                categories['aggregation'].append(ex)
            elif any('group' in f for f in features_lower):
                categories['group_by'].append(ex)
            elif any('subquery' in f or 'exists' in f for f in features_lower):
                categories['subquery'].append(ex)
            elif any(f in ['case', 'window'] for f in features_lower):
                categories['advanced'].append(ex)
            else:
                categories['simple_select'].append(ex)

        return categories


if __name__ == "__main__":
    # 사용 예제
    generator = OnlineSyntheticGenerator()

    question = "Find the top 5 products by sales in each category"

    schema = """
    CREATE TABLE products (id INT, name VARCHAR, category_id INT, price DECIMAL);
    CREATE TABLE categories (id INT, name VARCHAR);
    CREATE TABLE sales (id INT, product_id INT, quantity INT, date DATE);
    """

    filtered_columns = [
        "products.id", "products.name", "products.category_id",
        "categories.name", "sales.quantity", "sales.product_id"
    ]

    # Generate examples
    examples = generator.generate_examples(
        question=question,
        database_schema=schema,
        filtered_columns=filtered_columns,
        n_examples=5
    )

    print(f"Generated {len(examples)} examples:\n")
    for i, ex in enumerate(examples, 1):
        print(f"Example {i}:")
        print(f"  Question: {ex.question}")
        print(f"  SQL: {ex.sql}")
        print(f"  Complexity: {ex.complexity}")
        print(f"  Features: {', '.join(ex.sql_features)}")
        print()

    # Match to BIRD distribution
    matched = FeatureDistributionMatcher.match_distribution(examples, target_count=5)
    print(f"\nAfter matching BIRD distribution: {len(matched)} examples")