"""
SQL Generation Pipeline Module
Implements Section 4 of the paper - SQL Generation with Few-shot Learning
"""
import logging
import re
import json
from typing import Dict, List, Optional, Tuple, Set
from openai import AzureOpenAI
import numpy as np
import faiss
from collections import Counter
import sqlglot
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from configs.config import Config
from modules.schema_linker import SchemaLinker

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generate SQL queries using few-shot learning and validation"""

    def __init__(self, llm_client: AzureOpenAI, schema_linker: SchemaLinker):
        self.config = Config()
        self.llm_client = llm_client
        self.schema_linker = schema_linker
        # Create separate embedding client
        self.embedding_client = AzureOpenAI(
            azure_endpoint=self.config.EMBEDDING_ENDPOINT,
            api_key=self.config.EMBEDDING_API_KEY,
            api_version=self.config.EMBEDDING_API_VERSION
        )
        self.few_shot_index = None
        self.examples_db = []

    def build_few_shot_index(self, examples: List[Dict]):
        """
        Build FAISS index for few-shot example selection
        Section 4: Use vector database to find similar examples
        """
        self.examples_db = examples

        if not examples:
            logger.warning("No examples provided for few-shot learning")
            return

        # Extract questions for embedding
        questions = [ex['question'] for ex in examples]

        # Generate embeddings
        embeddings = self._get_embeddings(questions)

        # Create FAISS index
        dimension = embeddings.shape[1]
        self.few_shot_index = faiss.IndexFlatL2(dimension)
        self.few_shot_index.add(embeddings)

        logger.info(f"Built few-shot index with {len(examples)} examples")

    def _get_embeddings(self, texts: List[str]) -> np.ndarray:
        """Get text embeddings using Azure OpenAI"""
        embeddings = []

        for text in texts:
            try:
                response = self.embedding_client.embeddings.create(
                    model=self.config.EMBEDDING_DEPLOYMENT,
                    input=text
                )
                embedding = response.data[0].embedding
                embeddings.append(embedding)
            except Exception as e:
                logger.error(f"Error getting embedding: {e}")
                embeddings.append(np.zeros(self.config.VECTOR_DIM))

        return np.array(embeddings, dtype='float32')

    def select_few_shot_examples(self, question: str, k: int = 5) -> List[Dict]:
        """
        Select k most similar examples for few-shot learning
        """
        if not self.few_shot_index or not self.examples_db:
            return []

        # Get question embedding
        question_embedding = self._get_embeddings([question])

        # Search for similar examples
        distances, indices = self.few_shot_index.search(question_embedding, k)

        # Return selected examples
        selected_examples = []
        for idx in indices[0]:
            if idx < len(self.examples_db):
                selected_examples.append(self.examples_db[idx])

        return selected_examples

    def generate_sql_candidates(self, question: str, schema_context: str,
                               few_shot_examples: List[Dict]) -> List[str]:
        """
        Generate multiple SQL candidates with different parameters
        Section 4: Generate multiple candidates for majority voting
        """
        candidates = []

        # Build few-shot prompt
        few_shot_prompt = self._build_few_shot_prompt(few_shot_examples)

        # Generation configurations
        configs = [
            {'temperature': 0.1, 'top_p': 0.9},
            {'temperature': 0.3, 'top_p': 0.8},
            {'temperature': 0.5, 'top_p': 0.7},
            {'temperature': 0.2, 'top_p': 1.0},
            {'temperature': 0.4, 'top_p': 0.9}
        ]

        for config in configs:
            sql = self._generate_single_sql(
                question, schema_context, few_shot_prompt,
                temperature=config['temperature'],
                top_p=config['top_p']
            )
            if sql:
                candidates.append(sql)

        logger.info(f"Generated {len(candidates)} SQL candidates")
        return candidates

    def _build_few_shot_prompt(self, examples: List[Dict]) -> str:
        """Build few-shot learning prompt from examples"""
        if not examples:
            return ""

        prompt_parts = ["Here are some examples of questions and their corresponding SQL queries:\n"]

        for i, example in enumerate(examples, 1):
            prompt_parts.append(f"Example {i}:")
            prompt_parts.append(f"Question: {example['question']}")
            prompt_parts.append(f"SQL: {example['sql']}")
            prompt_parts.append("")

        return "\n".join(prompt_parts)

    def _generate_single_sql(self, question: str, schema_context: str,
                            few_shot_prompt: str, temperature: float = 0.1,
                            top_p: float = 0.9) -> str:
        """Generate a single SQL query"""
        prompt = f"""{few_shot_prompt}

Database Schema:
{schema_context}

Generate a SQL query to answer this question: {question}

Guidelines:
1. Use only the tables and columns mentioned in the schema
2. Pay attention to the column descriptions and sample values
3. Use appropriate JOIN conditions when needed
4. Include necessary WHERE clauses for filtering
5. Use proper GROUP BY and ORDER BY when needed

SQL Query:"""

        try:
            response = self.llm_client.chat.completions.create(
                model=self.config.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": "You are an expert SQL developer. Generate accurate SQL queries based on the schema and examples provided."},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                top_p=top_p,
                max_tokens=800
            )

            sql = response.choices[0].message.content.strip()
            return self._clean_sql(sql)

        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return ""

    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL query"""
        # Remove markdown code blocks
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)

        # Remove extra whitespace
        sql = re.sub(r'\s+', ' ', sql).strip()

        # Ensure it ends with semicolon
        if sql and not sql.endswith(';'):
            sql += ';'

        return sql

    def validate_sql(self, sql: str) -> Tuple[bool, str]:
        """
        Validate SQL syntax using sqlglot
        Section 4: SQL validation step
        """
        try:
            # Parse SQL using sqlglot
            parsed = sqlglot.parse_one(sql, dialect='postgres')
            if parsed:
                return True, "Valid SQL"
            else:
                return False, "Failed to parse SQL"
        except Exception as e:
            return False, f"SQL parsing error: {str(e)}"

    def check_sql_patterns(self, sql: str, focused_fields: Set[str]) -> Dict[str, bool]:
        """
        Check SQL for potentially incorrect patterns
        Section 4: Pattern checking for SQL validation
        """
        checks = {
            'has_select': bool(re.search(r'\bSELECT\b', sql, re.IGNORECASE)),
            'has_from': bool(re.search(r'\bFROM\b', sql, re.IGNORECASE)),
            'uses_focused_fields': False,
            'has_proper_joins': True,
            'no_syntax_errors': True,
            'reasonable_complexity': True
        }

        # Check if SQL uses focused fields
        for field in focused_fields:
            if field.replace('.', '\\.') in sql:
                checks['uses_focused_fields'] = True
                break

        # Check for proper JOIN syntax
        joins = re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
        if joins:
            # Simple check: ensure there's an ON clause for each JOIN
            on_clauses = len(re.findall(r'\bON\b', sql, re.IGNORECASE))
            if on_clauses < len(joins):
                checks['has_proper_joins'] = False

        # Check complexity (not too many nested queries)
        nested_selects = len(re.findall(r'\bSELECT\b', sql, re.IGNORECASE))
        if nested_selects > 3:
            checks['reasonable_complexity'] = False

        # Validate syntax
        is_valid, _ = self.validate_sql(sql)
        checks['no_syntax_errors'] = is_valid

        return checks

    def majority_voting(self, candidates: List[str], question: str) -> str:
        """
        Select best SQL using majority voting and validation
        Section 4: Final answer selection
        """
        if not candidates:
            return ""

        # Filter out invalid candidates
        valid_candidates = []
        for sql in candidates:
            is_valid, _ = self.validate_sql(sql)
            if is_valid:
                valid_candidates.append(sql)

        if not valid_candidates:
            logger.warning("No valid SQL candidates found")
            return candidates[0] if candidates else ""

        # If only one valid candidate, return it
        if len(valid_candidates) == 1:
            return valid_candidates[0]

        # Score candidates based on various criteria
        candidate_scores = []
        focused_fields = set()

        # Get focused fields from schema linking
        try:
            focused_schema = self.schema_linker.get_focused_schema(question)
            for table, columns in focused_schema.items():
                for col in columns:
                    focused_fields.add(f"{table}.{col}")
        except:
            pass

        for sql in valid_candidates:
            score = 0

            # Check patterns
            patterns = self.check_sql_patterns(sql, focused_fields)

            # Score based on pattern checks
            if patterns['has_select']: score += 1
            if patterns['has_from']: score += 1
            if patterns['uses_focused_fields']: score += 3
            if patterns['has_proper_joins']: score += 2
            if patterns['no_syntax_errors']: score += 2
            if patterns['reasonable_complexity']: score += 1

            # Prefer shorter, simpler queries
            query_length = len(sql.split())
            if query_length < 20: score += 1
            elif query_length > 50: score -= 1

            candidate_scores.append((sql, score))

        # Sort by score and return best candidate
        candidate_scores.sort(key=lambda x: x[1], reverse=True)
        best_sql = candidate_scores[0][0]

        logger.info(f"Selected SQL with score {candidate_scores[0][1]}: {best_sql[:100]}...")
        return best_sql

    def generate_sql(self, question: str, database_profile: Dict,
                    table_summaries: Dict) -> Dict:
        """
        Main SQL generation pipeline
        Section 4: Complete pipeline from question to SQL
        """
        logger.info(f"Generating SQL for question: {question}")

        # Step 1: Schema linking
        focused_schema = self.schema_linker.get_focused_schema(question)

        # Step 2: Generate schema context
        schema_context = self.schema_linker.generate_schema_context(
            'focused', 'maximal', focused_schema
        )

        # Step 3: Select few-shot examples
        few_shot_examples = self.select_few_shot_examples(question, k=3)

        # Step 4: Generate SQL candidates
        sql_candidates = self.generate_sql_candidates(
            question, schema_context, few_shot_examples
        )

        # Step 5: Select best SQL using majority voting
        final_sql = self.majority_voting(sql_candidates, question)

        # Step 6: Final validation
        is_valid, validation_msg = self.validate_sql(final_sql)

        return {
            'question': question,
            'focused_schema': focused_schema,
            'few_shot_examples': [ex['question'] for ex in few_shot_examples],
            'sql_candidates': sql_candidates,
            'final_sql': final_sql,
            'is_valid': is_valid,
            'validation_message': validation_msg,
            'schema_context': schema_context
        }

    def batch_generate_sql(self, questions: List[str], database_profile: Dict,
                          table_summaries: Dict) -> List[Dict]:
        """Generate SQL for multiple questions"""
        results = []

        for i, question in enumerate(questions, 1):
            logger.info(f"Processing question {i}/{len(questions)}")
            result = self.generate_sql(question, database_profile, table_summaries)
            results.append(result)

        return results

    def save_results(self, results: List[Dict], output_path: Path):
        """Save generation results to file"""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved SQL generation results to {output_path}")