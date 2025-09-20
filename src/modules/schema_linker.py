"""
Schema Linking Module
Implements Section 3 of the paper - Schema Linking with Profile Metadata
"""
import logging
from typing import Dict, List, Set, Tuple, Optional
import numpy as np
from datasketch import MinHashLSH, MinHash
import faiss
import re
from openai import AzureOpenAI
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from configs.config import Config

logger = logging.getLogger(__name__)


class SchemaLinker:
    """Schema linking using LSH for literal matching and FAISS for semantic similarity"""

    def __init__(self, llm_client: AzureOpenAI):
        self.config = Config()
        self.llm_client = llm_client
        # Create separate embedding client
        self.embedding_client = AzureOpenAI(
            azure_endpoint=self.config.EMBEDDING_ENDPOINT,
            api_key=self.config.EMBEDDING_API_KEY,
            api_version=self.config.EMBEDDING_API_VERSION
        )
        self.lsh_index = None
        self.faiss_index = None
        self.field_metadata = {}
        self.field_samples = {}
        self.foreign_keys = {}  # Store foreign key relationships
        self.table_relationships = {}  # Store table relationships

    def detect_foreign_keys(self, database_profile: Dict):
        """Detect foreign key relationships based on column naming patterns"""
        tables = database_profile.get('tables', {})

        # Common foreign key patterns
        fk_patterns = ['_id', '_code', '_num', '_no', '_key']

        for table_name, table_data in tables.items():
            self.table_relationships[table_name] = set()

            for col_name, col_data in table_data.get('columns', {}).items():
                # Check if column name suggests a foreign key
                for pattern in fk_patterns:
                    if pattern in col_name.lower():
                        # Try to find the referenced table
                        potential_table = col_name.lower().replace(pattern, '')

                        # Check if the potential table exists
                        for other_table in tables.keys():
                            if potential_table in other_table.lower() or other_table.lower() in potential_table:
                                fk_key = f"{table_name}.{col_name}"
                                ref_key = f"{other_table}.id"  # Assume 'id' as primary key

                                # Check if the referenced column exists
                                if 'id' in tables[other_table].get('columns', {}):
                                    self.foreign_keys[fk_key] = ref_key
                                    self.table_relationships[table_name].add(other_table)
                                    self.table_relationships.setdefault(other_table, set()).add(table_name)

        logger.info(f"Detected {len(self.foreign_keys)} potential foreign key relationships")

    def build_lsh_index(self, database_profile: Dict):
        """
        Build LSH index for literal matching
        Section 3: LSH index on sample values for each field
        """
        self.lsh_index = MinHashLSH(threshold=self.config.LSH_THRESHOLD,
                                    num_perm=self.config.MINHASH_PERMUTATIONS)

        for table_name, table_data in database_profile.get('tables', {}).items():
            for col_name, col_data in table_data.get('columns', {}).items():
                field_key = f"{table_name}.{col_name}"

                # Store top values as samples
                if 'top_values' in col_data:
                    samples = [v['value'] for v in col_data['top_values']]
                    self.field_samples[field_key] = samples

                    # Create MinHash for field values
                    m = MinHash(num_perm=self.config.MINHASH_PERMUTATIONS)
                    for value in samples:
                        m.update(str(value).encode('utf-8'))

                    self.lsh_index.insert(field_key, m)

        logger.info(f"Built LSH index with {len(self.field_samples)} fields")

        # Detect foreign keys after building index
        self.detect_foreign_keys(database_profile)

    def build_faiss_index(self, table_summaries: Dict):
        """
        Build FAISS index for semantic similarity on field descriptions
        """
        # Collect all field descriptions
        field_descriptions = []
        field_keys = []

        for table_name, table_summary in table_summaries.items():
            for col_name, col_summary in table_summary.get('column_summaries', {}).items():
                field_key = f"{table_name}.{col_name}"
                field_keys.append(field_key)

                # Use long description for semantic search
                description = col_summary.get('long_description', '')
                field_descriptions.append(description)

                # Store metadata
                self.field_metadata[field_key] = {
                    'short_description': col_summary.get('short_description', ''),
                    'long_description': description,
                    'profile': col_summary.get('profile', {})
                }

        # Generate embeddings
        embeddings = self._get_embeddings(field_descriptions)

        # Create FAISS index
        dimension = embeddings.shape[1]
        self.faiss_index = faiss.IndexFlatL2(dimension)
        self.faiss_index.add(embeddings)

        self.field_keys = field_keys
        logger.info(f"Built FAISS index with {len(field_keys)} fields")

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
                # Use zero vector as fallback
                embeddings.append(np.zeros(self.config.VECTOR_DIM))

        return np.array(embeddings, dtype='float32')

    def extract_literals(self, question: str) -> List[str]:
        """Extract potential literals from question"""
        literals = []

        # Extract quoted strings
        quoted = re.findall(r'"([^"]*)"', question) + re.findall(r"'([^']*)'", question)
        literals.extend(quoted)

        # Extract numbers
        numbers = re.findall(r'\b\d+\.?\d*\b', question)
        literals.extend(numbers)

        # Extract potential identifiers (capitalized words, acronyms)
        words = question.split()
        for word in words:
            if word.isupper() and len(word) > 1:  # Acronyms
                literals.append(word)
            elif word[0].isupper() and not word in ['The', 'A', 'An']:  # Proper nouns
                literals.append(word)

        return list(set(literals))

    def find_fields_with_literal(self, literal: str) -> List[str]:
        """Find fields that contain a literal value using LSH"""
        matching_fields = []

        # Create MinHash for the literal
        m = MinHash(num_perm=self.config.MINHASH_PERMUTATIONS)
        m.update(str(literal).encode('utf-8'))

        # Query LSH index
        try:
            results = self.lsh_index.query(m)
            matching_fields = list(results)
        except:
            pass

        # Also do exact matching on samples
        for field_key, samples in self.field_samples.items():
            if any(str(literal).lower() in str(s).lower() for s in samples):
                if field_key not in matching_fields:
                    matching_fields.append(field_key)

        return matching_fields

    def get_semantically_similar_fields(self, question: str, k: int = 10) -> List[str]:
        """Get semantically similar fields using FAISS"""
        if not self.faiss_index:
            return []

        # Get question embedding
        question_embedding = self._get_embeddings([question])

        # Search FAISS index
        distances, indices = self.faiss_index.search(question_embedding, k)

        # Return field keys
        similar_fields = []
        for idx in indices[0]:
            if idx < len(self.field_keys):
                similar_fields.append(self.field_keys[idx])

        return similar_fields

    def get_focused_schema(self, question: str) -> Dict[str, List[str]]:
        """
        Get focused schema based on question with enhanced relationship detection
        Section 3: Combination of semantic similarity and literal matching
        """
        focused_fields = set()

        # Get semantically similar fields
        similar_fields = self.get_semantically_similar_fields(question, k=20)
        focused_fields.update(similar_fields)

        # Extract literals and find matching fields
        literals = self.extract_literals(question)
        for literal in literals:
            matching_fields = self.find_fields_with_literal(literal)
            focused_fields.update(matching_fields)

        # Group by table
        focused_schema = {}
        for field in focused_fields:
            if '.' in field:
                table, column = field.split('.', 1)
                if table not in focused_schema:
                    focused_schema[table] = []
                focused_schema[table].append(column)

        # Add related tables through foreign keys
        tables_to_check = list(focused_schema.keys())
        for table in tables_to_check:
            if table in self.table_relationships:
                for related_table in self.table_relationships[table]:
                    if related_table not in focused_schema:
                        # Add the primary key and foreign key columns
                        focused_schema[related_table] = ['id']  # Assume 'id' as primary key

                        # Add foreign key columns that reference this table
                        for fk, ref in self.foreign_keys.items():
                            if ref.startswith(f"{related_table}."):
                                fk_table, fk_col = fk.split('.', 1)
                                if fk_table in focused_schema and fk_col not in focused_schema[fk_table]:
                                    focused_schema[fk_table].append(fk_col)

        logger.info(f"Focused schema has {len(focused_fields)} fields from {len(focused_schema)} tables (including related)")
        return focused_schema

    def generate_schema_context(self, schema_type: str, profile_type: str,
                               focused_schema: Optional[Dict] = None) -> str:
        """
        Generate schema context for LLM prompt
        schema_type: 'focused' or 'full'
        profile_type: 'minimal', 'maximal', or 'full'
        """
        context_lines = []

        # Determine which fields to include
        if schema_type == 'focused' and focused_schema:
            fields_to_include = focused_schema
        else:
            # Include all fields
            fields_to_include = {}
            for field_key in self.field_metadata.keys():
                if '.' in field_key:
                    table, column = field_key.split('.', 1)
                    if table not in fields_to_include:
                        fields_to_include[table] = []
                    fields_to_include[table].append(column)

        # Group by table for better organization
        for table, columns in fields_to_include.items():
            context_lines.append(f"\nTable: {table}")
            table_columns = []

            for column in columns:
                field_key = f"{table}.{column}"
                if field_key not in self.field_metadata:
                    # Basic column info if no metadata
                    table_columns.append(f"  - {column}")
                    continue

                metadata = self.field_metadata[field_key]

                if profile_type == 'minimal':
                    description = metadata.get('short_description', '')
                    # Check if this is a foreign key
                    fk_info = ""
                    field_key_check = f"{table}.{column}"
                    if field_key_check in self.foreign_keys:
                        fk_info = f" [FK -> {self.foreign_keys[field_key_check]}]"
                    table_columns.append(f"  - {column}: {description}{fk_info}")
                elif profile_type == 'maximal':
                    description = metadata.get('long_description', '')
                    # Truncate long descriptions for cleaner context
                    if len(description) > 200:
                        description = description[:200] + "..."
                    # Check if this is a foreign key
                    fk_info = ""
                    field_key_check = f"{table}.{column}"
                    if field_key_check in self.foreign_keys:
                        fk_info = f" [FK -> {self.foreign_keys[field_key_check]}]"
                    table_columns.append(f"  - {column}: {description}{fk_info}")
                else:  # full
                    short = metadata.get('short_description', '')
                    long = metadata.get('long_description', '')
                    if len(long) > 150:
                        long = long[:150] + "..."
                    description = f"{short}. {long}"
                    # Check if this is a foreign key
                    fk_info = ""
                    field_key_check = f"{table}.{column}"
                    if field_key_check in self.foreign_keys:
                        fk_info = f" [FK -> {self.foreign_keys[field_key_check]}]"
                    table_columns.append(f"  - {column}: {description}{fk_info}")

            context_lines.extend(table_columns[:10])  # Limit columns per table

        return "\n".join(context_lines)

    def multi_pass_schema_linking(self, question: str, llm_client) -> Set[str]:
        """
        Multi-pass schema linking algorithm from Section 3
        """
        all_fields = set()
        all_literals = set()

        # Get focused schema
        focused_schema = self.get_focused_schema(question)

        # Five schema variations as described in the paper
        variations = [
            ('focused', 'minimal'),
            ('focused', 'maximal'),
            ('full', 'minimal'),
            ('full', 'maximal'),
            ('focused', 'full')
        ]

        for schema_type, profile_type in variations:
            logger.info(f"Trying schema variation: {schema_type} schema with {profile_type} profile")

            # Generate schema context
            if schema_type == 'focused':
                context = self.generate_schema_context(schema_type, profile_type, focused_schema)
            else:
                context = self.generate_schema_context(schema_type, profile_type)

            # Generate SQL query
            sql_query = self._generate_sql_with_context(question, context, llm_client)

            # Extract fields and literals from SQL
            fields, literals = self._extract_from_sql(sql_query)
            all_fields.update(fields)
            all_literals.update(literals)

            # Check for literal mismatches and correct
            for _ in range(self.config.MAX_RETRIES):
                missing_literals = []
                lit_fields = {}

                for literal in literals:
                    fields_with_literal = self.find_fields_with_literal(literal)
                    if fields_with_literal:
                        # Check if any field is already in the query
                        if not any(f in fields for f in fields_with_literal):
                            lit_fields[literal] = fields_with_literal
                            missing_literals.append(literal)

                if not missing_literals:
                    break

                # Ask LLM to revise query
                sql_query = self._revise_sql_with_literals(
                    sql_query, lit_fields, llm_client
                )
                fields, literals = self._extract_from_sql(sql_query)
                all_fields.update(fields)

        return all_fields

    def _generate_sql_with_context(self, question: str, context: str,
                                   llm_client) -> str:
        """Generate SQL query with schema context"""
        prompt = f"""Given the following database schema information:

{context}

Generate a SQL query to answer this question: {question}

SQL Query:"""

        try:
            response = llm_client.chat.completions.create(
                model=self.config.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": "You are a SQL expert. Generate valid SQL queries."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            return ""

    def _revise_sql_with_literals(self, sql_query: str, lit_fields: Dict,
                                  llm_client) -> str:
        """Revise SQL query to use correct fields for literals"""
        revision_hints = []
        for literal, fields in lit_fields.items():
            fields_str = ', '.join(fields[:3])
            revision_hints.append(
                f"The literal '{literal}' should match against one of these fields: {fields_str}"
            )

        prompt = f"""Revise the following SQL query using these hints:

Original SQL:
{sql_query}

Hints:
{chr(10).join(revision_hints)}

Revised SQL Query:"""

        try:
            response = llm_client.chat.completions.create(
                model=self.config.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": "You are a SQL expert. Revise the query based on the hints."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error revising SQL: {e}")
            return sql_query

    def _extract_from_sql(self, sql_query: str) -> Tuple[Set[str], Set[str]]:
        """Extract fields and literals from SQL query"""
        fields = set()
        literals = set()

        # Simple extraction - would need more robust SQL parsing
        # Extract table.column references
        field_pattern = r'([a-zA-Z_]\w*\.[a-zA-Z_]\w*)'
        fields.update(re.findall(field_pattern, sql_query))

        # Extract string literals
        string_pattern = r"'([^']*)'"
        literals.update(re.findall(string_pattern, sql_query))

        # Extract numeric literals
        num_pattern = r'=\s*(\d+\.?\d*)'
        literals.update(re.findall(num_pattern, sql_query))

        return fields, literals