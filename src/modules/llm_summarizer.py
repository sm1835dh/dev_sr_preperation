"""
LLM Metadata Summarization Module
Implements Section 2.1 of the paper - Using profiling information for Text-to-SQL
"""
import logging
from typing import Dict, List, Optional
from openai import AzureOpenAI
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from configs.config import Config

logger = logging.getLogger(__name__)


class LLMSummarizer:
    """Generate natural language descriptions of database columns using LLM"""

    def __init__(self):
        self.config = Config()
        self.client = AzureOpenAI(
            azure_endpoint=self.config.AZURE_OPENAI_ENDPOINT,
            api_key=self.config.AZURE_OPENAI_KEY,
            api_version=self.config.AZURE_OPENAI_API_VERSION
        )

    def generate_profile_description(self, profile: Dict) -> str:
        """
        Generate mechanical English description of column profile
        Based on Section 2.1 examples
        """
        col_name = profile.get('column_name', '')
        data_type = profile.get('data_type', '')
        null_count = profile.get('null_count', 0)
        non_null_count = profile.get('non_null_count', 0)
        total_records = profile.get('total_records', 0)
        distinct_count = profile.get('distinct_count', 0)

        description = f"Column {col_name} has {null_count} NULL values out of {total_records} records. "
        description += f"There are {distinct_count} distinct values. "

        # Add type-specific information
        if 'min_value' in profile and 'max_value' in profile:
            description += f"The minimum value is '{profile['min_value']}' and "
            description += f"the maximum value is '{profile['max_value']}'. "

        if 'min_length' in profile and 'max_length' in profile:
            min_len = profile['min_length']
            max_len = profile['max_length']
            if min_len == max_len:
                description += f"The values are always {min_len} characters long. "
            else:
                description += f"The values range from {min_len} to {max_len} characters long. "

        # Add top values
        if 'top_values' in profile and profile['top_values']:
            top_values_str = ', '.join([f"'{v['value']}'" for v in profile['top_values'][:10]])
            description += f"Most common non-NULL column values are {top_values_str}. "

        # Add pattern information
        if 'pattern' in profile:
            pattern = profile['pattern']
            if pattern.get('has_digits') and not pattern.get('has_letters'):
                description += "Every column value looks like a number. "
            elif pattern.get('all_uppercase'):
                description += "All values are in uppercase. "
            elif pattern.get('all_lowercase'):
                description += "All values are in lowercase. "

        return description

    def generate_short_description(self, column_profile: Dict, table_name: str,
                                  other_columns: List[str]) -> str:
        """
        Generate short LLM description of column contents and meaning
        """
        profile_description = self.generate_profile_description(column_profile)
        column_name = column_profile.get('column_name', '')

        prompt = f"""Based on the following information about a database column, provide a SHORT description (1-2 sentences) of what this column stores and its meaning:

Table name: {table_name}
Column name: {column_name}
Other columns in table: {', '.join(other_columns[:10])}
Profile data: {profile_description}

Provide only the short description, focusing on:
1. What the column stores
2. The format/type of the values
3. The likely meaning or purpose

Short description:"""

        try:
            response = self.client.chat.completions.create(
                model=self.config.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": "You are a database expert helping to understand database schema."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=150
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating short description: {e}")
            return f"Column {column_name} in table {table_name}"

    def generate_long_description(self, column_profile: Dict, table_name: str,
                                 other_columns: List[str], short_description: str) -> str:
        """
        Generate long LLM description with detailed value information
        """
        profile_description = self.generate_profile_description(column_profile)
        column_name = column_profile.get('column_name', '')

        # Include sample values if available
        sample_values = ""
        if 'top_values' in column_profile and column_profile['top_values']:
            values = [v['value'] for v in column_profile['top_values'][:5]]
            sample_values = f"Sample values: {', '.join(values)}"

        prompt = f"""Based on the following information about a database column, provide a DETAILED description that includes both the meaning and specific value details:

Table name: {table_name}
Column name: {column_name}
Short description: {short_description}
Profile data: {profile_description}
{sample_values}

Provide a detailed description that:
1. Starts with the short description
2. Then adds specific details about the actual values in the column
3. Includes information about value ranges, formats, and examples
4. Helps an LLM understand what literal values to use in SQL queries

Detailed description:"""

        try:
            response = self.client.chat.completions.create(
                model=self.config.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": "You are a database expert providing detailed column documentation."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=300
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating long description: {e}")
            return short_description + f" {profile_description}"

    def summarize_table(self, table_profile: Dict) -> Dict:
        """
        Generate summaries for all columns in a table
        """
        table_name = table_profile.get('table_name', '')
        columns = table_profile.get('columns', {})

        # Get list of column names for context
        column_names = list(columns.keys())

        summaries = {}
        for col_name, col_profile in columns.items():
            logger.info(f"Generating summaries for {table_name}.{col_name}")

            # Generate short description
            short_desc = self.generate_short_description(
                col_profile, table_name, column_names
            )

            # Generate long description
            long_desc = self.generate_long_description(
                col_profile, table_name, column_names, short_desc
            )

            summaries[col_name] = {
                'short_description': short_desc,
                'long_description': long_desc,
                'profile': col_profile
            }

        return {
            'table_name': table_name,
            'column_summaries': summaries
        }

    def summarize_database(self, database_profile: Dict) -> Dict:
        """
        Generate summaries for all tables in a database
        """
        schema_name = database_profile.get('schema_name', '')
        tables = database_profile.get('tables', {})

        summaries = {}
        for table_name, table_profile in tables.items():
            logger.info(f"Summarizing table: {table_name}")
            table_summary = self.summarize_table(table_profile)
            summaries[table_name] = table_summary

        return {
            'schema_name': schema_name,
            'table_summaries': summaries
        }