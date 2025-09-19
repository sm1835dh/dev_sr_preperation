"""
Generate database-specific metadata and Q-SQL files
Creates {database_name}.txt files with metadata, questions, and SQL pairs
"""
import logging
import json
from pathlib import Path
from typing import Dict, List
import sys
from openai import AzureOpenAI

# Add src to path
sys.path.append(str(Path(__file__).parent))

from configs.config import Config
from modules.database import DatabaseManager
from modules.bird_loader import BIRDLoader
from modules.profiler import DatabaseProfiler
from modules.llm_summarizer import LLMSummarizer
from modules.schema_linker import SchemaLinker
from modules.sql_generator import SQLGenerator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseFileGenerator:
    """Generate database-specific metadata and Q-SQL files"""

    def __init__(self):
        self.config = Config()
        self.db_manager = DatabaseManager()
        self.llm_client = AzureOpenAI(
            azure_endpoint=self.config.AZURE_OPENAI_ENDPOINT,
            api_key=self.config.AZURE_OPENAI_KEY,
            api_version=self.config.AZURE_OPENAI_API_VERSION
        )

        # Initialize components
        self.bird_loader = BIRDLoader()
        self.profiler = DatabaseProfiler(self.db_manager)
        self.summarizer = LLMSummarizer()
        self.schema_linker = SchemaLinker(self.llm_client)
        self.sql_generator = SQLGenerator(self.llm_client, self.schema_linker)

        # Output directory
        self.output_dir = Path(self.config.DATA_DIR) / "database_files"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_single_database(self, db_name: str, question_data: Dict) -> Dict:
        """Process a single database and generate metadata"""
        logger.info(f"Processing database: {db_name}")

        try:
            # Load database to PostgreSQL (if not using database)
            # For this demo, we'll use mock data based on BIRD structure
            schema_name = f"bird_{db_name}".replace("-", "_")

            # Create mock profile based on question and SQL
            mock_profile = self.create_mock_profile_from_sql(db_name, question_data)

            # Generate LLM summaries
            logger.info(f"Generating LLM summaries for {db_name}")
            summaries = self.summarizer.summarize_database(mock_profile)

            # Build schema linking indexes
            logger.info(f"Building schema indexes for {db_name}")
            self.schema_linker.build_lsh_index(mock_profile)
            self.schema_linker.build_faiss_index(summaries['table_summaries'])

            # Generate focused schema for the question
            question = question_data['question']
            focused_schema = self.schema_linker.get_focused_schema(question)

            # Generate schema context
            schema_context = self.schema_linker.generate_schema_context(
                'focused', 'maximal', focused_schema
            )

            # Process question through SQL generation pipeline
            sql_result = self.process_question_to_sql(question, mock_profile, summaries)

            result = {
                'database_name': db_name,
                'schema_name': schema_name,
                'original_question': question,
                'original_sql': question_data.get('SQL', ''),
                'evidence': question_data.get('evidence', ''),
                'question_id': question_data.get('question_id', ''),
                'database_profile': mock_profile,
                'llm_summaries': summaries,
                'focused_schema': focused_schema,
                'schema_context': schema_context,
                'sql_generation_result': sql_result,
                'metadata': {
                    'total_tables': len(mock_profile.get('tables', {})),
                    'total_fields': sum(len(table.get('columns', {}))
                                      for table in mock_profile.get('tables', {}).values()),
                    'focused_tables': len(focused_schema),
                    'focused_fields': sum(len(cols) for cols in focused_schema.values())
                }
            }

            return result

        except Exception as e:
            logger.error(f"Error processing database {db_name}: {e}")
            return {
                'database_name': db_name,
                'error': str(e),
                'status': 'failed'
            }

    def create_mock_profile_from_sql(self, db_name: str, question_data: Dict) -> Dict:
        """Create mock database profile based on SQL and question"""
        sql = question_data.get('SQL', '')
        question = question_data.get('question', '')

        # Extract table names from SQL (simple extraction)
        import re
        table_pattern = r'FROM\s+(\w+)|JOIN\s+(\w+)'
        tables = []
        for match in re.finditer(table_pattern, sql, re.IGNORECASE):
            table_name = match.group(1) or match.group(2)
            if table_name and table_name not in tables:
                tables.append(table_name)

        # If no tables found, create default ones
        if not tables:
            tables = ['main_table']

        # Create mock profile
        profile = {
            'schema_name': f"bird_{db_name}".replace("-", "_"),
            'tables': {}
        }

        for table in tables:
            # Create mock columns based on common patterns
            columns = self.generate_mock_columns_for_table(table, sql, question)

            profile['tables'][table] = {
                'table_name': table,
                'record_count': 1000,  # Mock count
                'columns': columns
            }

        return profile

    def generate_mock_columns_for_table(self, table_name: str, sql: str, question: str) -> Dict:
        """Generate mock columns based on table name and SQL context"""
        columns = {}

        # Common column patterns
        id_col = f"{table_name}_id"
        columns[id_col] = {
            'column_name': id_col,
            'data_type': 'INTEGER',
            'null_count': 0,
            'non_null_count': 1000,
            'distinct_count': 1000,
            'min_value': 1,
            'max_value': 1000,
            'top_values': [{'value': '1', 'count': 1}, {'value': '2', 'count': 1}]
        }

        # Extract column references from SQL
        import re
        column_pattern = r'(\w+)\s*[=<>!]|SELECT\s+(\w+)|ORDER\s+BY\s+(\w+)|GROUP\s+BY\s+(\w+)'

        sql_columns = set()
        for match in re.finditer(column_pattern, sql, re.IGNORECASE):
            for group in match.groups():
                if group and group.lower() not in ['select', 'from', 'where', 'order', 'group', 'by']:
                    sql_columns.add(group)

        # Add columns found in SQL
        for col in sql_columns:
            if col not in columns:
                # Determine column type based on name patterns
                if 'id' in col.lower():
                    data_type = 'INTEGER'
                    mock_data = {
                        'min_value': 1,
                        'max_value': 1000,
                        'top_values': [{'value': '1', 'count': 1}]
                    }
                elif any(word in col.lower() for word in ['name', 'title', 'description']):
                    data_type = 'VARCHAR'
                    mock_data = {
                        'min_length': 3,
                        'max_length': 50,
                        'top_values': [{'value': 'Sample Name', 'count': 10}]
                    }
                elif any(word in col.lower() for word in ['date', 'time']):
                    data_type = 'DATE'
                    mock_data = {
                        'top_values': [{'value': '2023-01-01', 'count': 5}]
                    }
                elif any(word in col.lower() for word in ['price', 'amount', 'salary', 'cost']):
                    data_type = 'NUMERIC'
                    mock_data = {
                        'min_value': 0,
                        'max_value': 100000,
                        'avg_value': 50000,
                        'top_values': [{'value': '1000', 'count': 5}]
                    }
                else:
                    data_type = 'VARCHAR'
                    mock_data = {
                        'min_length': 1,
                        'max_length': 100,
                        'top_values': [{'value': 'Sample Value', 'count': 5}]
                    }

                columns[col] = {
                    'column_name': col,
                    'data_type': data_type,
                    'null_count': 10,
                    'non_null_count': 990,
                    'distinct_count': 500,
                    **mock_data
                }

        # Add some common columns if none found
        if len(columns) == 1:  # Only ID column
            columns['name'] = {
                'column_name': 'name',
                'data_type': 'VARCHAR',
                'null_count': 5,
                'non_null_count': 995,
                'distinct_count': 950,
                'min_length': 3,
                'max_length': 50,
                'top_values': [{'value': 'Sample Name', 'count': 2}]
            }

        return columns

    def process_question_to_sql(self, question: str, profile: Dict, summaries: Dict) -> Dict:
        """Process question through SQL generation pipeline"""
        try:
            # Create simple few-shot examples
            examples = [
                {
                    'question': 'How many records are there?',
                    'sql': 'SELECT COUNT(*) FROM table;'
                },
                {
                    'question': 'What is the average value?',
                    'sql': 'SELECT AVG(column) FROM table;'
                }
            ]

            self.sql_generator.build_few_shot_index(examples)

            # Generate SQL using pipeline
            result = self.sql_generator.generate_sql(question, profile, summaries)

            return result

        except Exception as e:
            logger.error(f"Error in SQL generation: {e}")
            return {
                'question': question,
                'error': str(e),
                'final_sql': '',
                'is_valid': False
            }

    def generate_database_file(self, db_result: Dict) -> str:
        """Generate formatted text file content for database"""
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append(f"DATABASE: {db_result['database_name']}")
        lines.append("=" * 80)
        lines.append("")

        # Basic Information
        lines.append("BASIC INFORMATION:")
        lines.append(f"  Database Name: {db_result['database_name']}")
        lines.append(f"  Schema Name: {db_result.get('schema_name', 'N/A')}")
        lines.append(f"  Question ID: {db_result.get('question_id', 'N/A')}")
        lines.append("")

        # Original Question and SQL
        lines.append("ORIGINAL QUESTION & SQL:")
        lines.append(f"  Question: {db_result['original_question']}")
        lines.append(f"  Original SQL: {db_result.get('original_sql', 'N/A')}")
        if db_result.get('evidence'):
            lines.append(f"  Evidence: {db_result['evidence']}")
        lines.append("")

        # Database Profile
        if 'database_profile' in db_result:
            profile = db_result['database_profile']
            lines.append("DATABASE PROFILE:")

            for table_name, table_data in profile.get('tables', {}).items():
                lines.append(f"  Table: {table_name}")
                lines.append(f"    Record Count: {table_data.get('record_count', 'N/A')}")
                lines.append(f"    Columns:")

                for col_name, col_data in table_data.get('columns', {}).items():
                    lines.append(f"      - {col_name} ({col_data.get('data_type', 'UNKNOWN')})")
                    lines.append(f"        NULL: {col_data.get('null_count', 0)}, Non-NULL: {col_data.get('non_null_count', 0)}")
                    lines.append(f"        Distinct: {col_data.get('distinct_count', 0)}")

                    if 'top_values' in col_data:
                        top_vals = [v['value'] for v in col_data['top_values'][:3]]
                        lines.append(f"        Top Values: {', '.join(top_vals)}")
                lines.append("")

        # LLM Summaries
        if 'llm_summaries' in db_result:
            summaries = db_result['llm_summaries']
            lines.append("LLM-GENERATED SUMMARIES:")

            for table_name, table_summary in summaries.get('table_summaries', {}).items():
                lines.append(f"  Table: {table_name}")

                for col_name, col_summary in table_summary.get('column_summaries', {}).items():
                    lines.append(f"    Column: {col_name}")
                    lines.append(f"      Short: {col_summary.get('short_description', 'N/A')}")
                    lines.append(f"      Long: {col_summary.get('long_description', 'N/A')}")
                lines.append("")

        # Focused Schema
        if 'focused_schema' in db_result:
            focused = db_result['focused_schema']
            lines.append("FOCUSED SCHEMA (Relevant to Question):")
            for table, columns in focused.items():
                lines.append(f"  Table {table}: {', '.join(columns)}")
            lines.append("")

        # SQL Generation Result
        if 'sql_generation_result' in db_result:
            sql_result = db_result['sql_generation_result']
            lines.append("SQL GENERATION RESULT:")
            lines.append(f"  Generated SQL: {sql_result.get('final_sql', 'N/A')}")
            lines.append(f"  Valid: {sql_result.get('is_valid', False)}")
            lines.append(f"  Validation Message: {sql_result.get('validation_message', 'N/A')}")

            if 'sql_candidates' in sql_result:
                lines.append(f"  Candidates Generated: {len(sql_result['sql_candidates'])}")
                for i, candidate in enumerate(sql_result['sql_candidates'][:3], 1):
                    lines.append(f"    {i}. {candidate}")
            lines.append("")

        # Metadata Statistics
        if 'metadata' in db_result:
            meta = db_result['metadata']
            lines.append("METADATA STATISTICS:")
            lines.append(f"  Total Tables: {meta.get('total_tables', 0)}")
            lines.append(f"  Total Fields: {meta.get('total_fields', 0)}")
            lines.append(f"  Focused Tables: {meta.get('focused_tables', 0)}")
            lines.append(f"  Focused Fields: {meta.get('focused_fields', 0)}")
            lines.append("")

        # Schema Context (truncated)
        if 'schema_context' in db_result:
            context = db_result['schema_context']
            lines.append("SCHEMA CONTEXT (First 500 chars):")
            lines.append(f"  {context[:500]}...")
            lines.append("")

        lines.append("=" * 80)
        lines.append("END OF DATABASE FILE")
        lines.append("=" * 80)

        return "\n".join(lines)

    def run_complete_pipeline(self):
        """Run the complete pipeline for all sampled databases"""
        logger.info("Starting complete pipeline for all databases")

        # Load sample databases
        logger.info("Loading sample databases from BIRD dataset...")
        samples = self.bird_loader.sample_databases(self.config.SAMPLE_SIZE)

        if not samples:
            logger.error("No samples loaded from BIRD dataset")
            return

        logger.info(f"Processing {len(samples)} databases...")

        results = []
        for i, (db_name, question_data) in enumerate(samples, 1):
            logger.info(f"Processing {i}/{len(samples)}: {db_name}")

            # Process single database
            result = self.process_single_database(db_name, question_data)
            results.append(result)

            # Generate database file
            file_content = self.generate_database_file(result)

            # Save to file
            output_file = self.output_dir / f"{db_name}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(file_content)

            logger.info(f"Generated file: {output_file}")

        # Generate summary file
        self.generate_summary_file(results)

        logger.info(f"Pipeline completed! Generated {len(results)} database files in {self.output_dir}")

    def generate_summary_file(self, results: List[Dict]):
        """Generate summary file with all databases"""
        lines = []

        lines.append("=" * 80)
        lines.append("TEXT-TO-SQL SYSTEM - DATABASE PROCESSING SUMMARY")
        lines.append("=" * 80)
        lines.append("")

        successful = [r for r in results if 'error' not in r]
        failed = [r for r in results if 'error' in r]

        lines.append(f"Total Databases Processed: {len(results)}")
        lines.append(f"Successful: {len(successful)}")
        lines.append(f"Failed: {len(failed)}")
        lines.append("")

        if successful:
            lines.append("SUCCESSFUL DATABASES:")
            for result in successful:
                lines.append(f"  - {result['database_name']}")
                if 'sql_generation_result' in result:
                    sql_valid = result['sql_generation_result'].get('is_valid', False)
                    lines.append(f"    SQL Valid: {sql_valid}")
        lines.append("")

        if failed:
            lines.append("FAILED DATABASES:")
            for result in failed:
                lines.append(f"  - {result['database_name']}: {result.get('error', 'Unknown error')}")
        lines.append("")

        # Overall statistics
        if successful:
            total_tables = sum(r['metadata']['total_tables'] for r in successful if 'metadata' in r)
            total_fields = sum(r['metadata']['total_fields'] for r in successful if 'metadata' in r)
            valid_sqls = sum(1 for r in successful
                           if 'sql_generation_result' in r and r['sql_generation_result'].get('is_valid', False))

            lines.append("OVERALL STATISTICS:")
            lines.append(f"  Total Tables Processed: {total_tables}")
            lines.append(f"  Total Fields Processed: {total_fields}")
            lines.append(f"  Valid SQL Generated: {valid_sqls}/{len(successful)}")
            lines.append("")

        lines.append("FILES GENERATED:")
        for result in results:
            if 'error' not in result:
                lines.append(f"  - {result['database_name']}.txt")
        lines.append("")

        lines.append("=" * 80)

        # Save summary
        summary_file = self.output_dir / "summary.txt"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))

        logger.info(f"Generated summary file: {summary_file}")


def main():
    """Main entry point"""
    logger.info("Starting Database File Generation")

    try:
        generator = DatabaseFileGenerator()
        generator.run_complete_pipeline()

        logger.info("Database file generation completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()