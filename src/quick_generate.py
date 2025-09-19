"""
Quick generation of database files without full pipeline processing
"""
import logging
import json
from pathlib import Path
from typing import Dict, List
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent))

from configs.config import Config
from modules.bird_loader import BIRDLoader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuickDatabaseFileGenerator:
    """Quick generation of database files"""

    def __init__(self):
        self.config = Config()
        self.bird_loader = BIRDLoader()

        # Output directory
        self.output_dir = Path(self.config.DATA_DIR) / "database_files"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def create_mock_metadata(self, db_name: str, question_data: Dict) -> Dict:
        """Create mock metadata based on SQL analysis"""
        sql = question_data.get('SQL', '')
        question = question_data.get('question', '')

        # Simple table extraction
        import re
        table_matches = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', sql, re.IGNORECASE)
        tables = []
        for match in table_matches:
            table = match[0] or match[1]
            if table and table not in tables:
                tables.append(table)

        if not tables:
            tables = ['main_table']

        # Column extraction
        column_matches = re.findall(r'(\w+)\.(\w+)|\b(SELECT|WHERE|ORDER|GROUP)\s+(\w+)', sql, re.IGNORECASE)
        columns = set()
        for match in column_matches:
            if match[1]:  # table.column format
                columns.add(match[1])
            elif match[3] and match[2].upper() not in ['SELECT', 'FROM', 'WHERE', 'ORDER', 'GROUP']:
                columns.add(match[3])

        metadata = {
            'database_name': db_name,
            'original_question': question,
            'original_sql': sql,
            'evidence': question_data.get('evidence', ''),
            'question_id': question_data.get('question_id', ''),
            'extracted_tables': tables,
            'extracted_columns': list(columns),
            'analysis': {
                'sql_complexity': len(sql.split()),
                'has_joins': 'JOIN' in sql.upper(),
                'has_aggregation': any(func in sql.upper() for func in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']),
                'has_groupby': 'GROUP BY' in sql.upper(),
                'has_orderby': 'ORDER BY' in sql.upper(),
                'has_where': 'WHERE' in sql.upper()
            }
        }

        return metadata

    def generate_database_file_content(self, metadata: Dict) -> str:
        """Generate file content for database"""
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append(f"DATABASE: {metadata['database_name']}")
        lines.append("=" * 80)
        lines.append("")

        # Basic Information
        lines.append("BASIC INFORMATION:")
        lines.append(f"  Database Name: {metadata['database_name']}")
        lines.append(f"  Question ID: {metadata.get('question_id', 'N/A')}")
        lines.append("")

        # Original Question and SQL
        lines.append("ORIGINAL QUESTION & SQL:")
        lines.append(f"  Question: {metadata['original_question']}")
        lines.append(f"  Original SQL: {metadata['original_sql']}")
        if metadata.get('evidence'):
            lines.append(f"  Evidence: {metadata['evidence']}")
        lines.append("")

        # SQL Analysis
        analysis = metadata.get('analysis', {})
        lines.append("SQL ANALYSIS:")
        lines.append(f"  SQL Complexity (word count): {analysis.get('sql_complexity', 0)}")
        lines.append(f"  Has JOINs: {analysis.get('has_joins', False)}")
        lines.append(f"  Has Aggregation: {analysis.get('has_aggregation', False)}")
        lines.append(f"  Has GROUP BY: {analysis.get('has_groupby', False)}")
        lines.append(f"  Has ORDER BY: {analysis.get('has_orderby', False)}")
        lines.append(f"  Has WHERE: {analysis.get('has_where', False)}")
        lines.append("")

        # Extracted Schema Elements
        lines.append("EXTRACTED SCHEMA ELEMENTS:")
        lines.append(f"  Tables: {', '.join(metadata.get('extracted_tables', []))}")
        lines.append(f"  Columns: {', '.join(metadata.get('extracted_columns', []))}")
        lines.append("")

        # Schema Complexity
        tables_count = len(metadata.get('extracted_tables', []))
        columns_count = len(metadata.get('extracted_columns', []))

        lines.append("SCHEMA COMPLEXITY:")
        lines.append(f"  Number of Tables: {tables_count}")
        lines.append(f"  Number of Columns: {columns_count}")

        if tables_count > 1:
            complexity = "High" if tables_count > 3 else "Medium"
        else:
            complexity = "Low"
        lines.append(f"  Complexity Level: {complexity}")
        lines.append("")

        # Question Type Analysis
        question = metadata['original_question'].lower()
        question_types = []

        if any(word in question for word in ['how many', 'count', 'number']):
            question_types.append("COUNT")
        if any(word in question for word in ['average', 'avg', 'mean']):
            question_types.append("AVERAGE")
        if any(word in question for word in ['total', 'sum']):
            question_types.append("SUM")
        if any(word in question for word in ['maximum', 'max', 'highest']):
            question_types.append("MAX")
        if any(word in question for word in ['minimum', 'min', 'lowest']):
            question_types.append("MIN")
        if any(word in question for word in ['list', 'show', 'display', 'name']):
            question_types.append("LIST")
        if any(word in question for word in ['when', 'where', 'which']):
            question_types.append("FILTER")

        lines.append("QUESTION TYPE ANALYSIS:")
        lines.append(f"  Detected Types: {', '.join(question_types) if question_types else 'GENERAL'}")
        lines.append("")

        # Text-to-SQL Challenge Assessment
        challenges = []
        if tables_count > 2:
            challenges.append("Multi-table joins")
        if analysis.get('has_aggregation'):
            challenges.append("Aggregation functions")
        if analysis.get('has_groupby'):
            challenges.append("Grouping operations")
        if 'CAST' in metadata['original_sql']:
            challenges.append("Type casting")
        if len([c for c in metadata.get('extracted_columns', []) if 'date' in c.lower()]) > 0:
            challenges.append("Date operations")

        lines.append("TEXT-TO-SQL CHALLENGES:")
        if challenges:
            for challenge in challenges:
                lines.append(f"  - {challenge}")
        else:
            lines.append("  - Basic SELECT query")
        lines.append("")

        # Footer
        lines.append("=" * 80)
        lines.append("Generated by Text-to-SQL System")
        lines.append(f"Database processed from BIRD dataset")
        lines.append("=" * 80)

        return "\n".join(lines)

    def run_quick_generation(self):
        """Run quick generation for all databases"""
        logger.info("Starting quick database file generation")

        # Load sample databases
        samples = self.bird_loader.sample_databases(self.config.SAMPLE_SIZE)

        if not samples:
            logger.error("No samples loaded")
            return

        logger.info(f"Processing {len(samples)} databases...")

        results = []
        for i, (db_name, question_data) in enumerate(samples, 1):
            logger.info(f"Processing {i}/{len(samples)}: {db_name}")

            # Create metadata
            metadata = self.create_mock_metadata(db_name, question_data)
            results.append(metadata)

            # Generate file content
            file_content = self.generate_database_file_content(metadata)

            # Save file
            output_file = self.output_dir / f"{db_name}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(file_content)

            logger.info(f"Generated: {output_file}")

        # Generate summary
        self.generate_summary(results)

        logger.info(f"Quick generation completed! Generated {len(results)} files in {self.output_dir}")

    def generate_summary(self, results: List[Dict]):
        """Generate summary file"""
        lines = []

        lines.append("=" * 80)
        lines.append("TEXT-TO-SQL DATABASE PROCESSING SUMMARY")
        lines.append("=" * 80)
        lines.append("")

        lines.append(f"Total Databases: {len(results)}")
        lines.append("")

        # Statistics
        total_tables = sum(len(r.get('extracted_tables', [])) for r in results)
        total_columns = sum(len(r.get('extracted_columns', [])) for r in results)

        with_joins = sum(1 for r in results if r.get('analysis', {}).get('has_joins', False))
        with_aggregation = sum(1 for r in results if r.get('analysis', {}).get('has_aggregation', False))

        lines.append("OVERALL STATISTICS:")
        lines.append(f"  Total Tables: {total_tables}")
        lines.append(f"  Total Columns: {total_columns}")
        lines.append(f"  Queries with JOINs: {with_joins}")
        lines.append(f"  Queries with Aggregation: {with_aggregation}")
        lines.append("")

        lines.append("DATABASE LIST:")
        for result in results:
            lines.append(f"  - {result['database_name']}")
            analysis = result.get('analysis', {})
            complexity = "High" if len(result.get('extracted_tables', [])) > 3 else "Medium" if len(result.get('extracted_tables', [])) > 1 else "Low"
            lines.append(f"    Complexity: {complexity}, Tables: {len(result.get('extracted_tables', []))}, Joins: {analysis.get('has_joins', False)}")
        lines.append("")

        lines.append("FILES GENERATED:")
        for result in results:
            lines.append(f"  - {result['database_name']}.txt")
        lines.append("")

        lines.append("=" * 80)

        # Save summary
        summary_file = self.output_dir / "summary.txt"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))

        logger.info(f"Generated summary: {summary_file}")


def main():
    """Main entry point"""
    logger.info("Starting Quick Database File Generation")

    try:
        generator = QuickDatabaseFileGenerator()
        generator.run_quick_generation()

        logger.info("Quick generation completed successfully!")

    except Exception as e:
        logger.error(f"Generation failed: {e}")
        raise


if __name__ == "__main__":
    main()