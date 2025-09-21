"""
SQLite Database Executor Module
Handles SQLite database operations for BIRD dataset
"""
import sqlite3
import logging
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class SQLiteExecutor:
    """Execute SQL queries on SQLite databases and compare results"""

    def __init__(self, bird_dataset_path: str):
        self.bird_dataset_path = Path(bird_dataset_path)
        self.train_databases_path = self.bird_dataset_path / 'train_databases'
        self.connections = {}

    def get_database_path(self, db_id: str) -> Path:
        """Get the SQLite database file path for a given db_id"""
        db_path = self.train_databases_path / db_id / f"{db_id}.sqlite"
        if not db_path.exists():
            # Try alternate naming patterns
            db_path = self.train_databases_path / db_id / f"{db_id}.db"
        return db_path

    def connect_to_database(self, db_id: str) -> sqlite3.Connection:
        """Connect to a SQLite database"""
        if db_id not in self.connections:
            db_path = self.get_database_path(db_id)
            if not db_path.exists():
                raise FileNotFoundError(f"Database file not found: {db_path}")

            self.connections[db_id] = sqlite3.connect(str(db_path))
            logger.info(f"Connected to SQLite database: {db_id}")

        return self.connections[db_id]

    def execute_sql(self, db_id: str, sql: str, timeout: int = 10) -> Tuple[bool, Any]:
        """
        Execute SQL query on SQLite database
        Returns: (success, result/error)
        """
        try:
            conn = self.connect_to_database(db_id)
            cursor = conn.cursor()

            # Set query timeout
            conn.execute(f"PRAGMA busy_timeout = {timeout * 1000}")

            # Execute query
            cursor.execute(sql)

            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            return True, {'columns': columns, 'rows': results}

        except sqlite3.Error as e:
            logger.error(f"SQLite error executing query on {db_id}: {e}")
            return False, str(e)
        except Exception as e:
            logger.error(f"Error executing query on {db_id}: {e}")
            return False, str(e)

    def get_database_schema(self, db_id: str) -> Dict:
        """Extract complete database schema from SQLite"""
        try:
            conn = self.connect_to_database(db_id)
            cursor = conn.cursor()

            # Get all tables
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]

            schema = {'db_id': db_id, 'tables': {}}

            for table in tables:
                # Get table info (with quotes for table names)
                cursor.execute(f'PRAGMA table_info("{table}")')
                columns = cursor.fetchall()

                # Get foreign keys
                cursor.execute(f'PRAGMA foreign_key_list("{table}")')
                foreign_keys = cursor.fetchall()

                # Get sample data
                cursor.execute(f'SELECT * FROM "{table}" LIMIT 5')
                sample_rows = cursor.fetchall()

                # Get row count
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                row_count = cursor.fetchone()[0]

                schema['tables'][table] = {
                    'columns': {},
                    'foreign_keys': [],
                    'row_count': row_count,
                    'sample_rows': sample_rows
                }

                # Process columns
                for col in columns:
                    col_id, name, dtype, notnull, default, pk = col

                    # Get distinct count and sample values (with error handling)
                    try:
                        cursor.execute(f'SELECT COUNT(DISTINCT "{name}") FROM "{table}"')
                        distinct_count = cursor.fetchone()[0]
                    except:
                        distinct_count = 0

                    try:
                        cursor.execute(f'''
                            SELECT DISTINCT "{name}" FROM "{table}"
                            WHERE "{name}" IS NOT NULL
                            LIMIT 10
                        ''')
                        sample_values = [row[0] for row in cursor.fetchall()]
                    except:
                        sample_values = []

                    schema['tables'][table]['columns'][name] = {
                        'type': dtype,
                        'nullable': not notnull,
                        'primary_key': bool(pk),
                        'default': default,
                        'distinct_count': distinct_count,
                        'sample_values': sample_values
                    }

                # Process foreign keys
                for fk in foreign_keys:
                    # Handle varying number of foreign key columns returned by different SQLite versions
                    if len(fk) >= 5:
                        fk_id, seq, ref_table, from_col, to_col = fk[:5]
                    else:
                        continue  # Skip malformed FK info
                    schema['tables'][table]['foreign_keys'].append({
                        'column': from_col,
                        'referenced_table': ref_table,
                        'referenced_column': to_col
                    })

            return schema

        except Exception as e:
            logger.error(f"Error extracting schema from {db_id}: {e}")
            return {'db_id': db_id, 'tables': {}, 'error': str(e)}

    def compare_results(self, result1: Dict, result2: Dict,
                       ordered: bool = False) -> Tuple[bool, float, str]:
        """
        Compare two SQL query results for semantic equivalence
        Returns: (exact_match, similarity_score, message)
        """
        if not isinstance(result1, dict) or not isinstance(result2, dict):
            return False, 0.0, "Invalid result format"

        rows1 = result1.get('rows', [])
        rows2 = result2.get('rows', [])

        # Check row count
        if len(rows1) != len(rows2):
            similarity = min(len(rows1), len(rows2)) / max(len(rows1), len(rows2)) if max(len(rows1), len(rows2)) > 0 else 0
            return False, similarity, f"Row count mismatch: {len(rows1)} vs {len(rows2)}"

        # Check column count
        if rows1 and rows2:
            if len(rows1[0]) != len(rows2[0]):
                return False, 0.5, f"Column count mismatch: {len(rows1[0])} vs {len(rows2[0])}"

        # Convert to sets for unordered comparison
        if not ordered:
            # Convert rows to tuples for hashing
            set1 = set(tuple(row) for row in rows1)
            set2 = set(tuple(row) for row in rows2)

            if set1 == set2:
                return True, 1.0, "Exact match (unordered)"

            # Calculate similarity based on intersection
            intersection = len(set1 & set2)
            union = len(set1 | set2)
            similarity = intersection / union if union > 0 else 0

            return False, similarity, f"Partial match: {intersection}/{union} rows match"

        else:
            # Ordered comparison
            if rows1 == rows2:
                return True, 1.0, "Exact match (ordered)"

            # Calculate row-by-row similarity
            matching_rows = sum(1 for r1, r2 in zip(rows1, rows2) if r1 == r2)
            similarity = matching_rows / len(rows1) if rows1 else 0

            return False, similarity, f"Partial match: {matching_rows}/{len(rows1)} rows match"

    def close_all_connections(self):
        """Close all database connections"""
        for db_id, conn in self.connections.items():
            conn.close()
            logger.info(f"Closed connection to {db_id}")
        self.connections.clear()


class BIRDSQLiteProfiler:
    """Create detailed profiles from BIRD SQLite databases"""

    def __init__(self, executor: SQLiteExecutor):
        self.executor = executor

    def create_database_profile(self, db_id: str) -> Dict:
        """Create comprehensive database profile for schema linking"""
        schema = self.executor.get_database_schema(db_id)

        if 'error' in schema:
            return schema

        profile = {
            'db_id': db_id,
            'schema_name': db_id,
            'tables': {}
        }

        for table_name, table_info in schema['tables'].items():
            profile['tables'][table_name] = {
                'table_name': table_name,
                'record_count': table_info['row_count'],
                'columns': {},
                'foreign_keys': table_info['foreign_keys']
            }

            for col_name, col_info in table_info['columns'].items():
                profile['tables'][table_name]['columns'][col_name] = {
                    'column_name': col_name,
                    'data_type': col_info['type'],
                    'nullable': col_info['nullable'],
                    'primary_key': col_info['primary_key'],
                    'distinct_count': col_info['distinct_count'],
                    'sample_values': col_info['sample_values'][:5],
                    'top_values': [
                        {'value': val, 'count': 1}  # Simplified for now
                        for val in col_info['sample_values'][:5]
                    ]
                }

        return profile

    def create_table_summaries(self, profile: Dict) -> Dict:
        """Create table summaries for LLM metadata generation"""
        summaries = {'table_summaries': {}}

        for table_name, table_data in profile.get('tables', {}).items():
            summaries['table_summaries'][table_name] = {
                'table_name': table_name,
                'record_count': table_data['record_count'],
                'column_summaries': {}
            }

            for col_name, col_data in table_data.get('columns', {}).items():
                # Generate descriptions based on column characteristics
                short_desc = f"{col_name} ({col_data['data_type']})"

                if col_data.get('primary_key'):
                    short_desc += " - Primary Key"

                long_desc = f"Column {col_name} of type {col_data['data_type']}"
                if col_data['distinct_count'] > 0:
                    long_desc += f" with {col_data['distinct_count']} distinct values"

                if col_data.get('sample_values'):
                    sample_str = ', '.join(str(v) for v in col_data['sample_values'][:3])
                    long_desc += f". Sample values: {sample_str}"

                summaries['table_summaries'][table_name]['column_summaries'][col_name] = {
                    'short_description': short_desc,
                    'long_description': long_desc,
                    'data_type': col_data['data_type'],
                    'is_primary_key': col_data.get('primary_key', False),
                    'is_nullable': col_data.get('nullable', True),
                    'distinct_count': col_data['distinct_count']
                }

        return summaries