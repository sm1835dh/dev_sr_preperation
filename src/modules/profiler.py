"""
Database Profiling Module
Implements database profiling as described in Section 2 of the paper
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter
import re
import numpy as np
from datasketch import MinHash, MinHashLSH
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from configs.config import Config
from modules.database import DatabaseManager

logger = logging.getLogger(__name__)


class DatabaseProfiler:
    """Profile database tables and columns to extract statistics"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.config = Config()

    def profile_table(self, schema_name: str, table_name: str) -> Dict:
        """Profile a single table"""
        full_table = f"{schema_name}.{table_name}"

        # Get record count
        count_query = f"SELECT COUNT(*) as count FROM {full_table}"
        result = self.db_manager.execute_query(count_query)
        record_count = result[0]['count'] if result else 0

        # Get columns
        columns_query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        columns = self.db_manager.execute_query(columns_query, (schema_name, table_name))

        # Profile each column
        column_profiles = {}
        for col in columns:
            col_name = col['column_name']
            col_type = col['data_type']
            profile = self.profile_column(full_table, col_name, col_type, record_count)
            column_profiles[col_name] = profile

        return {
            'table_name': table_name,
            'schema_name': schema_name,
            'record_count': record_count,
            'columns': column_profiles
        }

    def profile_column(self, table_name: str, column_name: str,
                      data_type: str, total_records: int) -> Dict:
        """
        Profile a single column to extract statistics
        Based on Section 2 of the paper
        """
        profile = {
            'column_name': column_name,
            'data_type': data_type,
            'total_records': total_records
        }

        # Count NULL vs non-NULL values
        null_query = f"""
            SELECT
                COUNT(*) FILTER (WHERE "{column_name}" IS NULL) as null_count,
                COUNT(*) FILTER (WHERE "{column_name}" IS NOT NULL) as non_null_count
            FROM {table_name}
        """
        null_result = self.db_manager.execute_query(null_query)
        if null_result:
            profile['null_count'] = null_result[0]['null_count']
            profile['non_null_count'] = null_result[0]['non_null_count']

        # Count distinct values
        distinct_query = f"""
            SELECT COUNT(DISTINCT "{column_name}") as distinct_count
            FROM {table_name}
        """
        distinct_result = self.db_manager.execute_query(distinct_query)
        if distinct_result:
            profile['distinct_count'] = distinct_result[0]['distinct_count']

        # Get data shape based on type
        if 'int' in data_type.lower() or 'numeric' in data_type.lower() or 'real' in data_type.lower():
            # Numeric columns: get min, max, avg
            stats_query = f"""
                SELECT
                    MIN("{column_name}") as min_val,
                    MAX("{column_name}") as max_val,
                    AVG("{column_name}")::FLOAT as avg_val
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
            """
            stats_result = self.db_manager.execute_query(stats_query)
            if stats_result:
                profile['min_value'] = stats_result[0]['min_val']
                profile['max_value'] = stats_result[0]['max_val']
                profile['avg_value'] = stats_result[0]['avg_val']

        elif 'char' in data_type.lower() or 'text' in data_type.lower():
            # Text columns: get length statistics and pattern
            text_stats_query = f"""
                SELECT
                    MIN(LENGTH("{column_name}")) as min_length,
                    MAX(LENGTH("{column_name}")) as max_length,
                    AVG(LENGTH("{column_name}"))::FLOAT as avg_length
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
            """
            text_result = self.db_manager.execute_query(text_stats_query)
            if text_result:
                profile['min_length'] = text_result[0]['min_length']
                profile['max_length'] = text_result[0]['max_length']
                profile['avg_length'] = text_result[0]['avg_length']

            # Analyze character patterns
            sample_query = f"""
                SELECT "{column_name}"
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
                LIMIT 100
            """
            samples = self.db_manager.execute_query(sample_query)
            if samples:
                pattern_info = self._analyze_text_pattern([s[column_name] for s in samples])
                profile.update(pattern_info)

        # Get top-k frequent values
        top_k_query = f"""
            SELECT "{column_name}", COUNT(*) as count
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            GROUP BY "{column_name}"
            ORDER BY count DESC
            LIMIT {self.config.TOP_K_VALUES}
        """
        top_k_result = self.db_manager.execute_query(top_k_query)
        if top_k_result:
            profile['top_values'] = [
                {'value': str(r[column_name]), 'count': r['count']}
                for r in top_k_result
            ]

        # Generate MinHash for the column
        profile['minhash'] = self._generate_minhash(table_name, column_name)

        return profile

    def _analyze_text_pattern(self, samples: List[str]) -> Dict:
        """Analyze text patterns in sample data"""
        if not samples:
            return {}

        patterns = {
            'has_digits': False,
            'has_letters': False,
            'has_special': False,
            'has_spaces': False,
            'all_uppercase': True,
            'all_lowercase': True,
            'common_prefix': None,
            'common_suffix': None
        }

        # Check patterns
        for sample in samples:
            if not sample:
                continue

            if any(c.isdigit() for c in sample):
                patterns['has_digits'] = True
            if any(c.isalpha() for c in sample):
                patterns['has_letters'] = True
            if any(not c.isalnum() and not c.isspace() for c in sample):
                patterns['has_special'] = True
            if ' ' in sample:
                patterns['has_spaces'] = True
            if not sample.isupper():
                patterns['all_uppercase'] = False
            if not sample.islower():
                patterns['all_lowercase'] = False

        # Find common prefix/suffix
        if len(samples) > 1:
            # Common prefix
            prefix = samples[0]
            for s in samples[1:]:
                while prefix and not s.startswith(prefix):
                    prefix = prefix[:-1]
            if len(prefix) > 1:
                patterns['common_prefix'] = prefix

            # Common suffix
            suffix = samples[0]
            for s in samples[1:]:
                while suffix and not s.endswith(suffix):
                    suffix = suffix[1:]
            if len(suffix) > 1:
                patterns['common_suffix'] = suffix

        return {'pattern': patterns}

    def _generate_minhash(self, table_name: str, column_name: str) -> List[int]:
        """
        Generate MinHash signature for a column
        Based on Section 2 of the paper
        """
        # Get sample values
        sample_query = f"""
            SELECT DISTINCT "{column_name}"
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            LIMIT 1000
        """
        results = self.db_manager.execute_query(sample_query)

        if not results:
            return []

        # Create MinHash
        m = MinHash(num_perm=self.config.MINHASH_PERMUTATIONS)
        for row in results:
            value = str(row[column_name])
            m.update(value.encode('utf-8'))

        return list(m.digest())

    def compute_resemblance(self, minhash1: List[int], minhash2: List[int]) -> float:
        """
        Compute resemblance between two MinHash signatures
        res(F, G) = |F ∩ G| / |F ∪ G|
        """
        if not minhash1 or not minhash2:
            return 0.0

        matches = sum(1 for h1, h2 in zip(minhash1, minhash2) if h1 == h2)
        return matches / len(minhash1)

    def profile_database(self, schema_name: str) -> Dict:
        """Profile all tables in a database schema"""
        # Get all tables in schema
        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
        """
        tables = self.db_manager.execute_query(tables_query, (schema_name,))

        profiles = {}
        for table in tables:
            table_name = table['table_name']
            logger.info(f"Profiling table: {schema_name}.{table_name}")
            profile = self.profile_table(schema_name, table_name)
            profiles[table_name] = profile

        return {
            'schema_name': schema_name,
            'tables': profiles
        }

    def find_join_paths(self, profile: Dict) -> List[Dict]:
        """
        Find potential join paths based on MinHash resemblance
        """
        join_candidates = []
        tables = profile.get('tables', {})

        # Compare all column pairs
        for table1, t1_profile in tables.items():
            for col1, c1_profile in t1_profile.get('columns', {}).items():
                minhash1 = c1_profile.get('minhash', [])
                if not minhash1:
                    continue

                for table2, t2_profile in tables.items():
                    if table1 == table2:
                        continue

                    for col2, c2_profile in t2_profile.get('columns', {}).items():
                        minhash2 = c2_profile.get('minhash', [])
                        if not minhash2:
                            continue

                        resemblance = self.compute_resemblance(minhash1, minhash2)
                        if resemblance > self.config.LSH_THRESHOLD:
                            join_candidates.append({
                                'table1': table1,
                                'column1': col1,
                                'table2': table2,
                                'column2': col2,
                                'resemblance': resemblance
                            })

        return join_candidates


if __name__ == "__main__":
    # Test profiling
    logging.basicConfig(level=logging.INFO)

    db_manager = DatabaseManager()
    db_manager.create_database()
    db_manager.connect()

    profiler = DatabaseProfiler(db_manager)

    # Test with a sample schema
    # This would need an actual loaded database to test properly