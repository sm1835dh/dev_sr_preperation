"""
BIRD Dataset Loader Module
Loads sample databases from BIRD dataset into PostgreSQL
"""
import json
import random
import sqlite3
from pathlib import Path
import logging
from typing import List, Dict, Tuple, Optional
import sys
sys.path.append(str(Path(__file__).parent.parent))

from configs.config import Config
from modules.database import DatabaseManager, SQLiteReader

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class BIRDLoader:
    """Load BIRD dataset into PostgreSQL"""

    def __init__(self):
        self.config = Config()
        self.db_manager = DatabaseManager()
        self.bird_path = Path(self.config.BIRD_DATASET_PATH)

    def get_available_databases(self) -> List[str]:
        """Get list of available databases in BIRD dataset"""
        train_db_path = self.bird_path / "train_databases"
        if not train_db_path.exists():
            logger.error(f"BIRD dataset path not found: {train_db_path}")
            return []

        databases = [d.name for d in train_db_path.iterdir() if d.is_dir()]
        logger.info(f"Found {len(databases)} databases in BIRD dataset")
        return databases

    def load_questions(self) -> List[Dict]:
        """Load questions from train.json"""
        train_file = self.bird_path / "train.json"
        if not train_file.exists():
            logger.error(f"train.json not found at {train_file}")
            return []

        with open(train_file, 'r') as f:
            data = json.load(f)
        return data

    def sample_databases(self, n: int = 10) -> List[Tuple[str, Dict]]:
        """
        Randomly sample n databases with their questions
        Returns list of (db_name, question_data) tuples
        """
        questions = self.load_questions()
        if not questions:
            return []

        # Group questions by database
        db_questions = {}
        for q in questions:
            db_id = q.get('db_id', '')
            if db_id not in db_questions:
                db_questions[db_id] = []
            db_questions[db_id].append(q)

        # Sample n databases
        available_dbs = list(db_questions.keys())
        sample_size = min(n, len(available_dbs))
        sampled_dbs = random.sample(available_dbs, sample_size)

        result = []
        for db_name in sampled_dbs:
            # Get one random question for this database
            questions_for_db = db_questions[db_name]
            sample_question = random.choice(questions_for_db)
            result.append((db_name, sample_question))

        logger.info(f"Sampled {len(result)} databases with questions")
        return result

    def load_database_to_postgres(self, db_name: str) -> bool:
        """Load a single SQLite database into PostgreSQL"""
        db_path = self.bird_path / "train_databases" / db_name / f"{db_name}.sqlite"

        if not db_path.exists():
            logger.error(f"Database file not found: {db_path}")
            return False

        try:
            # Read schema and data from SQLite
            schema = SQLiteReader.read_schema(db_path)

            # Connect to PostgreSQL
            if not self.db_manager.connection:
                self.db_manager.connect()

            cursor = self.db_manager.connection.cursor()

            # Create schema in PostgreSQL
            schema_name = f"bird_{db_name}".replace("-", "_")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

            for table_name, columns in schema.items():
                # Build CREATE TABLE statement
                pg_table = f"{schema_name}.{table_name}"

                # Drop table if exists
                cursor.execute(f"DROP TABLE IF EXISTS {pg_table} CASCADE")

                # Create column definitions
                col_defs = []
                for col in columns:
                    col_type = self._sqlite_to_postgres_type(col['type'])
                    col_def = f"\"{col['name']}\" {col_type}"
                    if col['primary_key']:
                        col_def += " PRIMARY KEY"
                    elif not col['nullable']:
                        col_def += " NOT NULL"
                    col_defs.append(col_def)

                create_stmt = f"CREATE TABLE {pg_table} ({', '.join(col_defs)})"
                cursor.execute(create_stmt)

                # Load data
                data = SQLiteReader.read_data(db_path, table_name)
                if data:
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_stmt = f"INSERT INTO {pg_table} VALUES ({placeholders})"
                    cursor.executemany(insert_stmt, data)

            self.db_manager.connection.commit()
            logger.info(f"Successfully loaded database: {db_name}")
            return True

        except Exception as e:
            logger.error(f"Error loading database {db_name}: {e}")
            if self.db_manager.connection:
                self.db_manager.connection.rollback()
            return False

    def _sqlite_to_postgres_type(self, sqlite_type: str) -> str:
        """Convert SQLite type to PostgreSQL type"""
        type_map = {
            'INTEGER': 'INTEGER',
            'TEXT': 'TEXT',
            'REAL': 'REAL',
            'BLOB': 'BYTEA',
            'NUMERIC': 'NUMERIC',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'FLOAT': 'FLOAT',
            'DOUBLE': 'DOUBLE PRECISION',
            'DECIMAL': 'DECIMAL',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'TIME': 'TIME'
        }

        # Handle parameterized types like VARCHAR(255)
        base_type = sqlite_type.split('(')[0].upper()
        if base_type in type_map:
            return type_map[base_type]
        return 'TEXT'  # Default to TEXT

    def load_sample_databases(self) -> List[Dict]:
        """Load sample databases and return their information"""
        # Create database if needed
        self.db_manager.create_database()

        # Sample databases
        samples = self.sample_databases(self.config.SAMPLE_SIZE)

        loaded_samples = []
        for db_name, question in samples:
            success = self.load_database_to_postgres(db_name)
            if success:
                loaded_samples.append({
                    'db_name': db_name,
                    'schema_name': f"bird_{db_name}".replace("-", "_"),
                    'question': question['question'],
                    'sql': question['SQL'],
                    'evidence': question.get('evidence', ''),
                    'question_id': question.get('question_id', '')
                })

        logger.info(f"Successfully loaded {len(loaded_samples)} sample databases")
        return loaded_samples

    def save_samples_metadata(self, samples: List[Dict]):
        """Save sample metadata for later use"""
        metadata_path = self.config.DATA_DIR / "samples_metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        with open(metadata_path, 'w') as f:
            json.dump(samples, f, indent=2)

        logger.info(f"Saved samples metadata to {metadata_path}")


if __name__ == "__main__":
    # Test the loader
    loader = BIRDLoader()

    # Check available databases
    dbs = loader.get_available_databases()
    print(f"Available databases: {len(dbs)}")
    if dbs:
        print(f"First 5 databases: {dbs[:5]}")

    # Load sample databases
    samples = loader.load_sample_databases()
    if samples:
        print(f"\nLoaded {len(samples)} samples")
        loader.save_samples_metadata(samples)
        for sample in samples[:2]:
            print(f"\nDatabase: {sample['db_name']}")
            print(f"Question: {sample['question'][:100]}...")
            print(f"SQL: {sample['sql'][:100]}...")