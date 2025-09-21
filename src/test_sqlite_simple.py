"""
Simple test for SQLite execution
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from modules.sqlite_executor import SQLiteExecutor, BIRDSQLiteProfiler
from configs.config import Config

config = Config()

# Test basic SQLite connection and schema extraction
executor = SQLiteExecutor(config.BIRD_DATASET_PATH)

# Test a specific database
db_id = 'sales'
print(f"Testing database: {db_id}")

try:
    # Test schema extraction
    schema = executor.get_database_schema(db_id)
    print(f"Schema extracted. Tables: {list(schema.get('tables', {}).keys())}")

    # Test SQL execution
    test_sql = "SELECT * FROM sales LIMIT 1"
    success, result = executor.execute_sql(db_id, test_sql)
    print(f"SQL execution success: {success}")
    if success:
        print(f"Result rows: {len(result['rows'])}")
    else:
        print(f"Error: {result}")

    # Test profile creation
    profiler = BIRDSQLiteProfiler(executor)
    profile = profiler.create_database_profile(db_id)
    print(f"Profile created. Tables in profile: {list(profile.get('tables', {}).keys())}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

executor.close_all_connections()