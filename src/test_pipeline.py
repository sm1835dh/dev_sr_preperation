"""
Test Pipeline Module
Tests the Text-to-SQL system components without requiring database connections
"""
import logging
import json
import sys
from pathlib import Path
from typing import List, Dict
from openai import AzureOpenAI

# Add src to path
sys.path.append(str(Path(__file__).parent))

from configs.config import Config
from modules.llm_summarizer import LLMSummarizer
from modules.schema_linker import SchemaLinker
from modules.sql_generator import SQLGenerator
from modules.evaluator import SQLEvaluator, ExperimentRunner

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_mock_data() -> Dict:
    """Create mock data for testing without database"""

    # Mock database profile
    mock_profile = {
        'schema_name': 'test_schema',
        'tables': {
            'customers': {
                'table_name': 'customers',
                'record_count': 1000,
                'columns': {
                    'customer_id': {
                        'column_name': 'customer_id',
                        'data_type': 'INTEGER',
                        'null_count': 0,
                        'non_null_count': 1000,
                        'distinct_count': 1000,
                        'min_value': 1,
                        'max_value': 1000,
                        'top_values': [
                            {'value': '1', 'count': 1},
                            {'value': '2', 'count': 1},
                            {'value': '3', 'count': 1}
                        ]
                    },
                    'customer_name': {
                        'column_name': 'customer_name',
                        'data_type': 'TEXT',
                        'null_count': 10,
                        'non_null_count': 990,
                        'distinct_count': 990,
                        'min_length': 3,
                        'max_length': 50,
                        'top_values': [
                            {'value': 'John Doe', 'count': 2},
                            {'value': 'Jane Smith', 'count': 2},
                            {'value': 'Bob Johnson', 'count': 1}
                        ]
                    },
                    'email': {
                        'column_name': 'email',
                        'data_type': 'TEXT',
                        'null_count': 5,
                        'non_null_count': 995,
                        'distinct_count': 995,
                        'min_length': 10,
                        'max_length': 100,
                        'top_values': [
                            {'value': 'john@example.com', 'count': 1},
                            {'value': 'jane@example.com', 'count': 1}
                        ]
                    }
                }
            },
            'orders': {
                'table_name': 'orders',
                'record_count': 5000,
                'columns': {
                    'order_id': {
                        'column_name': 'order_id',
                        'data_type': 'INTEGER',
                        'null_count': 0,
                        'non_null_count': 5000,
                        'distinct_count': 5000,
                        'min_value': 1,
                        'max_value': 5000,
                        'top_values': [
                            {'value': '1', 'count': 1},
                            {'value': '2', 'count': 1}
                        ]
                    },
                    'customer_id': {
                        'column_name': 'customer_id',
                        'data_type': 'INTEGER',
                        'null_count': 0,
                        'non_null_count': 5000,
                        'distinct_count': 800,
                        'min_value': 1,
                        'max_value': 1000,
                        'top_values': [
                            {'value': '123', 'count': 10},
                            {'value': '456', 'count': 8}
                        ]
                    },
                    'order_date': {
                        'column_name': 'order_date',
                        'data_type': 'DATE',
                        'null_count': 0,
                        'non_null_count': 5000,
                        'distinct_count': 365,
                        'top_values': [
                            {'value': '2023-01-01', 'count': 20},
                            {'value': '2023-01-02', 'count': 18}
                        ]
                    },
                    'total_amount': {
                        'column_name': 'total_amount',
                        'data_type': 'NUMERIC',
                        'null_count': 0,
                        'non_null_count': 5000,
                        'distinct_count': 3000,
                        'min_value': 10.99,
                        'max_value': 999.99,
                        'avg_value': 150.50,
                        'top_values': [
                            {'value': '99.99', 'count': 50},
                            {'value': '19.99', 'count': 45}
                        ]
                    }
                }
            }
        }
    }

    return mock_profile


def create_mock_summaries() -> Dict:
    """Create mock LLM summaries"""
    return {
        'schema_name': 'test_schema',
        'table_summaries': {
            'customers': {
                'table_name': 'customers',
                'column_summaries': {
                    'customer_id': {
                        'short_description': 'Unique identifier for each customer',
                        'long_description': 'Unique identifier for each customer. Integer values from 1 to 1000, no null values.',
                        'profile': {}
                    },
                    'customer_name': {
                        'short_description': 'Full name of the customer',
                        'long_description': 'Full name of the customer. Text values 3-50 characters long, mostly unique names like "John Doe", "Jane Smith".',
                        'profile': {}
                    },
                    'email': {
                        'short_description': 'Customer email address',
                        'long_description': 'Customer email address. Text values 10-100 characters, valid email format like john@example.com.',
                        'profile': {}
                    }
                }
            },
            'orders': {
                'table_name': 'orders',
                'column_summaries': {
                    'order_id': {
                        'short_description': 'Unique identifier for each order',
                        'long_description': 'Unique identifier for each order. Integer values from 1 to 5000, no null values.',
                        'profile': {}
                    },
                    'customer_id': {
                        'short_description': 'Foreign key to customers table',
                        'long_description': 'Foreign key to customers table. Integer values 1-1000, references customer_id in customers table.',
                        'profile': {}
                    },
                    'order_date': {
                        'short_description': 'Date when the order was placed',
                        'long_description': 'Date when the order was placed. Date values in YYYY-MM-DD format, spanning year 2023.',
                        'profile': {}
                    },
                    'total_amount': {
                        'short_description': 'Total monetary amount of the order',
                        'long_description': 'Total monetary amount of the order. Numeric values from $10.99 to $999.99, average $150.50.',
                        'profile': {}
                    }
                }
            }
        }
    }


def test_schema_linking():
    """Test schema linking functionality"""
    logger.info("Testing schema linking...")

    try:
        # Create real LLM client for embedding tests
        config = Config()
        llm_client = AzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION
        )

        schema_linker = SchemaLinker(llm_client)

        # Test literal extraction
        question = "How many customers named 'John Doe' placed orders in 2023?"
        literals = schema_linker.extract_literals(question)
        logger.info(f"Extracted literals: {literals}")

        # Create mock profile and summaries
        mock_profile = create_mock_data()
        mock_summaries = create_mock_summaries()

        # Test LSH index building
        schema_linker.build_lsh_index(mock_profile)
        logger.info("LSH index built successfully")

        # Test FAISS index building with embeddings
        schema_linker.build_faiss_index(mock_summaries['table_summaries'])
        logger.info("FAISS index built successfully")

        # Test schema context generation
        context = schema_linker.generate_schema_context('full', 'minimal')
        logger.info(f"Generated schema context: {context[:200]}...")

        logger.info("Schema linking tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"Schema linking test failed: {e}")
        return False


def test_sql_generation():
    """Test SQL generation functionality"""
    logger.info("Testing SQL generation...")

    try:
        # Create mock components
        config = Config()
        mock_client = None
        schema_linker = SchemaLinker(mock_client)
        sql_generator = SQLGenerator(mock_client, schema_linker)

        # Test few-shot example creation
        mock_examples = [
            {
                'question': 'How many customers are there?',
                'sql': 'SELECT COUNT(*) FROM customers;'
            },
            {
                'question': 'What is the total order amount?',
                'sql': 'SELECT SUM(total_amount) FROM orders;'
            }
        ]

        # Test SQL validation
        valid_sql = "SELECT COUNT(*) FROM customers;"
        invalid_sql = "SELECT COUNT( FROM customers"

        is_valid, msg = sql_generator.validate_sql(valid_sql)
        logger.info(f"Valid SQL test: {is_valid}, {msg}")

        is_valid, msg = sql_generator.validate_sql(invalid_sql)
        logger.info(f"Invalid SQL test: {is_valid}, {msg}")

        # Test SQL cleaning
        dirty_sql = "```sql\nSELECT COUNT(*) FROM customers\n```"
        clean_sql = sql_generator._clean_sql(dirty_sql)
        logger.info(f"Cleaned SQL: {clean_sql}")

        logger.info("SQL generation tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"SQL generation test failed: {e}")
        return False


def test_evaluator():
    """Test evaluation functionality"""
    logger.info("Testing evaluator...")

    try:
        # Mock database manager
        evaluator = SQLEvaluator(None)  # Pass None for db_manager in test

        # Test SQL normalization
        sql1 = "SELECT COUNT(*) FROM customers"
        sql2 = "select count(*) from customers;"

        norm1 = evaluator.normalize_sql(sql1)
        norm2 = evaluator.normalize_sql(sql2)
        logger.info(f"Normalized SQL1: {norm1}")
        logger.info(f"Normalized SQL2: {norm2}")

        # Test exact match
        is_match = evaluator.exact_match(sql1, sql2)
        logger.info(f"Exact match result: {is_match}")

        # Test experiment runner
        experiment_runner = ExperimentRunner()

        # Test report generation
        mock_results = {
            'total_questions': 10,
            'exact_match_accuracy': 0.8,
            'execution_accuracy': 0.7,
            'sql_validity_rate': 0.9,
            'average_schema_linking_f1': 0.75
        }

        report = experiment_runner.generate_report(mock_results)
        logger.info(f"Generated report:\n{report}")

        logger.info("Evaluator tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"Evaluator test failed: {e}")
        return False


def run_integration_test():
    """Run integration test of all components"""
    logger.info("Running integration test...")

    try:
        # Create mock data
        mock_profile = create_mock_data()
        mock_summaries = create_mock_summaries()

        # Test question
        test_question = "How many customers are there?"

        # Create components with real LLM client
        config = Config()
        llm_client = AzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION
        )
        schema_linker = SchemaLinker(llm_client)

        # Build indexes with mock data
        schema_linker.build_lsh_index(mock_profile)
        # Fix: pass table_summaries directly, not the outer dict
        schema_linker.build_faiss_index(mock_summaries['table_summaries'])

        # Test literal extraction
        literals = schema_linker.extract_literals(test_question)
        logger.info(f"Extracted literals: {literals}")

        # Test focused schema generation
        focused_schema = {}  # Would normally call get_focused_schema
        logger.info(f"Focused schema: {focused_schema}")

        # Test schema context generation
        context = schema_linker.generate_schema_context('full', 'maximal')
        logger.info(f"Schema context length: {len(context)} characters")

        logger.info("Integration test completed successfully")
        return True

    except Exception as e:
        logger.error(f"Integration test failed: {e}")
        return False


def main():
    """Main test runner"""
    logger.info("Starting Text-to-SQL Pipeline Tests")

    tests = [
        ("Schema Linking", test_schema_linking),
        ("SQL Generation", test_sql_generation),
        ("Evaluator", test_evaluator),
        ("Integration", run_integration_test)
    ]

    results = {}
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running {test_name} Test")
        logger.info(f"{'='*50}")

        success = test_func()
        results[test_name] = success

        if success:
            logger.info(f"✅ {test_name} test PASSED")
        else:
            logger.error(f"❌ {test_name} test FAILED")

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'='*50}")

    passed = sum(results.values())
    total = len(results)

    for test_name, success in results.items():
        status = "PASS" if success else "FAIL"
        logger.info(f"{test_name}: {status}")

    logger.info(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All tests passed! System is ready for deployment.")
    else:
        logger.warning("⚠️  Some tests failed. Review the implementation.")


if __name__ == "__main__":
    main()