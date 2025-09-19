"""
Demo script showing the Text-to-SQL system in action
"""
import logging
import sys
from pathlib import Path
from openai import AzureOpenAI

# Add src to path
sys.path.append(str(Path(__file__).parent))

from configs.config import Config
from modules.schema_linker import SchemaLinker
from modules.sql_generator import SQLGenerator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_data():
    """Create sample database profile and summaries"""

    # Sample database profile
    profile = {
        'schema_name': 'company_db',
        'tables': {
            'employees': {
                'table_name': 'employees',
                'record_count': 500,
                'columns': {
                    'employee_id': {
                        'column_name': 'employee_id',
                        'data_type': 'INTEGER',
                        'null_count': 0,
                        'non_null_count': 500,
                        'distinct_count': 500,
                        'min_value': 1,
                        'max_value': 500,
                        'top_values': [
                            {'value': '1', 'count': 1},
                            {'value': '2', 'count': 1}
                        ]
                    },
                    'name': {
                        'column_name': 'name',
                        'data_type': 'VARCHAR',
                        'null_count': 0,
                        'non_null_count': 500,
                        'distinct_count': 495,
                        'top_values': [
                            {'value': 'John Smith', 'count': 2},
                            {'value': 'Jane Doe', 'count': 2},
                            {'value': 'Bob Johnson', 'count': 1}
                        ]
                    },
                    'department': {
                        'column_name': 'department',
                        'data_type': 'VARCHAR',
                        'null_count': 5,
                        'non_null_count': 495,
                        'distinct_count': 8,
                        'top_values': [
                            {'value': 'Engineering', 'count': 150},
                            {'value': 'Sales', 'count': 100},
                            {'value': 'Marketing', 'count': 80},
                            {'value': 'HR', 'count': 50}
                        ]
                    },
                    'salary': {
                        'column_name': 'salary',
                        'data_type': 'NUMERIC',
                        'null_count': 10,
                        'non_null_count': 490,
                        'distinct_count': 200,
                        'min_value': 45000,
                        'max_value': 180000,
                        'avg_value': 87500,
                        'top_values': [
                            {'value': '75000', 'count': 25},
                            {'value': '80000', 'count': 20}
                        ]
                    }
                }
            },
            'departments': {
                'table_name': 'departments',
                'record_count': 8,
                'columns': {
                    'dept_id': {
                        'column_name': 'dept_id',
                        'data_type': 'INTEGER',
                        'null_count': 0,
                        'non_null_count': 8,
                        'distinct_count': 8,
                        'min_value': 1,
                        'max_value': 8,
                        'top_values': [
                            {'value': '1', 'count': 1},
                            {'value': '2', 'count': 1}
                        ]
                    },
                    'dept_name': {
                        'column_name': 'dept_name',
                        'data_type': 'VARCHAR',
                        'null_count': 0,
                        'non_null_count': 8,
                        'distinct_count': 8,
                        'top_values': [
                            {'value': 'Engineering', 'count': 1},
                            {'value': 'Sales', 'count': 1},
                            {'value': 'Marketing', 'count': 1}
                        ]
                    },
                    'manager': {
                        'column_name': 'manager',
                        'data_type': 'VARCHAR',
                        'null_count': 1,
                        'non_null_count': 7,
                        'distinct_count': 7,
                        'top_values': [
                            {'value': 'Alice Brown', 'count': 1},
                            {'value': 'Mike Wilson', 'count': 1}
                        ]
                    }
                }
            }
        }
    }

    # Sample summaries
    summaries = {
        'employees': {
            'table_name': 'employees',
            'column_summaries': {
                'employee_id': {
                    'short_description': 'Unique identifier for each employee',
                    'long_description': 'Unique identifier for each employee. Integer values from 1 to 500, no duplicates, used as primary key.',
                    'profile': {}
                },
                'name': {
                    'short_description': 'Full name of the employee',
                    'long_description': 'Full name of the employee. Text values like "John Smith", "Jane Doe". Most names are unique with few duplicates.',
                    'profile': {}
                },
                'department': {
                    'short_description': 'Department where employee works',
                    'long_description': 'Department where employee works. Common values are "Engineering" (150 employees), "Sales" (100), "Marketing" (80), "HR" (50).',
                    'profile': {}
                },
                'salary': {
                    'short_description': 'Annual salary in dollars',
                    'long_description': 'Annual salary in dollars. Numeric values ranging from $45,000 to $180,000, average $87,500. Common values around $75,000-$80,000.',
                    'profile': {}
                }
            }
        },
        'departments': {
            'table_name': 'departments',
            'column_summaries': {
                'dept_id': {
                    'short_description': 'Unique identifier for each department',
                    'long_description': 'Unique identifier for each department. Integer values 1-8, used as primary key.',
                    'profile': {}
                },
                'dept_name': {
                    'short_description': 'Name of the department',
                    'long_description': 'Name of the department. Values include "Engineering", "Sales", "Marketing", "HR", etc.',
                    'profile': {}
                },
                'manager': {
                    'short_description': 'Manager of the department',
                    'long_description': 'Manager of the department. Names like "Alice Brown", "Mike Wilson". One department has no manager assigned.',
                    'profile': {}
                }
            }
        }
    }

    return profile, summaries

def demo_schema_linking():
    """Demo schema linking functionality"""
    logger.info("=== Schema Linking Demo ===")

    # Create clients
    config = Config()
    llm_client = AzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version=config.AZURE_OPENAI_API_VERSION
    )

    # Create schema linker
    schema_linker = SchemaLinker(llm_client)

    # Get sample data
    profile, summaries = create_sample_data()

    # Build indexes
    logger.info("Building LSH and FAISS indexes...")
    schema_linker.build_lsh_index(profile)
    schema_linker.build_faiss_index(summaries)

    # Test questions
    questions = [
        "How many employees are in the Engineering department?",
        "What is the average salary of employees?",
        "Who are the department managers?",
        "List all employees with salary over 100000",
        "Which department has the most employees?"
    ]

    for question in questions:
        logger.info(f"\nQuestion: {question}")

        # Extract literals
        literals = schema_linker.extract_literals(question)
        logger.info(f"Extracted literals: {literals}")

        # Get focused schema
        focused_schema = schema_linker.get_focused_schema(question)
        logger.info(f"Focused schema: {focused_schema}")

        # Generate context
        context = schema_linker.generate_schema_context('focused', 'maximal', focused_schema)
        logger.info(f"Schema context (first 200 chars): {context[:200]}...")

def demo_sql_generation():
    """Demo SQL generation functionality"""
    logger.info("\n\n=== SQL Generation Demo ===")

    # Create clients
    config = Config()
    llm_client = AzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version=config.AZURE_OPENAI_API_VERSION
    )

    # Create components
    schema_linker = SchemaLinker(llm_client)
    sql_generator = SQLGenerator(llm_client, schema_linker)

    # Get sample data
    profile, summaries = create_sample_data()

    # Build indexes
    schema_linker.build_lsh_index(profile)
    schema_linker.build_faiss_index(summaries)

    # Sample few-shot examples
    examples = [
        {
            'question': 'How many users are there?',
            'sql': 'SELECT COUNT(*) FROM users;'
        },
        {
            'question': 'What is the average age of customers?',
            'sql': 'SELECT AVG(age) FROM customers;'
        },
        {
            'question': 'List all orders from last month',
            'sql': 'SELECT * FROM orders WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH);'
        }
    ]

    sql_generator.build_few_shot_index(examples)

    # Test SQL generation
    test_question = "How many employees are in the Engineering department?"

    logger.info(f"Generating SQL for: {test_question}")

    # Get focused schema
    focused_schema = schema_linker.get_focused_schema(test_question)
    logger.info(f"Focused schema: {focused_schema}")

    # Generate schema context
    context = schema_linker.generate_schema_context('focused', 'maximal', focused_schema)

    # Select few-shot examples
    selected_examples = sql_generator.select_few_shot_examples(test_question, k=2)
    logger.info(f"Selected examples: {[ex['question'] for ex in selected_examples]}")

    # Generate SQL candidates
    candidates = sql_generator.generate_sql_candidates(test_question, context, selected_examples)
    logger.info(f"Generated {len(candidates)} SQL candidates:")
    for i, sql in enumerate(candidates, 1):
        logger.info(f"  Candidate {i}: {sql}")

    # Select best SQL
    best_sql = sql_generator.majority_voting(candidates, test_question)
    logger.info(f"Best SQL: {best_sql}")

    # Validate
    is_valid, msg = sql_generator.validate_sql(best_sql)
    logger.info(f"SQL validation: {is_valid} - {msg}")

def main():
    """Run the complete demo"""
    logger.info("Starting Text-to-SQL System Demo")

    try:
        # Run schema linking demo
        demo_schema_linking()

        # Run SQL generation demo
        demo_sql_generation()

        logger.info("\n🎉 Demo completed successfully!")

    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise

if __name__ == "__main__":
    main()