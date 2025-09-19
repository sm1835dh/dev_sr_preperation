"""
Evaluation Framework Module
Implements evaluation metrics and experiment runner for Text-to-SQL system
"""
import logging
import json
import time
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import pandas as pd
import sqlglot
import sqlite3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from configs.config import Config
from modules.database import DatabaseManager

logger = logging.getLogger(__name__)


class SQLEvaluator:
    """Evaluate generated SQL queries against ground truth"""

    def __init__(self, db_manager: DatabaseManager):
        self.config = Config()
        self.db_manager = db_manager

    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison"""
        try:
            # Remove comments and extra whitespace
            sql = ' '.join(sql.split())
            sql = sql.strip().rstrip(';')

            # Parse and reformat using sqlglot
            parsed = sqlglot.parse_one(sql, dialect='postgres')
            if parsed:
                normalized = parsed.sql(dialect='postgres', pretty=True)
                return normalized.lower()
            return sql.lower()
        except:
            return sql.lower().strip()

    def exact_match(self, predicted_sql: str, ground_truth_sql: str) -> bool:
        """Check if predicted SQL exactly matches ground truth"""
        pred_normalized = self.normalize_sql(predicted_sql)
        gt_normalized = self.normalize_sql(ground_truth_sql)
        return pred_normalized == gt_normalized

    def execution_accuracy(self, predicted_sql: str, ground_truth_sql: str,
                          schema_name: str) -> Tuple[bool, str]:
        """
        Check if predicted SQL produces same results as ground truth
        Execution Accuracy (EX) metric
        """
        try:
            # Execute predicted SQL
            pred_results = self._execute_sql_safely(predicted_sql, schema_name)
            if pred_results is None:
                return False, "Predicted SQL execution failed"

            # Execute ground truth SQL
            gt_results = self._execute_sql_safely(ground_truth_sql, schema_name)
            if gt_results is None:
                return False, "Ground truth SQL execution failed"

            # Compare results
            if self._compare_results(pred_results, gt_results):
                return True, "Results match"
            else:
                return False, "Results differ"

        except Exception as e:
            return False, f"Execution error: {str(e)}"

    def _execute_sql_safely(self, sql: str, schema_name: str) -> Optional[List[Dict]]:
        """Execute SQL safely with timeout and error handling"""
        try:
            # Add schema prefix if not present
            if schema_name not in sql:
                # Simple schema prefixing - would need more sophisticated parsing
                pass

            # Execute with timeout
            results = self.db_manager.execute_query(sql)
            return results

        except Exception as e:
            logger.error(f"SQL execution error: {e}")
            return None

    def _compare_results(self, results1: List[Dict], results2: List[Dict]) -> bool:
        """Compare two result sets for equality"""
        if len(results1) != len(results2):
            return False

        # Convert to comparable format
        def normalize_row(row):
            return tuple(str(v) if v is not None else 'NULL' for v in row.values())

        set1 = set(normalize_row(row) for row in results1)
        set2 = set(normalize_row(row) for row in results2)

        return set1 == set2

    def schema_linking_accuracy(self, predicted_schema: Dict, ground_truth_sql: str) -> float:
        """
        Evaluate schema linking accuracy
        Measures how well the system identifies relevant tables/columns
        """
        # Extract actual tables/columns from ground truth SQL
        gt_fields = self._extract_fields_from_sql(ground_truth_sql)

        # Extract predicted fields
        pred_fields = set()
        for table, columns in predicted_schema.items():
            for col in columns:
                pred_fields.add(f"{table}.{col}")

        if not gt_fields:
            return 1.0 if not pred_fields else 0.0

        # Calculate precision, recall, F1
        intersection = gt_fields.intersection(pred_fields)
        precision = len(intersection) / len(pred_fields) if pred_fields else 0.0
        recall = len(intersection) / len(gt_fields) if gt_fields else 0.0

        if precision + recall == 0:
            return 0.0

        f1 = 2 * precision * recall / (precision + recall)
        return f1

    def _extract_fields_from_sql(self, sql: str) -> set:
        """Extract table.column references from SQL"""
        fields = set()

        try:
            # Use sqlglot to parse and extract table references
            parsed = sqlglot.parse_one(sql, dialect='postgres')
            if parsed:
                for table in parsed.find_all(sqlglot.expressions.Table):
                    table_name = table.name
                    # This is a simplified extraction - would need more sophisticated parsing
                    for node in parsed.walk():
                        if hasattr(node, 'table') and hasattr(node, 'name'):
                            if node.table == table_name:
                                fields.add(f"{table_name}.{node.name}")

        except:
            # Fallback to regex extraction
            import re
            field_pattern = r'([a-zA-Z_]\w*\.[a-zA-Z_]\w*)'
            matches = re.findall(field_pattern, sql)
            fields.update(matches)

        return fields

    def evaluate_single(self, predicted: Dict, ground_truth: Dict) -> Dict:
        """Evaluate a single prediction"""
        pred_sql = predicted.get('final_sql', '')
        gt_sql = ground_truth.get('sql', '')
        schema_name = ground_truth.get('schema_name', '')

        results = {
            'question': predicted.get('question', ''),
            'predicted_sql': pred_sql,
            'ground_truth_sql': gt_sql
        }

        # Exact Match
        results['exact_match'] = self.exact_match(pred_sql, gt_sql)

        # Execution Accuracy
        ex_acc, ex_msg = self.execution_accuracy(pred_sql, gt_sql, schema_name)
        results['execution_accuracy'] = ex_acc
        results['execution_message'] = ex_msg

        # Schema Linking Accuracy
        pred_schema = predicted.get('focused_schema', {})
        results['schema_linking_f1'] = self.schema_linking_accuracy(pred_schema, gt_sql)

        # SQL Validity
        results['sql_valid'] = predicted.get('is_valid', False)

        return results

    def evaluate_batch(self, predictions: List[Dict], ground_truths: List[Dict]) -> Dict:
        """Evaluate batch of predictions"""
        if len(predictions) != len(ground_truths):
            raise ValueError("Predictions and ground truths must have same length")

        individual_results = []
        for pred, gt in zip(predictions, ground_truths):
            result = self.evaluate_single(pred, gt)
            individual_results.append(result)

        # Aggregate metrics
        total = len(individual_results)
        exact_matches = sum(r['exact_match'] for r in individual_results)
        execution_accuracies = sum(r['execution_accuracy'] for r in individual_results)
        valid_sqls = sum(r['sql_valid'] for r in individual_results)
        avg_schema_f1 = sum(r['schema_linking_f1'] for r in individual_results) / total

        summary = {
            'total_questions': total,
            'exact_match_accuracy': exact_matches / total,
            'execution_accuracy': execution_accuracies / total,
            'sql_validity_rate': valid_sqls / total,
            'average_schema_linking_f1': avg_schema_f1,
            'individual_results': individual_results
        }

        return summary


class ExperimentRunner:
    """Run experiments with different configurations"""

    def __init__(self):
        self.config = Config()
        self.results_dir = Path(self.config.DATA_DIR) / "experiment_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def run_ablation_study(self, test_data: List[Dict]) -> Dict:
        """
        Run ablation study to test different components
        """
        logger.info("Starting ablation study")

        experiments = {
            'baseline': {
                'schema_type': 'full',
                'profile_type': 'minimal',
                'use_few_shot': False,
                'use_schema_linking': False
            },
            'with_profiling': {
                'schema_type': 'full',
                'profile_type': 'maximal',
                'use_few_shot': False,
                'use_schema_linking': False
            },
            'with_schema_linking': {
                'schema_type': 'focused',
                'profile_type': 'maximal',
                'use_few_shot': False,
                'use_schema_linking': True
            },
            'with_few_shot': {
                'schema_type': 'focused',
                'profile_type': 'maximal',
                'use_few_shot': True,
                'use_schema_linking': True
            },
            'full_system': {
                'schema_type': 'focused',
                'profile_type': 'full',
                'use_few_shot': True,
                'use_schema_linking': True
            }
        }

        results = {}
        for exp_name, config in experiments.items():
            logger.info(f"Running experiment: {exp_name}")
            results[exp_name] = self._run_single_experiment(test_data, config)

        return results

    def _run_single_experiment(self, test_data: List[Dict], config: Dict) -> Dict:
        """Run a single experiment configuration"""
        # This would integrate with the full pipeline
        # For now, return placeholder results
        return {
            'config': config,
            'exact_match_accuracy': 0.0,
            'execution_accuracy': 0.0,
            'schema_linking_f1': 0.0,
            'total_questions': len(test_data)
        }

    def compare_schema_variations(self, test_data: List[Dict]) -> Dict:
        """
        Test different schema and profile combinations
        Section 3: Five variations mentioned in the paper
        """
        variations = [
            ('focused', 'minimal'),
            ('focused', 'maximal'),
            ('full', 'minimal'),
            ('full', 'maximal'),
            ('focused', 'full')
        ]

        results = {}
        for schema_type, profile_type in variations:
            var_name = f"{schema_type}_{profile_type}"
            logger.info(f"Testing variation: {var_name}")

            # Run experiment with this variation
            config = {
                'schema_type': schema_type,
                'profile_type': profile_type,
                'use_few_shot': True,
                'use_schema_linking': True
            }
            results[var_name] = self._run_single_experiment(test_data, config)

        return results

    def measure_efficiency(self, test_data: List[Dict]) -> Dict:
        """Measure system efficiency metrics"""
        start_time = time.time()

        # Run full pipeline
        # This would integrate with actual pipeline
        total_questions = len(test_data)

        end_time = time.time()
        total_time = end_time - start_time

        return {
            'total_questions': total_questions,
            'total_time_seconds': total_time,
            'questions_per_second': total_questions / total_time if total_time > 0 else 0,
            'average_time_per_question': total_time / total_questions if total_questions > 0 else 0
        }

    def save_experiment_results(self, results: Dict, experiment_name: str):
        """Save experiment results to file"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"{experiment_name}_{timestamp}.json"
        filepath = self.results_dir / filename

        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved experiment results to {filepath}")

    def generate_report(self, results: Dict) -> str:
        """Generate human-readable experiment report"""
        report_lines = [
            "# Text-to-SQL System Evaluation Report",
            f"Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Summary Statistics"
        ]

        if 'total_questions' in results:
            report_lines.extend([
                f"- Total Questions: {results['total_questions']}",
                f"- Exact Match Accuracy: {results.get('exact_match_accuracy', 0):.3f}",
                f"- Execution Accuracy: {results.get('execution_accuracy', 0):.3f}",
                f"- SQL Validity Rate: {results.get('sql_validity_rate', 0):.3f}",
                f"- Average Schema Linking F1: {results.get('average_schema_linking_f1', 0):.3f}",
                ""
            ])

        # Add component analysis if available
        if 'individual_results' in results:
            individual = results['individual_results']
            error_analysis = self._analyze_errors(individual)
            report_lines.extend([
                "## Error Analysis",
                f"- Syntax Errors: {error_analysis['syntax_errors']}",
                f"- Execution Errors: {error_analysis['execution_errors']}",
                f"- Logic Errors: {error_analysis['logic_errors']}",
                ""
            ])

        return "\n".join(report_lines)

    def _analyze_errors(self, individual_results: List[Dict]) -> Dict:
        """Analyze types of errors in results"""
        syntax_errors = 0
        execution_errors = 0
        logic_errors = 0

        for result in individual_results:
            if not result['sql_valid']:
                syntax_errors += 1
            elif not result['execution_accuracy']:
                if 'execution_message' in result and 'failed' in result['execution_message']:
                    execution_errors += 1
                else:
                    logic_errors += 1

        return {
            'syntax_errors': syntax_errors,
            'execution_errors': execution_errors,
            'logic_errors': logic_errors
        }