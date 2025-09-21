"""
SQLite Execution-Based Evaluation Module
Evaluates Text-to-SQL performance using actual query execution
"""
import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple
import sys
from openai import AzureOpenAI
import random

# Add src to path
sys.path.append(str(Path(__file__).parent))

from configs.config import Config
from modules.bird_loader import BIRDLoader
from modules.schema_linker import SchemaLinker
from modules.sql_generator import SQLGenerator
from modules.sqlite_executor import SQLiteExecutor, BIRDSQLiteProfiler

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLiteEvaluator:
    """Evaluate Text-to-SQL with actual SQLite execution"""

    def __init__(self):
        self.config = Config()
        self.llm_client = AzureOpenAI(
            azure_endpoint=self.config.AZURE_OPENAI_ENDPOINT,
            api_key=self.config.AZURE_OPENAI_KEY,
            api_version=self.config.AZURE_OPENAI_API_VERSION
        )

        # Initialize modules
        self.bird_loader = BIRDLoader()
        self.schema_linker = SchemaLinker(self.llm_client)
        self.sql_generator = SQLGenerator(self.llm_client, self.schema_linker)
        self.sqlite_executor = SQLiteExecutor(self.config.BIRD_DATASET_PATH)
        self.profiler = BIRDSQLiteProfiler(self.sqlite_executor)

        # Output directory
        self.output_dir = Path(self.config.DATA_DIR) / "sqlite_evaluation_results"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def evaluate_single_question(self, question_data: Dict) -> Dict:
        """Evaluate a single question with execution-based comparison"""
        db_id = question_data['db_id']
        question = question_data['question']
        ground_truth_sql = question_data.get('SQL', '')
        evidence = question_data.get('evidence', '')

        logger.info(f"Evaluating question for database: {db_id}")

        try:
            # Step 1: Get real database profile
            profile = self.profiler.create_database_profile(db_id)
            summaries = self.profiler.create_table_summaries(profile)

            # Step 2: Build indexes with real data
            self.schema_linker.build_lsh_index(profile)
            self.schema_linker.build_faiss_index(summaries)

            # Step 3: Generate SQL prediction
            prediction_result = self.sql_generator.generate_sql(
                question, profile, summaries, evidence=evidence
            )
            predicted_sql = prediction_result.get('final_sql', '')

            # Step 4: Execute both SQLs
            gt_success, gt_result = self.sqlite_executor.execute_sql(db_id, ground_truth_sql)
            pred_success, pred_result = self.sqlite_executor.execute_sql(db_id, predicted_sql)

            # Step 5: Compare results
            if gt_success and pred_success:
                exact_match, similarity, message = self.sqlite_executor.compare_results(
                    gt_result, pred_result, ordered=False
                )
            else:
                exact_match = False
                similarity = 0.0
                message = f"Execution failed - GT: {gt_success}, Pred: {pred_success}"

            # Calculate performance score
            performance_score = self._calculate_performance_score(
                pred_success, exact_match, similarity, prediction_result.get('is_valid', False)
            )

            evaluation = {
                'db_id': db_id,
                'question': question,
                'evidence': evidence,
                'ground_truth_sql': ground_truth_sql,
                'predicted_sql': predicted_sql,
                'gt_execution_success': gt_success,
                'pred_execution_success': pred_success,
                'exact_match': exact_match,
                'similarity_score': similarity,
                'comparison_message': message,
                'sql_valid': prediction_result.get('is_valid', False),
                'performance_score': performance_score,
                'focused_schema': prediction_result.get('focused_schema', {}),
                'num_candidates': len(prediction_result.get('sql_candidates', []))
            }

            # Add execution results if successful
            if gt_success:
                evaluation['gt_row_count'] = len(gt_result.get('rows', []))
            if pred_success:
                evaluation['pred_row_count'] = len(pred_result.get('rows', []))

            return evaluation

        except Exception as e:
            logger.error(f"Error evaluating {db_id}: {e}")
            return {
                'db_id': db_id,
                'question': question,
                'error': str(e),
                'performance_score': 0.0,
                'exact_match': False,
                'similarity_score': 0.0
            }

    def _calculate_performance_score(self, execution_success: bool,
                                    exact_match: bool, similarity: float,
                                    sql_valid: bool) -> float:
        """Calculate overall performance score with execution-based metrics"""
        score = 0.0

        # SQL validity (20%)
        if sql_valid:
            score += 0.2

        # Execution success (30%)
        if execution_success:
            score += 0.3

        # Result accuracy (50%)
        if exact_match:
            score += 0.5
        else:
            # Partial credit based on similarity
            score += 0.5 * similarity

        return score

    def evaluate_database_batch(self, num_databases: int = 10,
                               questions_per_db: int = 5) -> Dict:
        """Evaluate multiple databases with execution-based comparison"""
        logger.info(f"Starting SQLite execution-based evaluation")

        # Load BIRD data
        bird_data = self.bird_loader.load_questions()

        # Group by database
        questions_by_db = {}
        for item in bird_data:
            db_id = item['db_id']
            if db_id not in questions_by_db:
                questions_by_db[db_id] = []
            questions_by_db[db_id].append(item)

        # Filter to databases that exist
        valid_dbs = []
        for db_id in questions_by_db.keys():
            db_path = self.sqlite_executor.get_database_path(db_id)
            if db_path.exists():
                valid_dbs.append(db_id)

        # Sample databases
        selected_dbs = random.sample(valid_dbs, min(num_databases, len(valid_dbs)))

        all_evaluations = []
        db_performances = {}

        for i, db_id in enumerate(selected_dbs, 1):
            logger.info(f"Evaluating database {i}/{len(selected_dbs)}: {db_id}")

            # Sample questions for this database
            db_questions = questions_by_db[db_id]
            selected_questions = random.sample(
                db_questions,
                min(questions_per_db, len(db_questions))
            )

            db_evaluations = []
            for question_data in selected_questions:
                evaluation = self.evaluate_single_question(question_data)
                db_evaluations.append(evaluation)
                all_evaluations.append(evaluation)

            # Calculate database-specific metrics
            db_performances[db_id] = self._calculate_db_metrics(db_evaluations)

        # Calculate overall metrics
        overall_metrics = self._calculate_overall_metrics(all_evaluations)

        # Save results
        self._save_results(all_evaluations, db_performances, overall_metrics)

        return {
            'overall_metrics': overall_metrics,
            'db_performances': db_performances,
            'total_questions': len(all_evaluations)
        }

    def _calculate_db_metrics(self, evaluations: List[Dict]) -> Dict:
        """Calculate metrics for a single database"""
        if not evaluations:
            return {}

        valid_evals = [e for e in evaluations if 'error' not in e]

        return {
            'total_questions': len(evaluations),
            'successful_evaluations': len(valid_evals),
            'avg_performance_score': sum(e['performance_score'] for e in valid_evals) / len(valid_evals) if valid_evals else 0,
            'execution_success_rate': sum(1 for e in valid_evals if e.get('pred_execution_success', False)) / len(valid_evals) if valid_evals else 0,
            'exact_match_rate': sum(1 for e in valid_evals if e.get('exact_match', False)) / len(valid_evals) if valid_evals else 0,
            'avg_similarity': sum(e.get('similarity_score', 0) for e in valid_evals) / len(valid_evals) if valid_evals else 0
        }

    def _calculate_overall_metrics(self, evaluations: List[Dict]) -> Dict:
        """Calculate overall metrics across all evaluations"""
        valid_evals = [e for e in evaluations if 'error' not in e]

        if not valid_evals:
            return {}

        return {
            'total_questions': len(evaluations),
            'successful_evaluations': len(valid_evals),
            'average_performance_score': sum(e['performance_score'] for e in valid_evals) / len(valid_evals),
            'sql_validity_rate': sum(1 for e in valid_evals if e.get('sql_valid', False)) / len(valid_evals),
            'execution_success_rate': sum(1 for e in valid_evals if e.get('pred_execution_success', False)) / len(valid_evals),
            'exact_match_rate': sum(1 for e in valid_evals if e.get('exact_match', False)) / len(valid_evals),
            'average_similarity_score': sum(e.get('similarity_score', 0) for e in valid_evals) / len(valid_evals),
            'databases_evaluated': len(set(e['db_id'] for e in valid_evals if 'db_id' in e))
        }

    def _save_results(self, evaluations: List[Dict], db_performances: Dict,
                     overall_metrics: Dict):
        """Save evaluation results"""
        # Save detailed results
        results_path = self.output_dir / 'sqlite_evaluation_results.json'
        with open(results_path, 'w') as f:
            json.dump({
                'overall_metrics': overall_metrics,
                'database_performances': db_performances,
                'evaluations': evaluations
            }, f, indent=2)

        # Save summary
        summary_path = self.output_dir / 'sqlite_evaluation_summary.txt'
        with open(summary_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("SQLITE EXECUTION-BASED EVALUATION SUMMARY\n")
            f.write("=" * 80 + "\n\n")

            f.write("OVERALL METRICS:\n")
            for key, value in overall_metrics.items():
                if isinstance(value, float):
                    f.write(f"  {key}: {value:.3f}\n")
                else:
                    f.write(f"  {key}: {value}\n")

            f.write("\nDATABASE-SPECIFIC PERFORMANCE:\n")
            for db_id, metrics in db_performances.items():
                score = metrics.get('avg_performance_score', 0)
                exact_match = metrics.get('exact_match_rate', 0)
                f.write(f"  {db_id}:\n")
                f.write(f"    Performance Score: {score:.3f}\n")
                f.write(f"    Exact Match Rate: {exact_match:.3f}\n")
                f.write(f"    Execution Success: {metrics.get('execution_success_rate', 0):.3f}\n")

        logger.info(f"Results saved to {self.output_dir}")


def main():
    """Main execution function"""
    evaluator = SQLiteEvaluator()

    logger.info("Starting SQLite execution-based evaluation")

    # Run evaluation on 5 databases with 2 questions each for faster testing
    results = evaluator.evaluate_database_batch(
        num_databases=5,
        questions_per_db=2
    )

    # Print summary
    print("\n" + "=" * 80)
    print("EVALUATION COMPLETE")
    print("=" * 80)
    print(f"Average Performance Score: {results['overall_metrics'].get('average_performance_score', 0):.3f}")
    print(f"Exact Match Rate: {results['overall_metrics'].get('exact_match_rate', 0):.3f}")
    print(f"Execution Success Rate: {results['overall_metrics'].get('execution_success_rate', 0):.3f}")
    print(f"Average Similarity Score: {results['overall_metrics'].get('average_similarity_score', 0):.3f}")

    # Close database connections
    evaluator.sqlite_executor.close_all_connections()


if __name__ == "__main__":
    main()