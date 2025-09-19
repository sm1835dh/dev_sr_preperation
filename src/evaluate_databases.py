"""
Database-specific Evaluation Module
Evaluates each database's Text-to-SQL performance and analyzes issues
"""
import logging
import json
from pathlib import Path
from typing import Dict, List
import sys
from openai import AzureOpenAI
import re
from collections import Counter

# Add src to path
sys.path.append(str(Path(__file__).parent))

from configs.config import Config
from modules.bird_loader import BIRDLoader
from modules.schema_linker import SchemaLinker
from modules.sql_generator import SQLGenerator
from modules.evaluator import SQLEvaluator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseEvaluator:
    """Evaluate each database's performance and identify issues"""

    def __init__(self):
        self.config = Config()
        self.llm_client = AzureOpenAI(
            azure_endpoint=self.config.AZURE_OPENAI_ENDPOINT,
            api_key=self.config.AZURE_OPENAI_KEY,
            api_version=self.config.AZURE_OPENAI_API_VERSION
        )

        self.bird_loader = BIRDLoader()
        self.schema_linker = SchemaLinker(self.llm_client)
        self.sql_generator = SQLGenerator(self.llm_client, self.schema_linker)
        self.evaluator = SQLEvaluator(None)  # No DB connection needed for text comparison

        # Output directory
        self.output_dir = Path(self.config.DATA_DIR) / "evaluation_results"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def evaluate_single_database(self, db_name: str, question_data: Dict) -> Dict:
        """Evaluate a single database's Text-to-SQL performance"""
        logger.info(f"Evaluating database: {db_name}")

        try:
            # Extract basic information
            question = question_data['question']
            ground_truth_sql = question_data.get('SQL', '')
            evidence = question_data.get('evidence', '')

            # Create mock profile for schema linking (simplified)
            mock_profile = self._create_simple_profile(db_name, ground_truth_sql)
            mock_summaries = self._create_simple_summaries(mock_profile)

            # Build indexes
            self.schema_linker.build_lsh_index(mock_profile)
            self.schema_linker.build_faiss_index(mock_summaries['table_summaries'])

            # Generate SQL using pipeline
            examples = [
                {'question': 'How many records are there?', 'sql': 'SELECT COUNT(*) FROM table;'},
                {'question': 'What is the average value?', 'sql': 'SELECT AVG(column) FROM table;'}
            ]
            self.sql_generator.build_few_shot_index(examples)

            # Generate prediction
            prediction_result = self.sql_generator.generate_sql(question, mock_profile, mock_summaries)
            predicted_sql = prediction_result.get('final_sql', '')

            # Evaluate prediction
            evaluation = {
                'database_name': db_name,
                'question': question,
                'ground_truth_sql': ground_truth_sql,
                'predicted_sql': predicted_sql,
                'evidence': evidence
            }

            # SQL syntax validation
            is_valid, validation_msg = self.sql_generator.validate_sql(predicted_sql)
            evaluation['sql_valid'] = is_valid
            evaluation['validation_message'] = validation_msg

            # Text-based comparison (since we don't have actual database execution)
            evaluation['exact_match'] = self.evaluator.exact_match(predicted_sql, ground_truth_sql)

            # Analyze SQL complexity and features
            evaluation['complexity_analysis'] = self._analyze_sql_complexity(ground_truth_sql, predicted_sql)

            # Identify potential issues
            evaluation['issues'] = self._identify_issues(evaluation)

            # Performance score
            evaluation['performance_score'] = self._calculate_performance_score(evaluation)

            return evaluation

        except Exception as e:
            logger.error(f"Error evaluating {db_name}: {e}")
            return {
                'database_name': db_name,
                'question': question_data.get('question', ''),
                'error': str(e),
                'performance_score': 0.0,
                'issues': ['evaluation_failed']
            }

    def _create_simple_profile(self, db_name: str, sql: str) -> Dict:
        """Create simplified profile for evaluation"""
        # Extract tables from SQL
        table_pattern = r'FROM\s+(\w+)|JOIN\s+(\w+)'
        tables = []
        for match in re.finditer(table_pattern, sql, re.IGNORECASE):
            table = match.group(1) or match.group(2)
            if table and table not in tables:
                tables.append(table)

        if not tables:
            tables = ['main_table']

        # Create mock profile
        profile = {'schema_name': f"bird_{db_name}", 'tables': {}}

        for table in tables:
            profile['tables'][table] = {
                'table_name': table,
                'record_count': 1000,
                'columns': {
                    f"{table}_id": {
                        'column_name': f"{table}_id",
                        'data_type': 'INTEGER',
                        'null_count': 0,
                        'non_null_count': 1000,
                        'distinct_count': 1000,
                        'top_values': [{'value': '1', 'count': 1}]
                    }
                }
            }

        return profile

    def _create_simple_summaries(self, profile: Dict) -> Dict:
        """Create simplified summaries for evaluation"""
        summaries = {'table_summaries': {}}

        for table_name, table_data in profile.get('tables', {}).items():
            summaries['table_summaries'][table_name] = {
                'table_name': table_name,
                'column_summaries': {}
            }

            for col_name in table_data.get('columns', {}):
                summaries['table_summaries'][table_name]['column_summaries'][col_name] = {
                    'short_description': f"Column {col_name} in table {table_name}",
                    'long_description': f"Detailed description of {col_name}",
                    'profile': {}
                }

        return summaries

    def _analyze_sql_complexity(self, ground_truth: str, predicted: str) -> Dict:
        """Analyze SQL complexity and features"""
        def analyze_sql(sql):
            return {
                'word_count': len(sql.split()),
                'has_joins': 'JOIN' in sql.upper(),
                'has_subquery': '(' in sql and 'SELECT' in sql[sql.find('('):],
                'has_aggregation': any(func in sql.upper() for func in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']),
                'has_groupby': 'GROUP BY' in sql.upper(),
                'has_orderby': 'ORDER BY' in sql.upper(),
                'has_having': 'HAVING' in sql.upper(),
                'has_distinct': 'DISTINCT' in sql.upper(),
                'has_union': 'UNION' in sql.upper(),
                'complexity_level': 'high' if 'JOIN' in sql.upper() and 'GROUP BY' in sql.upper() else
                                 'medium' if 'JOIN' in sql.upper() or 'GROUP BY' in sql.upper() else 'low'
            }

        gt_analysis = analyze_sql(ground_truth)
        pred_analysis = analyze_sql(predicted)

        return {
            'ground_truth': gt_analysis,
            'predicted': pred_analysis,
            'feature_match': {
                'joins': gt_analysis['has_joins'] == pred_analysis['has_joins'],
                'aggregation': gt_analysis['has_aggregation'] == pred_analysis['has_aggregation'],
                'groupby': gt_analysis['has_groupby'] == pred_analysis['has_groupby'],
                'orderby': gt_analysis['has_orderby'] == pred_analysis['has_orderby']
            }
        }

    def _identify_issues(self, evaluation: Dict) -> List[str]:
        """Identify potential issues with the prediction"""
        issues = []

        # SQL validity issues
        if not evaluation.get('sql_valid', False):
            issues.append('invalid_sql_syntax')

        # Empty prediction
        if not evaluation.get('predicted_sql', '').strip():
            issues.append('empty_prediction')

        # Exact match failure
        if not evaluation.get('exact_match', False):
            issues.append('no_exact_match')

        # Complexity mismatch
        complexity = evaluation.get('complexity_analysis', {})
        feature_match = complexity.get('feature_match', {})

        if not feature_match.get('joins', True):
            issues.append('join_mismatch')
        if not feature_match.get('aggregation', True):
            issues.append('aggregation_mismatch')
        if not feature_match.get('groupby', True):
            issues.append('groupby_mismatch')

        # Length mismatch (too simple or too complex)
        gt_analysis = complexity.get('ground_truth', {})
        pred_analysis = complexity.get('predicted', {})

        gt_words = gt_analysis.get('word_count', 0)
        pred_words = pred_analysis.get('word_count', 0)

        if pred_words < gt_words * 0.5:
            issues.append('prediction_too_simple')
        elif pred_words > gt_words * 2:
            issues.append('prediction_too_complex')

        return issues

    def _calculate_performance_score(self, evaluation: Dict) -> float:
        """Calculate overall performance score"""
        score = 0.0

        # SQL validity (30%)
        if evaluation.get('sql_valid', False):
            score += 0.3

        # Exact match (40%)
        if evaluation.get('exact_match', False):
            score += 0.4

        # Feature matching (30%)
        complexity = evaluation.get('complexity_analysis', {})
        feature_match = complexity.get('feature_match', {})

        feature_score = sum(feature_match.values()) / len(feature_match) if feature_match else 0
        score += 0.3 * feature_score

        return round(score, 3)

    def run_comprehensive_evaluation(self) -> Dict:
        """Run comprehensive evaluation on all databases"""
        logger.info("Starting comprehensive database evaluation")

        # Load sample databases
        samples = self.bird_loader.sample_databases(self.config.SAMPLE_SIZE)

        if not samples:
            logger.error("No samples loaded")
            return {}

        logger.info(f"Evaluating {len(samples)} databases...")

        results = []
        for i, (db_name, question_data) in enumerate(samples, 1):
            logger.info(f"Evaluating {i}/{len(samples)}: {db_name}")
            result = self.evaluate_single_database(db_name, question_data)
            results.append(result)

        # Analyze overall results
        analysis = self._analyze_overall_results(results)

        # Generate individual database reports
        for result in results:
            self._generate_database_report(result)

        # Generate summary report
        self._generate_summary_report(results, analysis)

        logger.info(f"Evaluation completed! Reports saved to {self.output_dir}")

        return {
            'individual_results': results,
            'overall_analysis': analysis
        }

    def _analyze_overall_results(self, results: List[Dict]) -> Dict:
        """Analyze overall evaluation results"""
        total = len(results)
        successful = [r for r in results if 'error' not in r]

        if not successful:
            return {'error': 'No successful evaluations'}

        # Calculate aggregate metrics
        avg_score = sum(r['performance_score'] for r in successful) / len(successful)
        valid_sql_rate = sum(1 for r in successful if r.get('sql_valid', False)) / len(successful)
        exact_match_rate = sum(1 for r in successful if r.get('exact_match', False)) / len(successful)

        # Identify common issues
        all_issues = []
        for result in successful:
            all_issues.extend(result.get('issues', []))

        issue_counts = Counter(all_issues)

        # Complexity analysis
        complexity_levels = []
        for result in successful:
            complexity = result.get('complexity_analysis', {})
            gt_complexity = complexity.get('ground_truth', {}).get('complexity_level', 'unknown')
            complexity_levels.append(gt_complexity)

        complexity_distribution = Counter(complexity_levels)

        return {
            'total_databases': total,
            'successful_evaluations': len(successful),
            'average_performance_score': round(avg_score, 3),
            'sql_validity_rate': round(valid_sql_rate, 3),
            'exact_match_rate': round(exact_match_rate, 3),
            'common_issues': dict(issue_counts.most_common(10)),
            'complexity_distribution': dict(complexity_distribution),
            'issue_analysis': self._analyze_issues(issue_counts, len(successful))
        }

    def _analyze_issues(self, issue_counts: Counter, total_successful: int) -> Dict:
        """Analyze the identified issues and their implications"""
        analysis = {
            'critical_issues': [],
            'moderate_issues': [],
            'minor_issues': [],
            'recommendations': []
        }

        for issue, count in issue_counts.items():
            rate = count / total_successful

            if rate >= 0.7:  # Affects 70%+ of cases
                analysis['critical_issues'].append({
                    'issue': issue,
                    'frequency': count,
                    'rate': round(rate, 3),
                    'impact': 'high'
                })
            elif rate >= 0.3:  # Affects 30-70% of cases
                analysis['moderate_issues'].append({
                    'issue': issue,
                    'frequency': count,
                    'rate': round(rate, 3),
                    'impact': 'medium'
                })
            else:  # Affects <30% of cases
                analysis['minor_issues'].append({
                    'issue': issue,
                    'frequency': count,
                    'rate': round(rate, 3),
                    'impact': 'low'
                })

        # Generate recommendations based on issues
        if any('invalid_sql_syntax' in issue['issue'] for issue in analysis['critical_issues']):
            analysis['recommendations'].append("Improve SQL syntax validation and error handling")

        if any('no_exact_match' in issue['issue'] for issue in analysis['critical_issues']):
            analysis['recommendations'].append("Enhance semantic understanding and query generation accuracy")

        if any('join_mismatch' in issue['issue'] for issue in analysis['moderate_issues']):
            analysis['recommendations'].append("Improve join detection and relationship inference")

        if any('aggregation_mismatch' in issue['issue'] for issue in analysis['moderate_issues']):
            analysis['recommendations'].append("Better detection of aggregation requirements from natural language")

        return analysis

    def _generate_database_report(self, result: Dict):
        """Generate individual database evaluation report"""
        db_name = result['database_name']

        lines = []
        lines.append("=" * 80)
        lines.append(f"DATABASE EVALUATION REPORT: {db_name}")
        lines.append("=" * 80)
        lines.append("")

        # Basic Information
        lines.append("BASIC INFORMATION:")
        lines.append(f"  Database: {db_name}")
        lines.append(f"  Question: {result.get('question', 'N/A')}")
        lines.append("")

        # SQL Comparison
        lines.append("SQL COMPARISON:")
        lines.append(f"  Ground Truth: {result.get('ground_truth_sql', 'N/A')}")
        lines.append(f"  Predicted:    {result.get('predicted_sql', 'N/A')}")
        lines.append("")

        # Evaluation Results
        lines.append("EVALUATION RESULTS:")
        lines.append(f"  Performance Score: {result.get('performance_score', 0.0)}")
        lines.append(f"  SQL Valid: {result.get('sql_valid', False)}")
        lines.append(f"  Exact Match: {result.get('exact_match', False)}")

        if result.get('validation_message'):
            lines.append(f"  Validation Message: {result['validation_message']}")
        lines.append("")

        # Complexity Analysis
        complexity = result.get('complexity_analysis', {})
        if complexity:
            lines.append("COMPLEXITY ANALYSIS:")

            gt = complexity.get('ground_truth', {})
            pred = complexity.get('predicted', {})

            lines.append(f"  Ground Truth Complexity: {gt.get('complexity_level', 'unknown')}")
            lines.append(f"  Predicted Complexity: {pred.get('complexity_level', 'unknown')}")

            feature_match = complexity.get('feature_match', {})
            lines.append("  Feature Matching:")
            for feature, matches in feature_match.items():
                status = "✓" if matches else "✗"
                lines.append(f"    {feature}: {status}")
            lines.append("")

        # Issues Identified
        issues = result.get('issues', [])
        lines.append("ISSUES IDENTIFIED:")
        if issues:
            for issue in issues:
                lines.append(f"  - {issue.replace('_', ' ').title()}")
        else:
            lines.append("  No issues identified")
        lines.append("")

        # Evidence
        if result.get('evidence'):
            lines.append("EVIDENCE:")
            lines.append(f"  {result['evidence']}")
            lines.append("")

        lines.append("=" * 80)

        # Save report
        report_file = self.output_dir / f"{db_name}_evaluation.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))

    def _generate_summary_report(self, results: List[Dict], analysis: Dict):
        """Generate overall evaluation summary report"""
        lines = []
        lines.append("=" * 80)
        lines.append("TEXT-TO-SQL SYSTEM EVALUATION SUMMARY")
        lines.append("=" * 80)
        lines.append("")

        # Overall Statistics
        lines.append("OVERALL STATISTICS:")
        lines.append(f"  Total Databases: {analysis.get('total_databases', 0)}")
        lines.append(f"  Successful Evaluations: {analysis.get('successful_evaluations', 0)}")
        lines.append(f"  Average Performance Score: {analysis.get('average_performance_score', 0.0)}")
        lines.append(f"  SQL Validity Rate: {analysis.get('sql_validity_rate', 0.0)}")
        lines.append(f"  Exact Match Rate: {analysis.get('exact_match_rate', 0.0)}")
        lines.append("")

        # Complexity Distribution
        complexity_dist = analysis.get('complexity_distribution', {})
        lines.append("COMPLEXITY DISTRIBUTION:")
        for complexity, count in complexity_dist.items():
            lines.append(f"  {complexity.title()}: {count}")
        lines.append("")

        # Common Issues
        common_issues = analysis.get('common_issues', {})
        lines.append("MOST COMMON ISSUES:")
        for issue, count in list(common_issues.items())[:10]:
            lines.append(f"  {issue.replace('_', ' ').title()}: {count}")
        lines.append("")

        # Issue Analysis
        issue_analysis = analysis.get('issue_analysis', {})

        critical = issue_analysis.get('critical_issues', [])
        if critical:
            lines.append("CRITICAL ISSUES (>70% of cases):")
            for issue_info in critical:
                lines.append(f"  - {issue_info['issue'].replace('_', ' ').title()}: {issue_info['rate']*100:.1f}%")
            lines.append("")

        moderate = issue_analysis.get('moderate_issues', [])
        if moderate:
            lines.append("MODERATE ISSUES (30-70% of cases):")
            for issue_info in moderate:
                lines.append(f"  - {issue_info['issue'].replace('_', ' ').title()}: {issue_info['rate']*100:.1f}%")
            lines.append("")

        # Recommendations
        recommendations = issue_analysis.get('recommendations', [])
        lines.append("RECOMMENDATIONS FOR IMPROVEMENT:")
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                lines.append(f"  {i}. {rec}")
        else:
            lines.append("  No specific recommendations - system performing well")
        lines.append("")

        # Database-specific Performance
        lines.append("DATABASE-SPECIFIC PERFORMANCE:")
        for result in results:
            if 'error' not in result:
                score = result.get('performance_score', 0.0)
                status = "🟢" if score >= 0.8 else "🟡" if score >= 0.5 else "🔴"
                lines.append(f"  {status} {result['database_name']}: {score}")
        lines.append("")

        lines.append("=" * 80)
        lines.append("Generated by Text-to-SQL Evaluation System")
        lines.append("=" * 80)

        # Save summary report
        summary_file = self.output_dir / "evaluation_summary.txt"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))

        # Also save as JSON for programmatic use
        json_file = self.output_dir / "evaluation_results.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({
                'individual_results': results,
                'overall_analysis': analysis
            }, f, indent=2, ensure_ascii=False)


def main():
    """Main entry point"""
    logger.info("Starting Database Evaluation")

    try:
        evaluator = DatabaseEvaluator()
        results = evaluator.run_comprehensive_evaluation()

        logger.info("Database evaluation completed successfully!")

        # Print summary
        analysis = results.get('overall_analysis', {})
        print(f"\n📊 Evaluation Summary:")
        print(f"   Average Score: {analysis.get('average_performance_score', 0.0)}")
        print(f"   SQL Validity: {analysis.get('sql_validity_rate', 0.0)*100:.1f}%")
        print(f"   Exact Match: {analysis.get('exact_match_rate', 0.0)*100:.1f}%")

    except Exception as e:
        logger.error(f"Evaluation failed: {e}")
        raise


if __name__ == "__main__":
    main()