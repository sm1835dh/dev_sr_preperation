"""
Main Pipeline Integration Module
Integrates all components to run the complete Text-to-SQL system
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
from modules.database import DatabaseManager
from modules.bird_loader import BIRDLoader
from modules.profiler import DatabaseProfiler
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


class TextToSQLPipeline:
    """Complete Text-to-SQL pipeline integrating all components"""

    def __init__(self):
        self.config = Config()
        self.db_manager = DatabaseManager()
        self.llm_client = AzureOpenAI(
            azure_endpoint=self.config.AZURE_OPENAI_ENDPOINT,
            api_key=self.config.AZURE_OPENAI_KEY,
            api_version=self.config.AZURE_OPENAI_API_VERSION
        )

        # Initialize components
        self.bird_loader = BIRDLoader()
        self.profiler = DatabaseProfiler(self.db_manager)
        self.summarizer = LLMSummarizer()
        self.schema_linker = SchemaLinker(self.llm_client)
        self.sql_generator = SQLGenerator(self.llm_client, self.schema_linker)
        self.evaluator = SQLEvaluator(self.db_manager)

    def setup_system(self) -> List[Dict]:
        """
        Setup the complete system:
        1. Load sample databases
        2. Profile databases
        3. Generate LLM summaries
        4. Build indexes
        """
        logger.info("Setting up Text-to-SQL system...")

        # Step 1: Load sample databases from BIRD
        logger.info("Loading sample databases from BIRD dataset...")
        samples = self.bird_loader.load_sample_databases()
        if not samples:
            logger.error("Failed to load sample databases")
            return []

        # Save sample metadata
        self.bird_loader.save_samples_metadata(samples)

        # Step 2: Profile each database
        logger.info("Profiling databases...")
        for sample in samples:
            schema_name = sample['schema_name']
            logger.info(f"Profiling schema: {schema_name}")

            # Profile database
            profile = self.profiler.profile_database(schema_name)

            # Generate LLM summaries
            logger.info(f"Generating LLM summaries for {schema_name}")
            summaries = self.summarizer.summarize_database(profile)

            # Store results
            sample['profile'] = profile
            sample['summaries'] = summaries

        # Step 3: Build schema linking indexes
        logger.info("Building schema linking indexes...")
        for sample in samples:
            profile = sample['profile']
            summaries = sample['summaries']

            # Build LSH index for literal matching
            self.schema_linker.build_lsh_index(profile)

            # Build FAISS index for semantic similarity
            self.schema_linker.build_faiss_index(summaries)

        # Step 4: Build few-shot example index
        logger.info("Building few-shot example index...")
        examples = []
        for sample in samples:
            examples.append({
                'question': sample['question'],
                'sql': sample['sql'],
                'db_name': sample['db_name'],
                'schema_name': sample['schema_name']
            })

        self.sql_generator.build_few_shot_index(examples)

        logger.info(f"System setup complete with {len(samples)} databases")
        return samples

    def process_question(self, question: str, target_schema: str = None) -> Dict:
        """
        Process a single question through the complete pipeline
        """
        logger.info(f"Processing question: {question}")

        # If no target schema specified, use the first available one
        if not target_schema:
            # This would need to be improved to automatically detect the right schema
            # For now, use the first schema from setup
            target_schema = "bird_chinook"  # Default fallback

        # Get database profile and summaries for the target schema
        profile = self.profiler.profile_database(target_schema)
        summaries = self.summarizer.summarize_database(profile)

        # Generate SQL
        result = self.sql_generator.generate_sql(question, profile, summaries)

        return result

    def run_evaluation(self, test_samples: List[Dict]) -> Dict:
        """
        Run evaluation on test samples
        """
        logger.info("Running evaluation...")

        predictions = []
        ground_truths = []

        for sample in test_samples:
            # Process question
            result = self.process_question(
                sample['question'],
                sample['schema_name']
            )
            predictions.append(result)

            # Prepare ground truth
            ground_truth = {
                'question': sample['question'],
                'sql': sample['sql'],
                'schema_name': sample['schema_name']
            }
            ground_truths.append(ground_truth)

        # Evaluate
        evaluation_results = self.evaluator.evaluate_batch(predictions, ground_truths)

        return evaluation_results

    def run_experiments(self, samples: List[Dict]) -> Dict:
        """
        Run comprehensive experiments
        """
        logger.info("Running experiments...")

        experiment_runner = ExperimentRunner()

        # Split data for training/testing (simple split for demo)
        split_point = int(len(samples) * 0.7)
        train_samples = samples[:split_point]
        test_samples = samples[split_point:]

        logger.info(f"Using {len(train_samples)} samples for training, {len(test_samples)} for testing")

        # Run ablation study
        ablation_results = experiment_runner.run_ablation_study(test_samples)

        # Test schema variations
        schema_results = experiment_runner.compare_schema_variations(test_samples)

        # Measure efficiency
        efficiency_results = experiment_runner.measure_efficiency(test_samples)

        # Combine all results
        all_results = {
            'ablation_study': ablation_results,
            'schema_variations': schema_results,
            'efficiency': efficiency_results,
            'train_size': len(train_samples),
            'test_size': len(test_samples)
        }

        # Save results
        experiment_runner.save_experiment_results(all_results, "complete_evaluation")

        # Generate report
        report = experiment_runner.generate_report(all_results)
        report_path = Path(self.config.DATA_DIR) / "experiment_results" / "evaluation_report.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, 'w') as f:
            f.write(report)

        logger.info(f"Evaluation report saved to {report_path}")

        return all_results

    def save_pipeline_state(self, samples: List[Dict]):
        """Save the current pipeline state for later use"""
        state_path = Path(self.config.DATA_DIR) / "pipeline_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        # Prepare state data (exclude large objects)
        state_data = {
            'samples': samples,
            'config': {
                'sample_size': self.config.SAMPLE_SIZE,
                'top_k_values': self.config.TOP_K_VALUES,
                'lsh_threshold': self.config.LSH_THRESHOLD
            },
            'timestamp': str(Path(__file__).stat().st_mtime)
        }

        with open(state_path, 'w') as f:
            json.dump(state_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Pipeline state saved to {state_path}")


def main():
    """Main entry point"""
    logger.info("Starting Text-to-SQL Pipeline")

    try:
        # Initialize pipeline
        pipeline = TextToSQLPipeline()

        # Setup system
        samples = pipeline.setup_system()
        if not samples:
            logger.error("Failed to setup system")
            return

        # Save pipeline state
        pipeline.save_pipeline_state(samples)

        # Test with a sample question
        test_question = "How many customers are there?"
        result = pipeline.process_question(test_question)

        logger.info("Sample question processing result:")
        logger.info(f"Question: {result['question']}")
        logger.info(f"Generated SQL: {result['final_sql']}")
        logger.info(f"Valid: {result['is_valid']}")

        # Run evaluation
        evaluation_results = pipeline.run_evaluation(samples[:5])  # Test on first 5 samples
        logger.info(f"Evaluation results: {evaluation_results}")

        # Run experiments
        experiment_results = pipeline.run_experiments(samples)
        logger.info("Experiments completed")

        logger.info("Pipeline execution completed successfully")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()