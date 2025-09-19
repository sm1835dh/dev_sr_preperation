"""
Configuration module for Text-to-SQL implementation
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

class Config:
    """Configuration settings for the Text-to-SQL system"""

    # Azure OpenAI settings for text generation
    AZURE_OPENAI_ENDPOINT = os.getenv("ENDPOINT_URL")
    AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
    AZURE_OPENAI_DEPLOYMENT = os.getenv("DEPLOYMENT_NAME", "gpt-4-1106-preview")
    AZURE_OPENAI_API_VERSION = "2025-01-01-preview"

    # Azure OpenAI settings for embeddings
    EMBEDDING_ENDPOINT = os.getenv("EMBEDDING_ENDPOINT_URL", os.getenv("ENDPOINT_URL"))
    EMBEDDING_API_KEY = os.getenv("EMBEDDING_AZURE_OPENAI_API_KEY", os.getenv("AZURE_OPENAI_API_KEY"))
    EMBEDDING_DEPLOYMENT = os.getenv("EMBEDDING_DEPLOYMENT_NAME", "text-embedding-ada-002")
    EMBEDDING_API_VERSION = os.getenv("EMBEDDING_API_VERSION", "2025-01-01-preview")

    # BIRD Dataset settings
    BIRD_DATASET_PATH = os.getenv("BIRD_DATASET", "/Users/toby/prog/kt/rubicon/dataset/BIRD/train")
    SAMPLE_SIZE = 10  # Use only 10 random samples for development

    # PostgreSQL settings
    DB_NAME = "bird_db"
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")

    # Profiling settings
    TOP_K_VALUES = 10  # Number of top frequent values to collect
    MINHASH_PERMUTATIONS = 128  # Number of hash functions for MinHash
    LSH_THRESHOLD = 0.5  # Similarity threshold for LSH
    SAMPLE_SIZE_PER_FIELD = 10000  # Sample size for field value indexing

    # Schema linking settings
    MAX_RETRIES = 3  # Maximum retries for schema linking
    VECTOR_DIM = 1536  # Dimension for text embeddings (text-embedding-ada-002)

    # SQL generation settings
    NUM_CANDIDATES = 3  # Number of SQL candidates to generate
    FEW_SHOT_EXAMPLES = 8  # Number of few-shot examples to use

    # Paths
    DATA_DIR = Path(__file__).parent.parent / "data"
    EXPERIMENTS_DIR = Path(__file__).parent.parent / "experiments"

    @classmethod
    def get_db_connection_string(cls):
        """Get PostgreSQL connection string"""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_KEY", "EMBEDDING_ENDPOINT", "EMBEDDING_API_KEY", "BIRD_DATASET_PATH"]
        missing = [key for key in required if not getattr(cls, key)]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        return True