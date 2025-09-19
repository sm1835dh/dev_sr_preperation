"""
Test embedding functionality with different deployment names
"""
import logging
from openai import AzureOpenAI
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from configs.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_embedding_deployment():
    """Test different embedding deployment names"""
    config = Config()

    # Test models to try
    models_to_test = [
        "text-embedding-ada-002",
        "text-embedding-3-large",
        "text-embedding-3-small",
        "ada",
        "embedding"
    ]

    client = AzureOpenAI(
        azure_endpoint=config.EMBEDDING_ENDPOINT,
        api_key=config.EMBEDDING_API_KEY,
        api_version=config.EMBEDDING_API_VERSION
    )

    test_text = "Hello world"

    for model in models_to_test:
        try:
            logger.info(f"Testing model: {model}")
            response = client.embeddings.create(
                model=model,
                input=test_text
            )
            embedding = response.data[0].embedding
            logger.info(f"✅ SUCCESS: {model} - embedding dimension: {len(embedding)}")

            # Update .env file with working model
            with open(Path(__file__).parent / '.env', 'r') as f:
                content = f.read()

            # Replace embedding deployment name
            updated_content = content.replace(
                f"EMBEDDING_DEPLOYMENT_NAME={config.EMBEDDING_DEPLOYMENT}",
                f"EMBEDDING_DEPLOYMENT_NAME={model}"
            )

            with open(Path(__file__).parent / '.env', 'w') as f:
                f.write(updated_content)

            logger.info(f"Updated .env with working model: {model}")
            return True

        except Exception as e:
            logger.error(f"❌ FAILED: {model} - {e}")
            continue

    logger.error("No working embedding models found!")
    return False

if __name__ == "__main__":
    test_embedding_deployment()