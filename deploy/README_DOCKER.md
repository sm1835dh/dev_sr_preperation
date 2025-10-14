# Docker Deployment Guide

## Prerequisites
- Docker installed (version 20.10+)
- Docker Compose installed (version 2.0+)
- `.env` file with required environment variables

## Environment Variables
Create a `.env` file with the following variables:

```bash
# PostgreSQL Configuration
PG_HOST=your-postgresql-host
PG_PORT=5432
PG_DATABASE=your-database
PG_USER=your-username
PG_PASSWORD=your-password

# MongoDB/Cosmos DB Configuration
MONGODB_CONNECTION_STRING=mongodb://...
# or
COSMOS_CONNECTION_STRING=mongodb://...

# Azure OpenAI Configuration
ENDPOINT_URL=https://your-openai-endpoint.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AZURE_API_VERSION=2024-02-01
DEPLOYMENT_NAME=your-deployment-name

# Docker Compose Variables (optional)
MODE=both  # direct, metadata, or both
TABLE_NAME=kt_merged_product_20251001
DIRECT_COLLECTION=product_data_direct
METADATA_COLLECTION=synonyms_20251014
BATCH_SIZE=50
SAMPLE_SIZE=10000
```

## Building the Docker Image

### Build the image
```bash
docker build -t rubicon-data-ingestion:latest .
```

### Or use docker-compose
```bash
docker-compose build
```

## Running the Container

### Option 1: Using Docker Run
```bash
# Direct transfer only
docker run --env-file .env rubicon-data-ingestion:latest \
  --mode direct \
  --direct-collection product_raw_data

# Metadata generation only
docker run --env-file .env rubicon-data-ingestion:latest \
  --mode metadata \
  --metadata-collection product_descriptions

# Both operations
docker run --env-file .env rubicon-data-ingestion:latest \
  --mode both
```

### Option 2: Using Docker Compose (Recommended)
```bash
# Run with default settings
docker-compose up

# Run in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the container
docker-compose down
```

### Option 3: Override settings with environment variables
```bash
MODE=direct BATCH_SIZE=100 docker-compose up
```

## Volume Mounts
The docker-compose configuration includes volume mounts for:
- `./metadata:/app/metadata` - Backup JSON files
- `./logs:/app/logs` - Application logs (if implemented)

## Troubleshooting

### Check container logs
```bash
docker logs rubicon-data-ingestion
```

### Execute commands inside container
```bash
docker exec -it rubicon-data-ingestion bash
```

### Test database connections
```bash
# Test PostgreSQL connection
docker exec -it rubicon-data-ingestion python -c "from db_connector import DatabaseConnector; db = DatabaseConnector(); conn = db.get_postgres_connection()"

# Test MongoDB connection
docker exec -it rubicon-data-ingestion python -c "from db_connector import DatabaseConnector; db = DatabaseConnector(); client = db.get_mongodb_client()"
```

## Production Deployment

### Using Azure Container Registry (ACR)
```bash
# Login to ACR
az acr login --name yourregistry

# Tag the image
docker tag rubicon-data-ingestion:latest yourregistry.azurecr.io/rubicon-data-ingestion:latest

# Push to ACR
docker push yourregistry.azurecr.io/rubicon-data-ingestion:latest
```

### Using Azure Container Instances (ACI)
```bash
az container create \
  --resource-group your-rg \
  --name rubicon-data-ingestion \
  --image yourregistry.azurecr.io/rubicon-data-ingestion:latest \
  --registry-login-server yourregistry.azurecr.io \
  --registry-username <username> \
  --registry-password <password> \
  --secure-environment-variables \
    PG_PASSWORD="$PG_PASSWORD" \
    AZURE_OPENAI_API_KEY="$AZURE_OPENAI_API_KEY" \
    MONGODB_CONNECTION_STRING="$MONGODB_CONNECTION_STRING"
```

### Using Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rubicon-data-ingestion
spec:
  replicas: 1
  selector:
    matchLabels:
      app: rubicon-data-ingestion
  template:
    metadata:
      labels:
        app: rubicon-data-ingestion
    spec:
      containers:
      - name: data-ingestion
        image: yourregistry.azurecr.io/rubicon-data-ingestion:latest
        envFrom:
        - secretRef:
            name: rubicon-secrets
        args:
        - "--mode"
        - "both"
        - "--batch-size"
        - "100"
```

## Monitoring

### Check resource usage
```bash
docker stats rubicon-data-ingestion
```

### Health check
```bash
docker inspect rubicon-data-ingestion --format='{{.State.Health.Status}}'
```

## Cleanup

### Remove container
```bash
docker-compose down
```

### Remove image
```bash
docker rmi rubicon-data-ingestion:latest
```

### Clean up volumes
```bash
docker volume prune
```