#!/bin/bash

# =============================================================================
# Setup Script for Rubicon Data Ingestion
# =============================================================================
# Description: Initial setup and environment configuration helper
# Usage: ./setup.sh
# =============================================================================

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Files
ENV_EXAMPLE=".env.example"
ENV_FILE=".env"

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Print header
print_header() {
    echo ""
    echo "================================================"
    echo "    🚀 Rubicon Data Ingestion Setup"
    echo "================================================"
    echo ""
}

# Check if .env exists
check_existing_env() {
    if [[ -f "$ENV_FILE" ]]; then
        print_message "$YELLOW" "⚠️  Found existing .env file"
        echo ""
        echo "What would you like to do?"
        echo "  1) Keep existing .env (update values only)"
        echo "  2) Create fresh .env from template"
        echo "  3) Exit without changes"
        echo ""
        read -p "Choose option [1-3]: " choice

        case $choice in
            1)
                print_message "$BLUE" "📌 Keeping existing .env file"
                return 0
                ;;
            2)
                cp "$ENV_FILE" "${ENV_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
                print_message "$GREEN" "✓ Backed up existing .env"
                create_env_from_template
                ;;
            3)
                print_message "$YELLOW" "👋 Exiting without changes"
                exit 0
                ;;
            *)
                print_message "$RED" "❌ Invalid option"
                exit 1
                ;;
        esac
    else
        create_env_from_template
    fi
}

# Create .env from template
create_env_from_template() {
    if [[ ! -f "$ENV_EXAMPLE" ]]; then
        print_message "$BLUE" "📝 Creating .env.example template..."
        create_env_example
    fi

    print_message "$BLUE" "📝 Creating .env from template..."
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    print_message "$GREEN" "✓ Created .env file"
}

# Create .env.example
create_env_example() {
    cat > "$ENV_EXAMPLE" << 'EOF'
# =============================================================================
# Environment Configuration for Rubicon Data Ingestion
# =============================================================================

# -----------------------------------------------------------------------------
# 🔐 SENSITIVE INFORMATION (Must be configured)
# -----------------------------------------------------------------------------

# Azure OpenAI API Key
AZURE_OPENAI_API_KEY=YOUR_API_KEY_HERE

# PostgreSQL Password
PG_PASSWORD=YOUR_PASSWORD_HERE

# MongoDB/Cosmos DB Connection String
MONGODB_CONNECTION_STRING=mongodb://YOUR_CONNECTION_STRING_HERE

# -----------------------------------------------------------------------------
# 🌐 SERVER CONFIGURATION (Update if different from defaults)
# -----------------------------------------------------------------------------

# PostgreSQL Configuration
PG_HOST=dev-rubicon-postgresql.postgres.database.azure.com
PG_PORT=5432
PG_DATABASE=postgres
PG_USER=rubicon

# Azure OpenAI Configuration
ENDPOINT_URL=https://kt-azure-openai-dev-donghunseo.openai.azure.com/
DEPLOYMENT_NAME=gpt-4.1
AZURE_API_VERSION=2024-02-01

# Embedding Configuration (if using)
EMBEDDING_ENDPOINT_URL=https://kt-azure-openai-dev-donghunseo.openai.azure.com/
EMBEDDING_MODEL_NAME=text-embedding-3-small
EMBEDDING_DEPLOYMENT_NAME=text-embedding-3-small
EMBEDDING_API_VERSION=2024-12-01-preview

# -----------------------------------------------------------------------------
# 🎯 RUNTIME PARAMETERS (Can be overridden via run.sh)
# -----------------------------------------------------------------------------

# Table name to process
TABLE_NAME=kt_merged_product_20251001

# MongoDB collection names
DIRECT_COLLECTION=product_data_direct
METADATA_COLLECTION=synonyms_20251014

# Processing mode: both, direct, or metadata
MODE=both

# Processing parameters
BATCH_SIZE=50
SAMPLE_SIZE=10000

# -----------------------------------------------------------------------------
# 📁 OPTIONAL FILE PATHS (If needed)
# -----------------------------------------------------------------------------

# Upload file path (if using file upload feature)
# PG_UPLOAD_FILE_PATH=/path/to/data/file.tsv

# BIRD dataset path (if using)
# BIRD_DATASET=/path/to/BIRD/train
EOF

    print_message "$GREEN" "✓ Created .env.example template"
}

# Prompt for value with validation
prompt_value() {
    local var_name=$1
    local description=$2
    local is_password=$3
    local current_value=$4

    echo ""
    print_message "$CYAN" "📝 $description"

    if [[ ! -z "$current_value" ]] && [[ "$current_value" != *"YOUR_"* ]] && [[ "$current_value" != "XXXXXXXXX" ]]; then
        if [[ "$is_password" == "true" ]]; then
            echo "   Current: [HIDDEN]"
        else
            echo "   Current: $current_value"
        fi
        read -p "   Enter new value (or press Enter to keep current): " new_value
    else
        if [[ "$is_password" == "true" ]]; then
            read -s -p "   Enter value: " new_value
            echo ""
        else
            read -p "   Enter value: " new_value
        fi
    fi

    if [[ ! -z "$new_value" ]]; then
        # Update the value in .env file
        # Use a different approach to handle special characters in values
        # Create a temporary file with the updated value
        local temp_file="${ENV_FILE}.tmp"

        # Read the file line by line and replace the matching line
        while IFS= read -r line; do
            if [[ "$line" =~ ^${var_name}= ]]; then
                echo "${var_name}=${new_value}"
            else
                echo "$line"
            fi
        done < "$ENV_FILE" > "$temp_file"

        # Move the temporary file to the original
        mv "$temp_file" "$ENV_FILE"

        print_message "$GREEN" "   ✓ Updated"
    elif [[ -z "$current_value" ]] || [[ "$current_value" == *"YOUR_"* ]] || [[ "$current_value" == "XXXXXXXXX" ]]; then
        print_message "$YELLOW" "   ⚠️  Skipped (using default placeholder)"
    else
        print_message "$BLUE" "   → Keeping current value"
    fi
}

# Configure sensitive information
configure_sensitive() {
    print_message "$MAGENTA" "\n🔐 Configuring Sensitive Information"
    print_message "$YELLOW" "   These values are required for the application to work"

    # Get current values
    local api_key=$(grep "^AZURE_OPENAI_API_KEY=" "$ENV_FILE" | cut -d'=' -f2-)
    local pg_pass=$(grep "^PG_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2-)
    local mongo_conn=$(grep "^MONGODB_CONNECTION_STRING=" "$ENV_FILE" | cut -d'=' -f2-)

    prompt_value "AZURE_OPENAI_API_KEY" "Azure OpenAI API Key" "true" "$api_key"
    prompt_value "PG_PASSWORD" "PostgreSQL Password" "true" "$pg_pass"
    prompt_value "MONGODB_CONNECTION_STRING" "MongoDB/Cosmos DB Connection String" "false" "$mongo_conn"
}

# Configure server settings
configure_server() {
    print_message "$MAGENTA" "\n🌐 Configuring Server Settings"
    print_message "$YELLOW" "   Press Enter to keep default values"

    # Get current values
    local pg_host=$(grep "^PG_HOST=" "$ENV_FILE" | cut -d'=' -f2-)
    local pg_user=$(grep "^PG_USER=" "$ENV_FILE" | cut -d'=' -f2-)
    local endpoint=$(grep "^ENDPOINT_URL=" "$ENV_FILE" | cut -d'=' -f2-)

    prompt_value "PG_HOST" "PostgreSQL Host" "false" "$pg_host"
    prompt_value "PG_USER" "PostgreSQL User" "false" "$pg_user"
    prompt_value "ENDPOINT_URL" "Azure OpenAI Endpoint URL" "false" "$endpoint"
}

# Configure runtime parameters
configure_runtime() {
    print_message "$MAGENTA" "\n🎯 Configuring Runtime Parameters"
    print_message "$YELLOW" "   These can be overridden when running ./run.sh"

    # Get current values
    local table_name=$(grep "^TABLE_NAME=" "$ENV_FILE" | cut -d'=' -f2-)
    local direct_coll=$(grep "^DIRECT_COLLECTION=" "$ENV_FILE" | cut -d'=' -f2-)
    local meta_coll=$(grep "^METADATA_COLLECTION=" "$ENV_FILE" | cut -d'=' -f2-)

    prompt_value "TABLE_NAME" "Table Name to Process" "false" "$table_name"
    prompt_value "DIRECT_COLLECTION" "MongoDB Direct Collection Name" "false" "$direct_coll"
    prompt_value "METADATA_COLLECTION" "MongoDB Metadata Collection Name" "false" "$meta_coll"
}

# Test connections
test_connections() {
    echo ""
    print_message "$BLUE" "🔍 Would you like to test connections? (y/n)"
    read -p "   Choice: " test_choice

    if [[ "$test_choice" != "y" ]] && [[ "$test_choice" != "Y" ]]; then
        return
    fi

    print_message "$BLUE" "\n🧪 Testing Connections..."

    # Test PostgreSQL
    echo ""
    print_message "$CYAN" "Testing PostgreSQL connection..."

    # Get values from .env
    source "$ENV_FILE"

    # Test with Python (since psycopg2 will be used)
    if command -v python3 &> /dev/null; then
        python3 << EOF 2>/dev/null
import os
import sys
os.environ['PG_HOST'] = '$PG_HOST'
os.environ['PG_PORT'] = '$PG_PORT'
os.environ['PG_DATABASE'] = '$PG_DATABASE'
os.environ['PG_USER'] = '$PG_USER'
os.environ['PG_PASSWORD'] = '$PG_PASSWORD'

try:
    import psycopg2
    conn = psycopg2.connect(
        host=os.environ['PG_HOST'],
        port=os.environ['PG_PORT'],
        database=os.environ['PG_DATABASE'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD']
    )
    conn.close()
    print("   ✓ PostgreSQL connection successful")
except ImportError:
    print("   ⚠️  psycopg2 not installed (will work in Docker)")
except Exception as e:
    print(f"   ❌ PostgreSQL connection failed: {str(e)[:50]}...")
EOF
    else
        print_message "$YELLOW" "   ⚠️  Python not found, skipping test"
    fi

    # Basic URL validation for Azure OpenAI
    echo ""
    print_message "$CYAN" "Testing Azure OpenAI endpoint..."
    if [[ "$ENDPOINT_URL" == https://* ]]; then
        if curl -s --head "$ENDPOINT_URL" > /dev/null 2>&1; then
            print_message "$GREEN" "   ✓ Endpoint is reachable"
        else
            print_message "$YELLOW" "   ⚠️  Could not reach endpoint (may require API key)"
        fi
    else
        print_message "$RED" "   ❌ Invalid endpoint URL format"
    fi
}

# Show summary
show_summary() {
    echo ""
    print_message "$GREEN" "✅ Setup Complete!"
    echo ""
    print_message "$BLUE" "📋 Configuration Summary:"
    echo ""

    # Source the env file to get values
    set -a
    source "$ENV_FILE"
    set +a

    # Check sensitive values
    local config_complete=true

    echo "   Sensitive Information:"
    if [[ "$AZURE_OPENAI_API_KEY" != *"YOUR_"* ]] && [[ "$AZURE_OPENAI_API_KEY" != "XXXXXXXXX" ]]; then
        echo "     • Azure OpenAI API Key: [CONFIGURED]"
    else
        echo "     • Azure OpenAI API Key: ⚠️  [NOT CONFIGURED]"
        config_complete=false
    fi

    if [[ "$PG_PASSWORD" != *"YOUR_"* ]] && [[ "$PG_PASSWORD" != "XXXXXXXXX" ]]; then
        echo "     • PostgreSQL Password: [CONFIGURED]"
    else
        echo "     • PostgreSQL Password: ⚠️  [NOT CONFIGURED]"
        config_complete=false
    fi

    if [[ "$MONGODB_CONNECTION_STRING" != *"YOUR_"* ]]; then
        echo "     • MongoDB Connection: [CONFIGURED]"
    else
        echo "     • MongoDB Connection: ⚠️  [NOT CONFIGURED]"
        config_complete=false
    fi

    echo ""
    echo "   Runtime Configuration:"
    echo "     • Table: $TABLE_NAME"
    echo "     • Mode: $MODE"
    echo "     • Batch Size: $BATCH_SIZE"

    if [[ "$config_complete" != "true" ]]; then
        echo ""
        print_message "$YELLOW" "⚠️  Some values are not configured"
        print_message "$YELLOW" "   Please update them in .env before running"
    fi

    echo ""
    print_message "$BLUE" "📝 Next Steps:"
    echo ""
    echo "   1. Build Docker image:"
    echo "      ./build.sh"
    echo ""
    echo "   2. Run the container:"
    echo "      ./run.sh"
    echo ""
    echo "   3. Run with custom parameters:"
    echo "      ./run.sh --table your_table --mode direct"
    echo ""
}

# Main menu
main_menu() {
    print_header

    # Check existing .env
    check_existing_env

    echo ""
    print_message "$BLUE" "What would you like to configure?"
    echo ""
    echo "  1) Quick Setup (Sensitive info only)"
    echo "  2) Full Setup (All configurations)"
    echo "  3) Test Connections Only"
    echo "  4) Skip Setup"
    echo ""
    read -p "Choose option [1-4]: " setup_choice

    case $setup_choice in
        1)
            configure_sensitive
            ;;
        2)
            configure_sensitive
            configure_server
            configure_runtime
            ;;
        3)
            if [[ ! -f "$ENV_FILE" ]]; then
                print_message "$RED" "❌ No .env file found"
                exit 1
            fi
            ;;
        4)
            print_message "$YELLOW" "📌 Skipping setup"
            ;;
        *)
            print_message "$RED" "❌ Invalid option"
            exit 1
            ;;
    esac

    # Test connections
    if [[ "$setup_choice" != "4" ]]; then
        test_connections
    fi

    # Show summary
    show_summary
}

# Make scripts executable
make_executable() {
    chmod +x build.sh 2>/dev/null || true
    chmod +x run.sh 2>/dev/null || true
    chmod +x setup.sh 2>/dev/null || true
}

# Main execution
main() {
    make_executable
    main_menu
}

# Run main function
main "$@"