#!/bin/bash

# =============================================================================
# Docker Run Script for Rubicon Data Ingestion
# =============================================================================
# Description: Run Docker container with flexible parameter overrides
# Usage: ./run.sh [OPTIONS]
# =============================================================================

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
IMAGE_NAME="rubicon-data-ingestion"
TAG="latest"
CONTAINER_NAME="rubicon-data-ingestion"
ENV_FILE=".env"

# Runtime parameters (can be overridden)
TABLE_NAME=""
DIRECT_COLLECTION=""
METADATA_COLLECTION=""
MODE=""
BATCH_SIZE=""
SAMPLE_SIZE=""

# Flags
DETACH_MODE=""
FOLLOW_LOGS=""
REMOVE_EXISTING=""

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
    echo "=============================================="
    echo "  Docker Run - Rubicon Data Ingestion"
    echo "=============================================="
    echo ""
}

# Check Docker
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_message "$RED" "❌ Error: Docker is not installed"
        exit 1
    fi
}

# Check image exists
check_image() {
    local full_tag="${IMAGE_NAME}:${TAG}"

    if ! docker images "$full_tag" | grep -q "$IMAGE_NAME"; then
        print_message "$RED" "❌ Error: Image '$full_tag' not found"
        print_message "$YELLOW" "📌 Please run ./build.sh first"
        exit 1
    fi

    print_message "$GREEN" "✓ Image found: $full_tag"
}

# Check environment file
check_env_file() {
    if [[ ! -f "$ENV_FILE" ]]; then
        print_message "$RED" "❌ Error: Environment file '$ENV_FILE' not found"
        print_message "$YELLOW" "📌 Please run ./setup.sh to create .env file"
        exit 1
    fi

    # Check if required variables exist
    local required_vars=(
        "AZURE_OPENAI_API_KEY"
        "PG_PASSWORD"
        "MONGODB_CONNECTION_STRING"
    )

    local missing_vars=()
    for var in "${required_vars[@]}"; do
        if ! grep -q "^${var}=" "$ENV_FILE"; then
            missing_vars+=("$var")
        fi
    done

    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        print_message "$RED" "❌ Missing required variables in $ENV_FILE:"
        for var in "${missing_vars[@]}"; do
            echo "   - $var"
        done
        print_message "$YELLOW" "📌 Please run ./setup.sh to configure environment"
        exit 1
    fi

    print_message "$GREEN" "✓ Environment file validated"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --table)
                TABLE_NAME="$2"
                shift 2
                ;;
            --direct-collection)
                DIRECT_COLLECTION="$2"
                shift 2
                ;;
            --metadata-collection)
                METADATA_COLLECTION="$2"
                shift 2
                ;;
            --mode)
                MODE="$2"
                if [[ ! "$MODE" =~ ^(both|direct|metadata)$ ]]; then
                    print_message "$RED" "❌ Invalid mode: $MODE (use: both|direct|metadata)"
                    exit 1
                fi
                shift 2
                ;;
            --batch-size)
                BATCH_SIZE="$2"
                shift 2
                ;;
            --sample-size)
                SAMPLE_SIZE="$2"
                shift 2
                ;;
            --detach|-d)
                DETACH_MODE="-d"
                shift
                ;;
            --follow|-f)
                FOLLOW_LOGS="true"
                shift
                ;;
            --rm)
                REMOVE_EXISTING="true"
                shift
                ;;
            --env-file)
                ENV_FILE="$2"
                shift 2
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                print_message "$RED" "❌ Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# Show help
show_help() {
    cat << EOF
Usage: ./run.sh [OPTIONS]

Runtime Parameters:
    --table TABLE_NAME              Override TABLE_NAME
    --direct-collection NAME        Override DIRECT_COLLECTION
    --metadata-collection NAME      Override METADATA_COLLECTION
    --mode [both|direct|metadata]   Override MODE
    --batch-size NUMBER             Override BATCH_SIZE
    --sample-size NUMBER            Override SAMPLE_SIZE

Container Options:
    -d, --detach                    Run container in background
    -f, --follow                    Follow logs after starting
    --rm                            Remove existing container before starting
    --env-file FILE                 Use custom environment file (default: .env)

Other Options:
    -h, --help                      Show this help message

Examples:
    # Basic run with defaults from .env
    ./run.sh

    # Run with different table
    ./run.sh --table kt_merged_product_20251015

    # Run metadata generation only
    ./run.sh --mode metadata --table test_table

    # Run in background and follow logs
    ./run.sh -d -f

    # Full custom run
    ./run.sh \\
        --table new_table_20251015 \\
        --direct-collection product_v2 \\
        --metadata-collection synonyms_v2 \\
        --mode both \\
        --batch-size 100 \\
        --sample-size 5000

EOF
}

# Check if container is running
check_container() {
    local container_status=$(docker ps -a --filter "name=${CONTAINER_NAME}" --format "{{.Status}}" 2>/dev/null | head -n 1)

    if [[ ! -z "$container_status" ]]; then
        if [[ "$container_status" =~ ^Up ]]; then
            print_message "$YELLOW" "⚠️  Container '$CONTAINER_NAME' is already running"

            if [[ "$REMOVE_EXISTING" == "true" ]]; then
                print_message "$YELLOW" "📌 Stopping and removing existing container..."
                docker stop "$CONTAINER_NAME" 2>/dev/null || true
                docker rm "$CONTAINER_NAME" 2>/dev/null || true
                print_message "$GREEN" "✓ Existing container removed"
            else
                print_message "$RED" "❌ Please stop the container or use --rm flag"
                echo "   docker stop $CONTAINER_NAME"
                echo "   docker rm $CONTAINER_NAME"
                echo "   Or: ./run.sh --rm [other options]"
                exit 1
            fi
        else
            # Container exists but not running
            print_message "$YELLOW" "📌 Removing stopped container..."
            docker rm "$CONTAINER_NAME" 2>/dev/null || true
        fi
    fi
}

# Build docker run command
build_run_command() {
    local full_tag="${IMAGE_NAME}:${TAG}"
    local cmd="docker run"

    # Add detach mode if specified
    if [[ ! -z "$DETACH_MODE" ]]; then
        cmd="$cmd -d"
    else
        cmd="$cmd -it"
    fi

    # Add container name
    cmd="$cmd --name $CONTAINER_NAME"

    # Add environment file
    cmd="$cmd --env-file $ENV_FILE"

    # Add environment variable overrides
    if [[ ! -z "$TABLE_NAME" ]]; then
        cmd="$cmd -e TABLE_NAME='$TABLE_NAME'"
    fi

    if [[ ! -z "$DIRECT_COLLECTION" ]]; then
        cmd="$cmd -e DIRECT_COLLECTION='$DIRECT_COLLECTION'"
    fi

    if [[ ! -z "$METADATA_COLLECTION" ]]; then
        cmd="$cmd -e METADATA_COLLECTION='$METADATA_COLLECTION'"
    fi

    if [[ ! -z "$MODE" ]]; then
        cmd="$cmd -e MODE='$MODE'"
    fi

    if [[ ! -z "$BATCH_SIZE" ]]; then
        cmd="$cmd -e BATCH_SIZE='$BATCH_SIZE'"
    fi

    if [[ ! -z "$SAMPLE_SIZE" ]]; then
        cmd="$cmd -e SAMPLE_SIZE='$SAMPLE_SIZE'"
    fi

    # Add image
    cmd="$cmd $full_tag"

    echo "$cmd"
}

# Show configuration
show_configuration() {
    print_message "$BLUE" "📋 Configuration:"
    echo ""

    # Load defaults from .env
    if [[ -f "$ENV_FILE" ]]; then
        source <(grep -E '^(TABLE_NAME|DIRECT_COLLECTION|METADATA_COLLECTION|MODE|BATCH_SIZE|SAMPLE_SIZE)=' "$ENV_FILE" | sed 's/^/local /')
    fi

    # Show final values (overrides applied)
    echo "   Table Name:        ${TABLE_NAME:-$TABLE_NAME (from .env)}"
    echo "   Direct Collection: ${DIRECT_COLLECTION:-$DIRECT_COLLECTION (from .env)}"
    echo "   Metadata Collection: ${METADATA_COLLECTION:-$METADATA_COLLECTION (from .env)}"
    echo "   Mode:             ${MODE:-$MODE (from .env)}"
    echo "   Batch Size:       ${BATCH_SIZE:-$BATCH_SIZE (from .env)}"
    echo "   Sample Size:      ${SAMPLE_SIZE:-$SAMPLE_SIZE (from .env)}"
    echo ""
}

# Run container
run_container() {
    local cmd=$(build_run_command)

    print_message "$BLUE" "🚀 Starting container..."
    print_message "$YELLOW" "📌 Command: $cmd"
    echo ""

    # Execute the command
    eval $cmd

    if [[ $? -eq 0 ]]; then
        print_message "$GREEN" "✅ Container started successfully!"

        # Show container info
        if [[ ! -z "$DETACH_MODE" ]]; then
            echo ""
            print_message "$BLUE" "📊 Container Status:"
            docker ps --filter "name=${CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

            # Follow logs if requested
            if [[ "$FOLLOW_LOGS" == "true" ]]; then
                echo ""
                print_message "$CYAN" "📜 Following logs (Ctrl+C to stop)..."
                echo ""
                docker logs -f "$CONTAINER_NAME"
            else
                echo ""
                print_message "$BLUE" "📝 View logs with:"
                echo "   docker logs $CONTAINER_NAME -f"
            fi
        fi
    else
        print_message "$RED" "❌ Failed to start container"
        exit 1
    fi
}

# Show post-run commands
show_post_run_commands() {
    if [[ ! -z "$DETACH_MODE" ]] && [[ "$FOLLOW_LOGS" != "true" ]]; then
        echo ""
        print_message "$BLUE" "📝 Useful Commands:"
        echo ""
        echo "   # View logs"
        echo "   docker logs $CONTAINER_NAME -f"
        echo ""
        echo "   # Check status"
        echo "   docker ps --filter name=$CONTAINER_NAME"
        echo ""
        echo "   # Stop container"
        echo "   docker stop $CONTAINER_NAME"
        echo ""
        echo "   # Remove container"
        echo "   docker rm $CONTAINER_NAME"
        echo ""
    fi
}

# Main execution
main() {
    print_header

    # Parse arguments
    parse_args "$@"

    # Checks
    check_docker
    check_image
    check_env_file
    check_container

    # Show configuration
    show_configuration

    # Run container
    run_container

    # Show post-run commands
    show_post_run_commands
}

# Run main function
main "$@"