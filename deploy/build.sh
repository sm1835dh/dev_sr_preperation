#!/bin/bash

# =============================================================================
# Docker Build Script for Rubicon Data Ingestion
# =============================================================================
# Description: Build Docker image for data ingestion pipeline
# Usage: ./build.sh [--clean] [--tag TAG_NAME]
# =============================================================================

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
IMAGE_NAME="rubicon-data-ingestion"
TAG="latest"
NO_CACHE=""

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
    echo "  Docker Build - Rubicon Data Ingestion"
    echo "=============================================="
    echo ""
}

# Check Docker installation
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_message "$RED" "❌ Error: Docker is not installed"
        exit 1
    fi
    print_message "$GREEN" "✓ Docker found: $(docker --version)"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --clean)
                NO_CACHE="--no-cache"
                print_message "$YELLOW" "📌 Clean build enabled (no cache)"
                shift
                ;;
            --tag)
                TAG="$2"
                print_message "$BLUE" "📌 Custom tag: $TAG"
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
Usage: ./build.sh [OPTIONS]

Options:
    --clean         Build without using cache
    --tag TAG       Specify custom tag (default: latest)
    -h, --help      Show this help message

Examples:
    ./build.sh                    # Basic build
    ./build.sh --clean            # Clean build without cache
    ./build.sh --tag v1.0.0       # Build with custom tag
    ./build.sh --clean --tag dev  # Clean build with custom tag

EOF
}

# Check required files
check_requirements() {
    local required_files=(
        "Dockerfile"
        "requirements.txt"
        "main.py"
        "data_ingestion.py"
        "db_connector.py"
        "schema_analyzer.py"
        "openai_generator.py"
        "mongodb_saver.py"
    )

    print_message "$BLUE" "🔍 Checking required files..."

    local missing_files=()
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            missing_files+=("$file")
        fi
    done

    if [[ ${#missing_files[@]} -gt 0 ]]; then
        print_message "$RED" "❌ Missing required files:"
        for file in "${missing_files[@]}"; do
            echo "   - $file"
        done
        exit 1
    fi

    print_message "$GREEN" "✓ All required files present"
}

# Clean old images
clean_old_images() {
    print_message "$BLUE" "🧹 Cleaning old images..."

    # Get dangling images
    local dangling_images=$(docker images -f "dangling=true" -q 2>/dev/null)
    if [[ ! -z "$dangling_images" ]]; then
        docker rmi $dangling_images 2>/dev/null || true
        print_message "$GREEN" "✓ Removed dangling images"
    fi

    # Optional: Remove old versions of the same image
    local old_images=$(docker images "$IMAGE_NAME" -q | tail -n +2 2>/dev/null)
    if [[ ! -z "$old_images" ]]; then
        print_message "$YELLOW" "📌 Found old versions of $IMAGE_NAME"
        # Not removing automatically, just informing
    fi
}

# Build Docker image
build_image() {
    local full_tag="${IMAGE_NAME}:${TAG}"

    print_message "$BLUE" "🔨 Building Docker image: $full_tag"
    echo ""

    # Build command
    local build_cmd="docker build $NO_CACHE -t $full_tag ."

    # Show build command
    print_message "$YELLOW" "📌 Build command: $build_cmd"
    echo ""

    # Execute build
    if $build_cmd; then
        echo ""
        print_message "$GREEN" "✅ Build successful!"
    else
        echo ""
        print_message "$RED" "❌ Build failed!"
        exit 1
    fi
}

# Show image info
show_image_info() {
    local full_tag="${IMAGE_NAME}:${TAG}"

    print_message "$BLUE" "📊 Image Information:"
    echo ""

    # Get image size
    local image_size=$(docker images "$full_tag" --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}" | tail -n 1)
    echo "   $image_size"

    # Get image ID
    local image_id=$(docker images "$full_tag" -q | head -n 1)
    echo "   ID: $image_id"

    # Get creation time
    local created=$(docker inspect "$full_tag" --format='{{.Created}}' 2>/dev/null | cut -d'T' -f1)
    echo "   Created: $created"
}

# Show next steps
show_next_steps() {
    echo ""
    print_message "$BLUE" "📝 Next Steps:"
    echo ""
    echo "   1. Create/Update .env file (if not exists):"
    echo "      ./setup.sh"
    echo ""
    echo "   2. Run the container:"
    echo "      ./run.sh"
    echo ""
    echo "   3. Run with custom parameters:"
    echo "      ./run.sh --table your_table_name --mode direct"
    echo ""
}

# Main execution
main() {
    print_header

    # Parse arguments
    parse_args "$@"

    # Check Docker
    check_docker

    # Check requirements
    check_requirements

    # Clean old images
    clean_old_images

    # Build image
    build_image

    # Show image info
    show_image_info

    # Show next steps
    show_next_steps

    print_message "$GREEN" "✨ Build process completed successfully!"
    echo ""
}

# Run main function
main "$@"