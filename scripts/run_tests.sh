#!/bin/bash
# GDPR Healthcare Pipeline - Test Runner
#
# Usage:
#   ./scripts/run_tests.sh           # Run all tests
#   ./scripts/run_tests.sh phase1    # Run Phase 1 tests only
#   ./scripts/run_tests.sh phase2    # Run Phase 2 tests only
#   ./scripts/run_tests.sh phase3    # Run Phase 3 tests only
#   ./scripts/run_tests.sh fast      # Run non-slow tests only
#   ./scripts/run_tests.sh all       # Run all tests with verbose output

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Default environment
export ENVIRONMENT_NAME="${ENVIRONMENT_NAME:-gdpr-healthcare}"
export AWS_REGION="${AWS_REGION:-eu-central-1}"

cd "${PROJECT_DIR}"

# Install test dependencies if needed
if ! python -c "import pytest" 2>/dev/null; then
    echo "Installing test dependencies..."
    pip install -r tests/requirements.txt
fi

echo "============================================"
echo "GDPR Healthcare Pipeline - Test Suite"
echo "============================================"
echo "Environment: ${ENVIRONMENT_NAME}"
echo "Region: ${AWS_REGION}"
echo ""

case "${1:-all}" in
    phase1)
        echo "Running Phase 1 tests (Infrastructure)..."
        pytest -m phase1 -v
        ;;
    phase2)
        echo "Running Phase 2 tests (Processing)..."
        pytest -m phase2 -v
        ;;
    phase3)
        echo "Running Phase 3 tests (Redshift)..."
        pytest -m phase3 -v
        ;;
    fast)
        echo "Running fast tests only (excluding slow tests)..."
        pytest -m "not slow" -v
        ;;
    integration)
        echo "Running integration tests..."
        pytest -m integration -v
        ;;
    all)
        echo "Running all tests..."
        pytest -v
        ;;
    *)
        echo "Usage: $0 {phase1|phase2|phase3|fast|integration|all}"
        echo ""
        echo "Options:"
        echo "  phase1      - Infrastructure tests (KMS, VPC, S3, Firehose)"
        echo "  phase2      - Processing tests (Glue ETL, data validation)"
        echo "  phase3      - Redshift integration tests"
        echo "  fast        - All tests except slow ones"
        echo "  integration - Integration tests only"
        echo "  all         - Run all tests (default)"
        exit 1
        ;;
esac

echo ""
echo "Tests completed!"
