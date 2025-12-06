#!/bin/bash
set -e

ENVIRONMENT_NAME="${1:-gdpr-healthcare}"
REGION="${2:-eu-central-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="${SCRIPT_DIR}/../infrastructure"

echo "Deploying GDPR Healthcare Pipeline infrastructure..."
echo "Environment: ${ENVIRONMENT_NAME}"
echo "Region: ${REGION}"
echo ""

# Deploy networking stack
echo "[1/3] Deploying networking stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/networking.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[1/3] Networking stack deployed successfully."

# Deploy security stack
echo "[2/3] Deploying security stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/security.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[2/3] Security stack deployed successfully."

# Deploy storage and ingestion stack
echo "[3/3] Deploying storage and ingestion stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/storage-ingestion.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[3/3] Storage and ingestion stack deployed successfully."

echo ""
echo "All stacks deployed successfully!"
echo ""
echo "Stack outputs:"
aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs' \
    --output table
