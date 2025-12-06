#!/bin/bash
set -e

ENVIRONMENT_NAME="${1:-gdpr-healthcare}"
REGION="${2:-eu-central-1}"

echo "Tearing down GDPR Healthcare Pipeline infrastructure..."
echo "Environment: ${ENVIRONMENT_NAME}"
echo "Region: ${REGION}"
echo ""

# Delete in reverse order of dependencies
echo "[1/3] Deleting storage and ingestion stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" 2>/dev/null || echo "Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" 2>/dev/null || true

echo "[2/3] Deleting security stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --region "${REGION}" 2>/dev/null || echo "Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --region "${REGION}" 2>/dev/null || true

echo "[3/3] Deleting networking stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --region "${REGION}" 2>/dev/null || echo "Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --region "${REGION}" 2>/dev/null || true

echo ""
echo "Teardown complete!"
echo ""
echo "Note: S3 bucket has DeletionPolicy=Retain and must be emptied/deleted manually:"
echo "  aws s3 rb s3://${ENVIRONMENT_NAME}-raw-<account-id>-${REGION} --force"
