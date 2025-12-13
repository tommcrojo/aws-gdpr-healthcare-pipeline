#!/bin/bash
set -e

ENVIRONMENT_NAME="${1:-gdpr-healthcare}"
REGION="${2:-eu-central-1}"

echo "Tearing down GDPR Healthcare Pipeline infrastructure..."
echo "Environment: ${ENVIRONMENT_NAME}"
echo "Region: ${REGION}"
echo ""

# Delete in reverse order of dependencies
echo "[1/4] Deleting processing stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --region "${REGION}" 2>/dev/null || echo "Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --region "${REGION}" 2>/dev/null || true

echo "[2/4] Deleting storage and ingestion stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" 2>/dev/null || echo "Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" 2>/dev/null || true

echo "[3/4] Deleting security stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --region "${REGION}" 2>/dev/null || echo "Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --region "${REGION}" 2>/dev/null || true

echo "[4/4] Deleting networking stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --region "${REGION}" 2>/dev/null || echo "Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --region "${REGION}" 2>/dev/null || true

echo ""
echo "Teardown complete!"
echo ""
echo "Note: S3 buckets have DeletionPolicy=Retain and must be emptied/deleted manually:"
echo "  aws s3 rb s3://<bucket-name> --force"
echo ""
echo "Buckets to clean up:"
echo "  - Raw data bucket"
echo "  - Curated data bucket"
echo "  - Quarantine data bucket"
echo ""
echo "Note: The KMS stack (${ENVIRONMENT_NAME}-kms) is NOT deleted."
echo "      This is intentional - the KMS key persists to avoid \$1/month charges on redeploy."
echo "      To delete the KMS stack: aws cloudformation delete-stack --stack-name ${ENVIRONMENT_NAME}-kms"
