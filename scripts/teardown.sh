#!/bin/bash
set -e

ENVIRONMENT_NAME="${1:-gdpr-healthcare}"
REGION="${2:-eu-central-1}"

echo "============================================"
echo "GDPR Healthcare Pipeline - Teardown"
echo "============================================"
echo "Environment: ${ENVIRONMENT_NAME}"
echo "Region: ${REGION}"
echo ""

# Function to empty S3 bucket
empty_bucket() {
    local bucket_name=$1
    if aws s3api head-bucket --bucket "${bucket_name}" 2>/dev/null; then
        echo "  Emptying bucket: ${bucket_name}"
        aws s3 rm "s3://${bucket_name}" --recursive --quiet 2>/dev/null || true
    fi
}

# Get bucket names from stack outputs before deletion
echo "[0/6] Getting S3 bucket names..."
RAW_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --query "Stacks[0].Outputs[?OutputKey=='RawBucketName'].OutputValue" \
    --output text --region "${REGION}" 2>/dev/null) || true

CURATED_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --query "Stacks[0].Outputs[?OutputKey=='CuratedBucketName'].OutputValue" \
    --output text --region "${REGION}" 2>/dev/null) || true

QUARANTINE_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --query "Stacks[0].Outputs[?OutputKey=='QuarantineBucketName'].OutputValue" \
    --output text --region "${REGION}" 2>/dev/null) || true

SCRIPTS_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --query "Stacks[0].Outputs[?OutputKey=='GlueScriptsBucketName'].OutputValue" \
    --output text --region "${REGION}" 2>/dev/null) || true

# Empty S3 buckets (required before stack deletion)
echo "[1/6] Emptying S3 buckets..."
[ -n "${RAW_BUCKET}" ] && [ "${RAW_BUCKET}" != "None" ] && empty_bucket "${RAW_BUCKET}"
[ -n "${CURATED_BUCKET}" ] && [ "${CURATED_BUCKET}" != "None" ] && empty_bucket "${CURATED_BUCKET}"
[ -n "${QUARANTINE_BUCKET}" ] && [ "${QUARANTINE_BUCKET}" != "None" ] && empty_bucket "${QUARANTINE_BUCKET}"
[ -n "${SCRIPTS_BUCKET}" ] && [ "${SCRIPTS_BUCKET}" != "None" ] && empty_bucket "${SCRIPTS_BUCKET}"

# Delete in reverse order of dependencies
echo "[2/7] Deleting compliance stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-compliance" \
    --region "${REGION}" 2>/dev/null || echo "  Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-compliance" \
    --region "${REGION}" 2>/dev/null || true

echo "[3/7] Deleting processing stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --region "${REGION}" 2>/dev/null || echo "  Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --region "${REGION}" 2>/dev/null || true

echo "[4/7] Deleting redshift stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-redshift" \
    --region "${REGION}" 2>/dev/null || echo "  Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-redshift" \
    --region "${REGION}" 2>/dev/null || true

echo "[5/7] Deleting storage and ingestion stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" 2>/dev/null || echo "  Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" 2>/dev/null || true

echo "[6/7] Deleting security stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --region "${REGION}" 2>/dev/null || echo "  Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --region "${REGION}" 2>/dev/null || true

echo "[7/7] Deleting networking stack..."
aws cloudformation delete-stack \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --region "${REGION}" 2>/dev/null || echo "  Stack not found or already deleted"

aws cloudformation wait stack-delete-complete \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --region "${REGION}" 2>/dev/null || true

echo ""
echo "============================================"
echo "Teardown complete!"
echo "============================================"
echo ""
echo "Note: The KMS stack (${ENVIRONMENT_NAME}-kms) is NOT deleted."
echo "      This is intentional - KMS keys have a minimum 7-day deletion window"
echo "      and keeping the stack avoids \$1/month charge on each redeploy."
echo ""
echo "      To delete the KMS stack:"
echo "      aws cloudformation delete-stack --stack-name ${ENVIRONMENT_NAME}-kms"
echo ""
