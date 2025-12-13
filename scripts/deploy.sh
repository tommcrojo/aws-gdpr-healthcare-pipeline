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

# Check if KMS stack exists
KMS_STACK_NAME="${ENVIRONMENT_NAME}-kms"
KMS_STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name "${KMS_STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [ "$KMS_STACK_STATUS" == "DOES_NOT_EXIST" ] || [ "$KMS_STACK_STATUS" == "DELETE_COMPLETE" ]; then
    echo "ERROR: KMS stack not found!"
    echo ""
    echo "Please run setup.sh first to create the KMS key:"
    echo "  ./scripts/setup.sh"
    echo ""
    exit 1
fi

# Get KMS key info from foundation stack
KMS_KEY_ARN=$(aws cloudformation describe-stacks \
    --stack-name "${KMS_STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`KmsKeyArn`].OutputValue' \
    --output text)

KMS_KEY_ID=$(aws cloudformation describe-stacks \
    --stack-name "${KMS_STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`KmsKeyId`].OutputValue' \
    --output text)

echo "Using KMS Key: ${KMS_KEY_ARN}"
echo ""

# Deploy networking stack
echo "[1/4] Deploying networking stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/networking.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[1/4] Networking stack deployed successfully."

# Deploy security stack
echo "[2/4] Deploying security stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/security.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-security" \
    --parameter-overrides \
        EnvironmentName="${ENVIRONMENT_NAME}" \
        KmsKeyArn="${KMS_KEY_ARN}" \
        KmsKeyId="${KMS_KEY_ID}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[2/4] Security stack deployed successfully."

# Deploy storage and ingestion stack
echo "[3/4] Deploying storage and ingestion stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/storage-ingestion.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[3/4] Storage and ingestion stack deployed successfully."

# Deploy processing stack
echo "[4/4] Deploying processing stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/processing.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[4/4] Processing stack deployed successfully."

# Upload ETL script to Glue scripts bucket
GLUE_SCRIPTS_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueScriptsBucketName`].OutputValue' \
    --output text)

echo "Uploading ETL script to s3://${GLUE_SCRIPTS_BUCKET}/scripts/"
aws s3 cp "${SCRIPT_DIR}/../src/processing/etl_job.py" \
    "s3://${GLUE_SCRIPTS_BUCKET}/scripts/etl_job.py" \
    --region "${REGION}"

echo ""
echo "All stacks deployed successfully!"
echo ""
echo "Stack outputs:"
aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs' \
    --output table
