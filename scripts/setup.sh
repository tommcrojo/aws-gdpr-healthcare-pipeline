#!/bin/bash
set -e

ENVIRONMENT_NAME="${1:-gdpr-healthcare}"
REGION="${2:-eu-central-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="${SCRIPT_DIR}/../infrastructure"
KMS_STACK_NAME="${ENVIRONMENT_NAME}-kms"

echo "GDPR Healthcare Pipeline - One-Time Setup"
echo "=========================================="
echo "Environment: ${ENVIRONMENT_NAME}"
echo "Region: ${REGION}"
echo ""

# Check if KMS stack already exists
STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name "${KMS_STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [ "$STACK_STATUS" != "DOES_NOT_EXIST" ] && [ "$STACK_STATUS" != "DELETE_COMPLETE" ]; then
    echo "KMS stack already exists (status: ${STACK_STATUS})"
    echo "Skipping KMS deployment - reusing existing key."
    echo ""

    # Show existing key info
    KMS_KEY_ARN=$(aws cloudformation describe-stacks \
        --stack-name "${KMS_STACK_NAME}" \
        --region "${REGION}" \
        --query 'Stacks[0].Outputs[?OutputKey==`KmsKeyArn`].OutputValue' \
        --output text)

    echo "Existing KMS Key ARN: ${KMS_KEY_ARN}"
    echo ""
    echo "You can now run: ./scripts/deploy.sh"
    exit 0
fi

echo "Deploying KMS foundation stack..."
echo "This creates the KMS key that will be reused across deployments."
echo ""

aws cloudformation deploy \
    --template-file "${INFRA_DIR}/kms.yaml" \
    --stack-name "${KMS_STACK_NAME}" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo ""
echo "KMS stack deployed successfully!"
echo ""

# Show the created key info
KMS_KEY_ARN=$(aws cloudformation describe-stacks \
    --stack-name "${KMS_STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`KmsKeyArn`].OutputValue' \
    --output text)

echo "KMS Key ARN: ${KMS_KEY_ARN}"
echo "KMS Alias: alias/healthcare-gdpr"
echo ""
echo "Setup complete! You can now run: ./scripts/deploy.sh"
echo ""
echo "Note: This KMS key will persist across teardown/deploy cycles."
echo "      You only need to run setup.sh once per AWS account."
