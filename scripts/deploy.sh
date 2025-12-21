#!/bin/bash
set -e

ENVIRONMENT_NAME="${1:-gdpr-healthcare}"
REGION="${2:-eu-central-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="${SCRIPT_DIR}/../infrastructure"

echo "============================================"
echo "GDPR Healthcare Pipeline - Deployment"
echo "============================================"
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

# Update KMS stack with service permissions (must be first for service validation)
echo "[1/7] Updating KMS stack with service permissions..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/kms.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-kms" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[1/7] KMS stack updated successfully."

# Deploy networking stack (includes VPC endpoints for all services)
echo "[2/7] Deploying networking stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/networking.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-networking" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[2/7] Networking stack deployed successfully."

# Deploy security stack
echo "[3/7] Deploying security stack..."
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

echo "[3/7] Security stack deployed successfully."

# Deploy storage and ingestion stack
echo "[4/7] Deploying storage and ingestion stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/storage-ingestion.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[4/7] Storage and ingestion stack deployed successfully."

# Deploy Redshift stack (must be before processing for exports)
echo "[5/7] Deploying Redshift stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/redshift.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-redshift" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[5/7] Redshift stack deployed successfully."

# Wait for Redshift workgroup to be available
REDSHIFT_WORKGROUP="${ENVIRONMENT_NAME}-workgroup"
echo "Waiting for Redshift workgroup to be available..."
aws redshift-serverless wait workgroup-available \
    --workgroup-name "${REDSHIFT_WORKGROUP}" \
    --region "${REGION}" 2>/dev/null || echo "Workgroup wait completed (or already available)"

# Create patient_vitals table in Redshift
echo "Creating patient_vitals table in Redshift..."
aws redshift-data execute-statement \
    --workgroup-name "${REDSHIFT_WORKGROUP}" \
    --database healthcare_analytics \
    --sql "$(cat ${SCRIPT_DIR}/sql/create_patient_vitals.sql)" \
    --region "${REGION}" || echo "Table creation submitted (check Redshift console for status)"

# Deploy processing stack (depends on Redshift exports)
echo "[6/7] Deploying processing stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/processing.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-processing" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[6/7] Processing stack deployed successfully."

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

# Package and upload erasure handler Lambda
echo "Packaging erasure handler Lambda..."
COMPLIANCE_DIR="${SCRIPT_DIR}/../src/compliance"
python3 -c "
import zipfile
import os
src_dir = '${COMPLIANCE_DIR}'
with zipfile.ZipFile('${COMPLIANCE_DIR}/erasure_handler.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.write(os.path.join(src_dir, 'erasure_handler.py'), 'erasure_handler.py')
"
aws s3 cp "${COMPLIANCE_DIR}/erasure_handler.zip" \
    "s3://${GLUE_SCRIPTS_BUCKET}/compliance/erasure_handler.zip" \
    --region "${REGION}"
rm "${COMPLIANCE_DIR}/erasure_handler.zip"
echo "Erasure handler uploaded to s3://${GLUE_SCRIPTS_BUCKET}/compliance/"

# Deploy compliance stack (GDPR Article 17 erasure)
echo "[7/7] Deploying compliance stack..."
aws cloudformation deploy \
    --template-file "${INFRA_DIR}/compliance.yaml" \
    --stack-name "${ENVIRONMENT_NAME}-compliance" \
    --parameter-overrides EnvironmentName="${ENVIRONMENT_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

echo "[7/7] Compliance stack deployed successfully."

echo ""
echo "============================================"
echo "All stacks deployed successfully!"
echo "============================================"
echo ""
echo "Stack outputs:"
aws cloudformation describe-stacks \
    --stack-name "${ENVIRONMENT_NAME}-storage-ingestion" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs' \
    --output table
