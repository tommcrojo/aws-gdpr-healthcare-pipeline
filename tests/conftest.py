"""
GDPR Healthcare Pipeline - Test Configuration

Shared fixtures and configuration for all test phases.
Run specific phases with: pytest -m phase1, pytest -m phase2, pytest -m phase3, pytest -m phase4
Run all tests with: pytest
"""

import os
import pytest
import boto3
from botocore.config import Config

# Default configuration
DEFAULT_ENVIRONMENT = "gdpr-healthcare"
DEFAULT_REGION = "eu-central-1"


def get_env_name():
    """Get environment name from env var or default."""
    return os.environ.get("ENVIRONMENT_NAME", DEFAULT_ENVIRONMENT)


def get_region():
    """Get AWS region from env var or default."""
    return os.environ.get("AWS_REGION", DEFAULT_REGION)


@pytest.fixture(scope="session")
def environment_name():
    """Environment name prefix for all resources."""
    return get_env_name()


@pytest.fixture(scope="session")
def aws_region():
    """AWS region for deployment."""
    return get_region()


@pytest.fixture(scope="session")
def boto_config():
    """Boto3 client configuration with retries."""
    return Config(
        region_name=get_region(),
        retries={"max_attempts": 3, "mode": "adaptive"}
    )


@pytest.fixture(scope="session")
def cloudformation_client(boto_config):
    """CloudFormation client."""
    return boto3.client("cloudformation", config=boto_config)


@pytest.fixture(scope="session")
def s3_client(boto_config):
    """S3 client."""
    return boto3.client("s3", config=boto_config)


@pytest.fixture(scope="session")
def ec2_client(boto_config):
    """EC2 client."""
    return boto3.client("ec2", config=boto_config)


@pytest.fixture(scope="session")
def kms_client(boto_config):
    """KMS client."""
    return boto3.client("kms", config=boto_config)


@pytest.fixture(scope="session")
def secretsmanager_client(boto_config):
    """Secrets Manager client."""
    return boto3.client("secretsmanager", config=boto_config)


@pytest.fixture(scope="session")
def firehose_client(boto_config):
    """Kinesis Firehose client."""
    return boto3.client("firehose", config=boto_config)


@pytest.fixture(scope="session")
def glue_client(boto_config):
    """Glue client."""
    return boto3.client("glue", config=boto_config)


@pytest.fixture(scope="session")
def redshift_serverless_client(boto_config):
    """Redshift Serverless client."""
    return boto3.client("redshift-serverless", config=boto_config)


@pytest.fixture(scope="session")
def redshift_data_client(boto_config):
    """Redshift Data API client."""
    return boto3.client("redshift-data", config=boto_config)


@pytest.fixture(scope="session")
def iam_client(boto_config):
    """IAM client."""
    return boto3.client("iam", config=boto_config)


@pytest.fixture(scope="session")
def dynamodb_client(boto_config):
    """DynamoDB client."""
    return boto3.client("dynamodb", config=boto_config)


@pytest.fixture(scope="session")
def lambda_client(boto_config):
    """Lambda client."""
    return boto3.client("lambda", config=boto_config)


@pytest.fixture(scope="session")
def athena_client(boto_config):
    """Athena client."""
    return boto3.client("athena", config=boto_config)


@pytest.fixture(scope="session")
def logs_client(boto_config):
    """CloudWatch Logs client."""
    return boto3.client("logs", config=boto_config)


@pytest.fixture(scope="session")
def cloudwatch_client(boto_config):
    """CloudWatch client."""
    return boto3.client("cloudwatch", config=boto_config)


# Stack output helpers
def get_stack_outputs(cf_client, stack_name):
    """Get outputs from a CloudFormation stack as a dictionary."""
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
        outputs = response["Stacks"][0].get("Outputs", [])
        return {o["OutputKey"]: o["OutputValue"] for o in outputs}
    except cf_client.exceptions.ClientError:
        return {}


def get_stack_status(cf_client, stack_name):
    """Get status of a CloudFormation stack."""
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
        return response["Stacks"][0]["StackStatus"]
    except cf_client.exceptions.ClientError:
        return None


@pytest.fixture(scope="session")
def kms_stack_outputs(cloudformation_client, environment_name):
    """Outputs from KMS stack."""
    return get_stack_outputs(cloudformation_client, f"{environment_name}-kms")


@pytest.fixture(scope="session")
def networking_stack_outputs(cloudformation_client, environment_name):
    """Outputs from networking stack."""
    return get_stack_outputs(cloudformation_client, f"{environment_name}-networking")


@pytest.fixture(scope="session")
def security_stack_outputs(cloudformation_client, environment_name):
    """Outputs from security stack."""
    return get_stack_outputs(cloudformation_client, f"{environment_name}-security")


@pytest.fixture(scope="session")
def storage_stack_outputs(cloudformation_client, environment_name):
    """Outputs from storage-ingestion stack."""
    return get_stack_outputs(cloudformation_client, f"{environment_name}-storage-ingestion")


@pytest.fixture(scope="session")
def processing_stack_outputs(cloudformation_client, environment_name):
    """Outputs from processing stack."""
    return get_stack_outputs(cloudformation_client, f"{environment_name}-processing")


@pytest.fixture(scope="session")
def redshift_stack_outputs(cloudformation_client, environment_name):
    """Outputs from redshift stack."""
    return get_stack_outputs(cloudformation_client, f"{environment_name}-redshift")


@pytest.fixture(scope="session")
def compliance_stack_outputs(cloudformation_client, environment_name):
    """Outputs from compliance stack."""
    return get_stack_outputs(cloudformation_client, f"{environment_name}-compliance")


# Pytest markers for phase selection
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "phase1: Phase 1 - Infrastructure tests")
    config.addinivalue_line("markers", "phase2: Phase 2 - Processing tests")
    config.addinivalue_line("markers", "phase3: Phase 3 - Redshift integration tests")
    config.addinivalue_line("markers", "phase4: Phase 4 - GDPR compliance tests")
    config.addinivalue_line("markers", "integration: Integration tests requiring deployed infrastructure")
    config.addinivalue_line("markers", "slow: Slow-running tests")
