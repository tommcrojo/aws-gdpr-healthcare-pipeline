"""
Phase 1 Tests - Security Configuration

Tests for IAM roles and Secrets Manager.
"""

import json
import pytest
from tests.conftest import get_stack_status


@pytest.mark.phase1
class TestSecurityStack:
    """Test security CloudFormation stack deployment."""

    def test_security_stack_exists(self, cloudformation_client, environment_name):
        """Security stack should be deployed."""
        status = get_stack_status(cloudformation_client, f"{environment_name}-security")
        assert status is not None, "Security stack not found"
        assert status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"], f"Security stack status: {status}"

    def test_security_stack_has_required_outputs(self, security_stack_outputs):
        """Security stack should export role ARNs and secret ARN."""
        required = ["FirehoseDeliveryRoleArn", "HashingSaltSecretArn"]
        for output in required:
            assert output in security_stack_outputs, f"Missing {output} output"


@pytest.mark.phase1
class TestFirehoseRole:
    """Test Firehose delivery role configuration."""

    def test_firehose_role_exists(self, iam_client, environment_name):
        """Firehose delivery role should exist."""
        role_name = f"{environment_name}-firehose-role"
        try:
            response = iam_client.get_role(RoleName=role_name)
            assert response["Role"]["RoleName"] == role_name
        except iam_client.exceptions.NoSuchEntityException:
            pytest.fail(f"Firehose role {role_name} not found")

    def test_firehose_role_trust_policy(self, iam_client, environment_name):
        """Firehose role should trust firehose.amazonaws.com."""
        role_name = f"{environment_name}-firehose-role"
        response = iam_client.get_role(RoleName=role_name)

        trust_policy = response["Role"]["AssumeRolePolicyDocument"]
        principals = []
        for statement in trust_policy.get("Statement", []):
            principal = statement.get("Principal", {})
            if isinstance(principal.get("Service"), str):
                principals.append(principal["Service"])
            elif isinstance(principal.get("Service"), list):
                principals.extend(principal["Service"])

        assert "firehose.amazonaws.com" in principals, "Firehose service not in trust policy"

    def test_firehose_role_has_s3_permissions(self, iam_client, environment_name):
        """Firehose role should have S3 permissions."""
        role_name = f"{environment_name}-firehose-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        # Check inline policies for S3 actions
        has_s3 = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "s3:" in policy_str:
                has_s3 = True
                break

        assert has_s3, "Firehose role missing S3 permissions"

    def test_firehose_role_has_kms_permissions(self, iam_client, environment_name):
        """Firehose role should have KMS permissions."""
        role_name = f"{environment_name}-firehose-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        has_kms = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "kms:" in policy_str:
                has_kms = True
                break

        assert has_kms, "Firehose role missing KMS permissions"


@pytest.mark.phase1
class TestHashingSalt:
    """Test hashing salt secret configuration."""

    def test_hashing_salt_secret_exists(self, secretsmanager_client, security_stack_outputs):
        """Hashing salt secret should exist."""
        secret_arn = security_stack_outputs.get("HashingSaltSecretArn")
        assert secret_arn, "Secret ARN not found in outputs"

        response = secretsmanager_client.describe_secret(SecretId=secret_arn)
        assert response["Name"], "Secret not found"

    def test_hashing_salt_is_kms_encrypted(self, secretsmanager_client, security_stack_outputs):
        """Hashing salt should be encrypted with KMS (not default key)."""
        secret_arn = security_stack_outputs.get("HashingSaltSecretArn")
        response = secretsmanager_client.describe_secret(SecretId=secret_arn)

        kms_key = response.get("KmsKeyId")
        assert kms_key, "Secret not encrypted with KMS"
        # Should not be the default AWS managed key
        assert "alias/aws/secretsmanager" not in kms_key, "Using default KMS key instead of CMK"

    def test_hashing_salt_value_exists(self, secretsmanager_client, security_stack_outputs):
        """Hashing salt should have a value with 'salt' key."""
        secret_arn = security_stack_outputs.get("HashingSaltSecretArn")
        response = secretsmanager_client.get_secret_value(SecretId=secret_arn)

        secret_string = response.get("SecretString")
        assert secret_string, "Secret has no value"

        secret_dict = json.loads(secret_string)
        assert "salt" in secret_dict, "Secret missing 'salt' key"
        assert len(secret_dict["salt"]) >= 32, "Salt is too short (should be >= 32 chars)"

    def test_hashing_salt_has_gdpr_tag(self, secretsmanager_client, security_stack_outputs):
        """Secret should be tagged for GDPR compliance."""
        secret_arn = security_stack_outputs.get("HashingSaltSecretArn")
        response = secretsmanager_client.describe_secret(SecretId=secret_arn)

        tags = {t["Key"]: t["Value"] for t in response.get("Tags", [])}
        assert "Purpose" in tags, "Missing Purpose tag"
        assert "GDPR" in tags.get("Purpose", "").upper() or "Pseudonymization" in tags.get("Purpose", ""), \
            "Purpose tag should reference GDPR/Pseudonymization"
