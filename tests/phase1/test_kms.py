"""
Phase 1 Tests - KMS Key Configuration

Tests for customer-managed KMS key used for data encryption.
"""

import pytest
from tests.conftest import get_stack_status


@pytest.mark.phase1
class TestKMSStack:
    """Test KMS CloudFormation stack deployment."""

    def test_kms_stack_exists(self, cloudformation_client, environment_name):
        """KMS stack should be deployed."""
        status = get_stack_status(cloudformation_client, f"{environment_name}-kms")
        assert status is not None, "KMS stack not found"
        assert status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"], f"KMS stack status: {status}"

    def test_kms_stack_has_required_outputs(self, kms_stack_outputs):
        """KMS stack should export key ARN and ID."""
        assert "KmsKeyArn" in kms_stack_outputs, "Missing KmsKeyArn output"
        assert "KmsKeyId" in kms_stack_outputs, "Missing KmsKeyId output"

    def test_kms_key_arn_format(self, kms_stack_outputs, aws_region):
        """KMS key ARN should have correct format."""
        arn = kms_stack_outputs.get("KmsKeyArn", "")
        assert arn.startswith(f"arn:aws:kms:{aws_region}:"), f"Invalid KMS ARN format: {arn}"


@pytest.mark.phase1
class TestKMSKeyConfiguration:
    """Test KMS key properties and configuration."""

    def test_kms_key_exists(self, kms_client, kms_stack_outputs):
        """KMS key should exist and be accessible."""
        key_id = kms_stack_outputs.get("KmsKeyId")
        assert key_id, "KMS key ID not found in stack outputs"

        response = kms_client.describe_key(KeyId=key_id)
        assert response["KeyMetadata"]["KeyState"] == "Enabled", "KMS key is not enabled"

    def test_kms_key_rotation_enabled(self, kms_client, kms_stack_outputs):
        """KMS key should have automatic rotation enabled."""
        key_id = kms_stack_outputs.get("KmsKeyId")
        response = kms_client.get_key_rotation_status(KeyId=key_id)
        assert response["KeyRotationEnabled"], "Key rotation is not enabled"

    def test_kms_key_is_symmetric(self, kms_client, kms_stack_outputs):
        """KMS key should be symmetric for encryption."""
        key_id = kms_stack_outputs.get("KmsKeyId")
        response = kms_client.describe_key(KeyId=key_id)
        assert response["KeyMetadata"]["KeySpec"] == "SYMMETRIC_DEFAULT", "Key is not symmetric"

    def test_kms_alias_exists(self, kms_client):
        """KMS key alias should exist."""
        response = kms_client.list_aliases()
        aliases = [a["AliasName"] for a in response["Aliases"]]
        assert "alias/healthcare-gdpr" in aliases, "KMS alias not found"


@pytest.mark.phase1
class TestKMSKeyPolicy:
    """Test KMS key policy for service principals."""

    def test_kms_policy_allows_firehose(self, kms_client, kms_stack_outputs):
        """KMS policy should allow Firehose service."""
        key_id = kms_stack_outputs.get("KmsKeyId")
        response = kms_client.get_key_policy(KeyId=key_id, PolicyName="default")
        policy = response["Policy"]
        assert "firehose.amazonaws.com" in policy, "Firehose not in KMS policy"

    def test_kms_policy_allows_glue(self, kms_client, kms_stack_outputs):
        """KMS policy should allow Glue service."""
        key_id = kms_stack_outputs.get("KmsKeyId")
        response = kms_client.get_key_policy(KeyId=key_id, PolicyName="default")
        policy = response["Policy"]
        assert "glue.amazonaws.com" in policy, "Glue not in KMS policy"

    def test_kms_policy_allows_redshift(self, kms_client, kms_stack_outputs):
        """KMS policy should allow Redshift service (Phase 3 requirement)."""
        key_id = kms_stack_outputs.get("KmsKeyId")
        response = kms_client.get_key_policy(KeyId=key_id, PolicyName="default")
        policy = response["Policy"]
        assert "redshift.amazonaws.com" in policy, "Redshift not in KMS policy"

    def test_kms_policy_has_region_condition(self, kms_client, kms_stack_outputs, aws_region):
        """KMS policy should use region condition (not IP-based)."""
        key_id = kms_stack_outputs.get("KmsKeyId")
        response = kms_client.get_key_policy(KeyId=key_id, PolicyName="default")
        policy = response["Policy"]
        assert "aws:RequestedRegion" in policy, "Region condition not found in policy"
        assert aws_region in policy, f"Region {aws_region} not in policy"
        # Ensure no IP-based conditions (per GDPR requirements)
        assert "aws:SourceIp" not in policy, "IP-based condition found (not allowed)"
