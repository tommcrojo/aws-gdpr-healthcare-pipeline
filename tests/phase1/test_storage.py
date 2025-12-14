"""
Phase 1 Tests - Storage and Ingestion Configuration

Tests for S3 buckets and Kinesis Firehose.
"""

import pytest
from tests.conftest import get_stack_status


@pytest.mark.phase1
class TestStorageStack:
    """Test storage-ingestion CloudFormation stack deployment."""

    def test_storage_stack_exists(self, cloudformation_client, environment_name):
        """Storage-ingestion stack should be deployed."""
        status = get_stack_status(cloudformation_client, f"{environment_name}-storage-ingestion")
        assert status is not None, "Storage stack not found"
        assert status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"], f"Storage stack status: {status}"

    def test_storage_stack_has_required_outputs(self, storage_stack_outputs):
        """Storage stack should export bucket names and Firehose ARN."""
        required = ["RawBucketName", "RawBucketArn", "DeliveryStreamArn"]
        for output in required:
            assert output in storage_stack_outputs, f"Missing {output} output"


@pytest.mark.phase1
class TestRawDataBucket:
    """Test raw data S3 bucket configuration."""

    def test_raw_bucket_exists(self, s3_client, storage_stack_outputs):
        """Raw data bucket should exist."""
        bucket_name = storage_stack_outputs.get("RawBucketName")
        assert bucket_name, "Bucket name not found"

        response = s3_client.head_bucket(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_raw_bucket_kms_encryption(self, s3_client, storage_stack_outputs):
        """Raw bucket should use KMS encryption."""
        bucket_name = storage_stack_outputs.get("RawBucketName")
        response = s3_client.get_bucket_encryption(Bucket=bucket_name)

        rules = response["ServerSideEncryptionConfiguration"]["Rules"]
        assert len(rules) > 0, "No encryption rules found"

        sse_algo = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
        assert sse_algo == "aws:kms", f"Expected aws:kms, got {sse_algo}"

    def test_raw_bucket_versioning_enabled(self, s3_client, storage_stack_outputs):
        """Raw bucket should have versioning enabled."""
        bucket_name = storage_stack_outputs.get("RawBucketName")
        response = s3_client.get_bucket_versioning(Bucket=bucket_name)
        assert response.get("Status") == "Enabled", "Versioning not enabled"

    def test_raw_bucket_blocks_public_access(self, s3_client, storage_stack_outputs):
        """Raw bucket should block all public access."""
        bucket_name = storage_stack_outputs.get("RawBucketName")
        response = s3_client.get_public_access_block(Bucket=bucket_name)

        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"], "BlockPublicAcls not enabled"
        assert config["BlockPublicPolicy"], "BlockPublicPolicy not enabled"
        assert config["IgnorePublicAcls"], "IgnorePublicAcls not enabled"
        assert config["RestrictPublicBuckets"], "RestrictPublicBuckets not enabled"

    def test_raw_bucket_denies_non_tls(self, s3_client, storage_stack_outputs):
        """Raw bucket policy should deny non-TLS access."""
        bucket_name = storage_stack_outputs.get("RawBucketName")
        try:
            response = s3_client.get_bucket_policy(Bucket=bucket_name)
            policy = response["Policy"]
            assert "aws:SecureTransport" in policy, "No TLS enforcement in bucket policy"
        except s3_client.exceptions.ClientError as e:
            if "NoSuchBucketPolicy" in str(e):
                pytest.fail("No bucket policy found - TLS should be enforced")
            raise


@pytest.mark.phase1
class TestFirehoseDeliveryStream:
    """Test Kinesis Firehose configuration."""

    def test_firehose_stream_exists(self, firehose_client, environment_name):
        """Firehose delivery stream should exist."""
        stream_name = f"{environment_name}-delivery-stream"
        response = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)

        assert response["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE", \
            "Firehose stream is not active"

    def test_firehose_destination_is_s3(self, firehose_client, environment_name):
        """Firehose should deliver to S3."""
        stream_name = f"{environment_name}-delivery-stream"
        response = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)

        destinations = response["DeliveryStreamDescription"]["Destinations"]
        assert len(destinations) > 0, "No destinations configured"

        # Check for S3 destination
        has_s3 = any(
            "S3DestinationDescription" in dest or "ExtendedS3DestinationDescription" in dest
            for dest in destinations
        )
        assert has_s3, "No S3 destination found"

    def test_firehose_uses_kms_encryption(self, firehose_client, environment_name, kms_stack_outputs):
        """Firehose should use KMS encryption for S3 delivery."""
        stream_name = f"{environment_name}-delivery-stream"
        response = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)

        destinations = response["DeliveryStreamDescription"]["Destinations"]
        kms_key_arn = kms_stack_outputs.get("KmsKeyArn")

        for dest in destinations:
            s3_config = dest.get("ExtendedS3DestinationDescription", {})
            encryption_config = s3_config.get("EncryptionConfiguration", {})

            if "KMSEncryptionConfig" in encryption_config:
                key_arn = encryption_config["KMSEncryptionConfig"].get("AWSKMSKeyARN")
                assert key_arn == kms_key_arn, f"Firehose using wrong KMS key: {key_arn}"
                return

        pytest.fail("KMS encryption not configured for Firehose S3 destination")

    def test_firehose_partitions_by_date(self, firehose_client, environment_name):
        """Firehose should partition data by date."""
        stream_name = f"{environment_name}-delivery-stream"
        response = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)

        destinations = response["DeliveryStreamDescription"]["Destinations"]

        for dest in destinations:
            s3_config = dest.get("ExtendedS3DestinationDescription", {})
            prefix = s3_config.get("Prefix", "")

            # Check for date partitioning pattern
            has_date_partition = (
                "year=" in prefix.lower() or
                "!{timestamp:" in prefix or
                "{timestamp:" in prefix
            )
            if has_date_partition:
                return

        pytest.fail("No date partitioning found in Firehose prefix")


@pytest.mark.phase1
@pytest.mark.integration
class TestFirehoseIntegration:
    """Integration tests for Firehose data delivery."""

    @pytest.mark.slow
    def test_firehose_can_put_record(self, firehose_client, environment_name):
        """Firehose should accept test records."""
        import json
        stream_name = f"{environment_name}-delivery-stream"

        test_record = {
            "record_id": "test-record-001",
            "patient_id": "test-patient",
            "timestamp": "2025-01-01T00:00:00Z",
            "event_type": "vital_signs",
            "data": {
                "heart_rate": 72,
                "blood_pressure_systolic": 120,
                "blood_pressure_diastolic": 80,
                "temperature_celsius": 36.6,
                "oxygen_saturation": 98
            },
            "metadata": {
                "source": "pytest",
                "version": "1.0",
                "is_test": True
            }
        }

        response = firehose_client.put_record(
            DeliveryStreamName=stream_name,
            Record={"Data": json.dumps(test_record).encode("utf-8")}
        )

        assert response["RecordId"], "No RecordId returned"
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
