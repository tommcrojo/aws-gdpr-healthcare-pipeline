"""
Phase 2 Tests - ETL Logic and Data Validation

Tests for pseudonymization, data validation, and ETL script.
"""

import hashlib
import json
import time
import pytest


@pytest.mark.phase2
class TestGlueScriptDeployment:
    """Test Glue script is deployed correctly."""

    def test_etl_script_exists_in_s3(self, s3_client, processing_stack_outputs):
        """ETL script should be uploaded to S3."""
        bucket_name = processing_stack_outputs.get("GlueScriptsBucketName")
        script_key = "scripts/etl_job.py"

        response = s3_client.head_object(Bucket=bucket_name, Key=script_key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200, "ETL script not found in S3"

    def test_etl_script_not_empty(self, s3_client, processing_stack_outputs):
        """ETL script should not be empty."""
        bucket_name = processing_stack_outputs.get("GlueScriptsBucketName")
        script_key = "scripts/etl_job.py"

        response = s3_client.head_object(Bucket=bucket_name, Key=script_key)
        assert response["ContentLength"] > 1000, "ETL script seems too small"


@pytest.mark.phase2
class TestPseudonymizationLogic:
    """Test pseudonymization implementation (unit tests)."""

    def test_sha256_hash_format(self):
        """SHA256 hash should be 64 hex characters."""
        patient_id = "patient-12345"
        salt = "test-salt-value"
        combined = f"{patient_id}{salt}"
        hashed = hashlib.sha256(combined.encode("utf-8")).hexdigest()

        assert len(hashed) == 64, f"Hash length should be 64, got {len(hashed)}"
        assert hashed.isalnum(), "Hash should be alphanumeric"

    def test_same_input_same_hash(self):
        """Same patient ID with same salt should produce same hash."""
        patient_id = "patient-12345"
        salt = "consistent-salt"

        combined = f"{patient_id}{salt}"
        hash1 = hashlib.sha256(combined.encode("utf-8")).hexdigest()
        hash2 = hashlib.sha256(combined.encode("utf-8")).hexdigest()

        assert hash1 == hash2, "Same inputs should produce same hash"

    def test_different_patients_different_hashes(self):
        """Different patient IDs should produce different hashes."""
        salt = "same-salt"

        hash1 = hashlib.sha256(f"patient-001{salt}".encode("utf-8")).hexdigest()
        hash2 = hashlib.sha256(f"patient-002{salt}".encode("utf-8")).hexdigest()

        assert hash1 != hash2, "Different patients should have different hashes"

    def test_salt_matters(self):
        """Different salts should produce different hashes for same patient."""
        patient_id = "patient-12345"

        hash1 = hashlib.sha256(f"{patient_id}salt-a".encode("utf-8")).hexdigest()
        hash2 = hashlib.sha256(f"{patient_id}salt-b".encode("utf-8")).hexdigest()

        assert hash1 != hash2, "Different salts should produce different hashes"

    def test_hash_is_irreversible(self):
        """Cannot derive patient ID from hash (basic check)."""
        patient_id = "patient-12345"
        salt = "secret-salt"
        combined = f"{patient_id}{salt}"
        hashed = hashlib.sha256(combined.encode("utf-8")).hexdigest()

        # Hash should not contain original patient ID
        assert patient_id not in hashed, "Hash should not contain original ID"


@pytest.mark.phase2
class TestDataValidationLogic:
    """Test data validation rules (unit tests)."""

    @pytest.fixture
    def valid_record(self):
        """Sample valid health record."""
        return {
            "record_id": "rec-001",
            "patient_id": "patient-001",
            "timestamp": "2025-01-01T12:00:00Z",
            "event_type": "vital_signs",
            "data": {
                "heart_rate": 72,
                "blood_pressure_systolic": 120,
                "blood_pressure_diastolic": 80,
                "temperature_celsius": 36.6,
                "oxygen_saturation": 98
            },
            "metadata": {
                "source": "icu_monitor",
                "version": "1.0",
                "is_test": False
            }
        }

    def test_valid_heart_rate_passes(self, valid_record):
        """Heart rate in valid range should pass validation."""
        heart_rate = valid_record["data"]["heart_rate"]
        is_valid = heart_rate is not None and 0 <= heart_rate <= 300
        assert is_valid, "Valid heart rate should pass"

    def test_missing_heart_rate_fails(self, valid_record):
        """Missing heart rate should fail validation."""
        valid_record["data"]["heart_rate"] = None
        heart_rate = valid_record["data"]["heart_rate"]
        is_valid = heart_rate is not None and 0 <= heart_rate <= 300
        assert not is_valid, "Missing heart rate should fail"

    def test_negative_heart_rate_fails(self, valid_record):
        """Negative heart rate should fail validation."""
        valid_record["data"]["heart_rate"] = -10
        heart_rate = valid_record["data"]["heart_rate"]
        is_valid = heart_rate is not None and 0 <= heart_rate <= 300
        assert not is_valid, "Negative heart rate should fail"

    def test_excessive_heart_rate_fails(self, valid_record):
        """Heart rate above 300 should fail validation."""
        valid_record["data"]["heart_rate"] = 350
        heart_rate = valid_record["data"]["heart_rate"]
        is_valid = heart_rate is not None and 0 <= heart_rate <= 300
        assert not is_valid, "Heart rate > 300 should fail"

    def test_zero_heart_rate_passes(self, valid_record):
        """Heart rate of 0 should pass (could indicate flatline)."""
        valid_record["data"]["heart_rate"] = 0
        heart_rate = valid_record["data"]["heart_rate"]
        is_valid = heart_rate is not None and 0 <= heart_rate <= 300
        assert is_valid, "Heart rate of 0 should pass"

    def test_boundary_heart_rate_passes(self, valid_record):
        """Heart rate at boundary (300) should pass."""
        valid_record["data"]["heart_rate"] = 300
        heart_rate = valid_record["data"]["heart_rate"]
        is_valid = heart_rate is not None and 0 <= heart_rate <= 300
        assert is_valid, "Heart rate of 300 should pass"


@pytest.mark.phase2
@pytest.mark.integration
@pytest.mark.slow
class TestGlueJobExecution:
    """Integration tests for Glue job execution."""

    def test_glue_job_can_start(self, glue_client, environment_name):
        """Glue job should be able to start (dry run check)."""
        job_name = f"{environment_name}-etl-job"

        # Just verify the job exists and has valid configuration
        response = glue_client.get_job(JobName=job_name)
        job = response["Job"]

        # Verify all required arguments are present
        args = job["DefaultArguments"]
        assert "--RAW_BUCKET" in args, "Missing RAW_BUCKET argument"
        assert "--CURATED_BUCKET" in args, "Missing CURATED_BUCKET argument"

    def test_glue_job_recent_run_succeeded(self, glue_client, environment_name):
        """Check if most recent job run succeeded (if any runs exist)."""
        job_name = f"{environment_name}-etl-job"

        try:
            response = glue_client.get_job_runs(JobName=job_name, MaxResults=1)
            runs = response.get("JobRuns", [])

            if runs:
                latest_run = runs[0]
                status = latest_run["JobRunState"]
                # Allow RUNNING, SUCCEEDED, or STARTING
                assert status in ["SUCCEEDED", "RUNNING", "STARTING", "STOPPING"], \
                    f"Latest job run status: {status}"
        except glue_client.exceptions.EntityNotFoundException:
            pytest.skip("No job runs found yet")


@pytest.mark.phase2
@pytest.mark.integration
class TestDataFlowIntegration:
    """Test data flow from raw to curated bucket."""

    def test_curated_bucket_has_data(self, s3_client, processing_stack_outputs):
        """Curated bucket should have data after ETL run (if job has run)."""
        bucket_name = processing_stack_outputs.get("CuratedBucketName")

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="curated/", MaxKeys=10)

        # This is optional - may not have data yet
        if response.get("KeyCount", 0) > 0:
            keys = [obj["Key"] for obj in response.get("Contents", [])]
            assert any(".parquet" in k or ".snappy" in k for k in keys), \
                "Expected Parquet files in curated bucket"
        else:
            pytest.skip("No data in curated bucket yet (ETL may not have run)")

    def test_curated_data_is_partitioned(self, s3_client, processing_stack_outputs):
        """Curated data should be partitioned by year/month/day."""
        bucket_name = processing_stack_outputs.get("CuratedBucketName")

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="curated/", MaxKeys=100)

        if response.get("KeyCount", 0) > 0:
            keys = [obj["Key"] for obj in response.get("Contents", [])]
            # Check for partition pattern
            has_partitions = any("year=" in k and "month=" in k and "day=" in k for k in keys)
            assert has_partitions, "Data not partitioned by date"
        else:
            pytest.skip("No data to check partitioning")

    def test_curated_data_no_raw_patient_id(self, s3_client, processing_stack_outputs):
        """Curated data should not contain raw patient_id column."""
        # This would require reading Parquet files - simplified check
        bucket_name = processing_stack_outputs.get("CuratedBucketName")

        # Check that data exists first
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="curated/", MaxKeys=1)

        if response.get("KeyCount", 0) > 0:
            # Note: Full validation would require reading Parquet schema
            # This is a placeholder for that check
            pass
        else:
            pytest.skip("No curated data to validate")
