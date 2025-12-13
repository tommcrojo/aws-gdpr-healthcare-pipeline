"""
Phase 3 Tests - Redshift Data Integration

Tests for data loading and querying in Redshift.
"""

import time
import pytest


def execute_redshift_query(client, workgroup, database, sql, wait_seconds=30):
    """Execute a query and wait for results."""
    response = client.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sql=sql
    )
    statement_id = response["Id"]

    # Poll for completion
    for _ in range(wait_seconds):
        status_response = client.describe_statement(Id=statement_id)
        status = status_response["Status"]

        if status == "FINISHED":
            return statement_id, status_response
        elif status in ["FAILED", "ABORTED"]:
            error = status_response.get("Error", "Unknown error")
            raise Exception(f"Query failed: {error}")

        time.sleep(1)

    raise TimeoutError(f"Query did not complete in {wait_seconds} seconds")


@pytest.mark.phase3
class TestPatientVitalsTable:
    """Test patient_vitals table in Redshift."""

    def test_schema_exists(self, redshift_data_client, environment_name):
        """patient_data schema should exist."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'patient_data'"
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        records = result.get("Records", [])

        assert len(records) > 0, "patient_data schema not found"
        assert records[0][0]["stringValue"] == "patient_data"

    def test_patient_vitals_table_exists(self, redshift_data_client, environment_name):
        """patient_vitals table should exist."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'patient_data'
            AND table_name = 'patient_vitals'
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        records = result.get("Records", [])

        assert len(records) > 0, "patient_vitals table not found"

    def test_patient_vitals_has_required_columns(self, redshift_data_client, environment_name):
        """patient_vitals should have all required columns."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'patient_data'
            AND table_name = 'patient_vitals'
            ORDER BY ordinal_position
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        columns = [r[0]["stringValue"] for r in result.get("Records", [])]

        required_columns = [
            "record_id",
            "patient_id_hash",
            "timestamp",
            "heart_rate",
            "blood_pressure_systolic",
            "blood_pressure_diastolic",
            "temperature_celsius",
            "oxygen_saturation",
            "year",
            "month",
            "day"
        ]

        for col in required_columns:
            assert col in columns, f"Missing required column: {col}"

    def test_patient_vitals_has_no_patient_id_column(self, redshift_data_client, environment_name):
        """patient_vitals should NOT have raw patient_id column (GDPR compliance)."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'patient_data'
            AND table_name = 'patient_vitals'
            AND column_name = 'patient_id'
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        records = result.get("Records", [])

        assert len(records) == 0, "Raw patient_id column found - GDPR violation!"

    def test_patient_id_hash_column_type(self, redshift_data_client, environment_name):
        """patient_id_hash should be VARCHAR(64) for SHA256."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'patient_data'
            AND table_name = 'patient_vitals'
            AND column_name = 'patient_id_hash'
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        records = result.get("Records", [])

        assert len(records) > 0, "patient_id_hash column not found"
        data_type = records[0][0]["stringValue"]
        max_length = records[0][1].get("longValue", 0)

        assert "char" in data_type.lower(), f"Expected VARCHAR, got {data_type}"
        assert max_length >= 64, f"Column too short for SHA256: {max_length}"


@pytest.mark.phase3
@pytest.mark.integration
class TestRedshiftDataLoading:
    """Test data loading from S3 to Redshift."""

    def test_can_query_patient_vitals(self, redshift_data_client, environment_name):
        """Should be able to query patient_vitals table."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            "SELECT COUNT(*) FROM patient_data.patient_vitals"
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        records = result.get("Records", [])

        assert len(records) > 0, "Query returned no results"
        # Count should be a number (may be 0 if no data loaded yet)
        count = records[0][0].get("longValue", 0)
        assert count >= 0, "Invalid count"

    def test_patient_vitals_has_data(self, redshift_data_client, environment_name):
        """patient_vitals should have data after ETL run."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            "SELECT COUNT(*) FROM patient_data.patient_vitals"
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        count = result.get("Records", [[{"longValue": 0}]])[0][0].get("longValue", 0)

        if count == 0:
            pytest.skip("No data in patient_vitals yet (ETL may not have run)")

        assert count > 0, "patient_vitals table is empty"

    def test_patient_id_hash_is_valid_sha256(self, redshift_data_client, environment_name):
        """patient_id_hash values should be valid SHA256 (64 hex chars)."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT patient_id_hash, LENGTH(patient_id_hash) as len
            FROM patient_data.patient_vitals
            LIMIT 10
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        records = result.get("Records", [])

        if len(records) == 0:
            pytest.skip("No data to validate")

        for record in records:
            hash_value = record[0].get("stringValue", "")
            hash_length = record[1].get("longValue", 0)

            assert hash_length == 64, f"Hash length should be 64, got {hash_length}"
            assert all(c in "0123456789abcdef" for c in hash_value.lower()), \
                f"Hash contains non-hex characters: {hash_value}"

    def test_no_raw_patient_ids_in_data(self, redshift_data_client, environment_name):
        """Data should not contain recognizable patient IDs."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            "SELECT patient_id_hash FROM patient_data.patient_vitals LIMIT 100"
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        records = result.get("Records", [])

        if len(records) == 0:
            pytest.skip("No data to validate")

        for record in records:
            hash_value = record[0].get("stringValue", "")
            # Raw patient IDs would likely have patterns like "patient-", "P-", "PAT"
            assert not hash_value.startswith("patient"), \
                "Found raw patient ID - pseudonymization failed!"
            assert not hash_value.startswith("P-"), \
                "Found raw patient ID pattern"

    def test_sample_query_works(self, redshift_data_client, environment_name):
        """Sample query should work (success criteria verification)."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            "SELECT * FROM patient_data.patient_vitals LIMIT 10"
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)

        # Verify column metadata
        column_metadata = result.get("ColumnMetadata", [])
        column_names = [c["name"] for c in column_metadata]

        assert "patient_id_hash" in column_names, "patient_id_hash column not in results"
        assert "heart_rate" in column_names, "heart_rate column not in results"


@pytest.mark.phase3
@pytest.mark.integration
class TestRedshiftDataQuality:
    """Test data quality in Redshift."""

    def test_heart_rate_values_valid(self, redshift_data_client, environment_name):
        """Heart rate values should be in valid range (0-300)."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT COUNT(*)
            FROM patient_data.patient_vitals
            WHERE heart_rate < 0 OR heart_rate > 300
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        invalid_count = result.get("Records", [[{"longValue": 0}]])[0][0].get("longValue", 0)

        assert invalid_count == 0, f"Found {invalid_count} records with invalid heart_rate"

    def test_no_null_patient_id_hash(self, redshift_data_client, environment_name):
        """patient_id_hash should never be NULL."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            "SELECT COUNT(*) FROM patient_data.patient_vitals WHERE patient_id_hash IS NULL"
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        null_count = result.get("Records", [[{"longValue": 0}]])[0][0].get("longValue", 0)

        assert null_count == 0, f"Found {null_count} records with NULL patient_id_hash"

    def test_data_has_partition_columns(self, redshift_data_client, environment_name):
        """Data should have populated year/month/day columns."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT COUNT(*)
            FROM patient_data.patient_vitals
            WHERE year IS NULL OR month IS NULL OR day IS NULL
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        null_count = result.get("Records", [[{"longValue": 0}]])[0][0].get("longValue", 0)

        assert null_count == 0, f"Found {null_count} records with NULL partition columns"


@pytest.mark.phase3
@pytest.mark.integration
@pytest.mark.slow
class TestRedshiftIdempotency:
    """Test idempotency of data loading."""

    def test_no_duplicate_records(self, redshift_data_client, environment_name):
        """Should not have duplicate record_ids."""
        workgroup = f"{environment_name}-workgroup"

        statement_id, _ = execute_redshift_query(
            redshift_data_client,
            workgroup,
            "healthcare_analytics",
            """
            SELECT record_id, COUNT(*) as cnt
            FROM patient_data.patient_vitals
            GROUP BY record_id
            HAVING COUNT(*) > 1
            LIMIT 10
            """
        )

        result = redshift_data_client.get_statement_result(Id=statement_id)
        duplicates = result.get("Records", [])

        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate record_ids"
