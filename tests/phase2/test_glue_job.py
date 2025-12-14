"""
Phase 2 Tests - Glue Job Configuration

Tests for Glue ETL job setup and IAM permissions.
"""

import json
import pytest
from tests.conftest import get_stack_status


@pytest.mark.phase2
class TestProcessingStack:
    """Test processing CloudFormation stack deployment."""

    def test_processing_stack_exists(self, cloudformation_client, environment_name):
        """Processing stack should be deployed."""
        status = get_stack_status(cloudformation_client, f"{environment_name}-processing")
        assert status is not None, "Processing stack not found"
        assert status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"], f"Processing stack status: {status}"

    def test_processing_stack_has_required_outputs(self, processing_stack_outputs):
        """Processing stack should export job and bucket names."""
        required = [
            "GlueJobName",
            "GlueDatabaseName",
            "QuarantineBucketName",
            "GlueScriptsBucketName"
        ]
        for output in required:
            assert output in processing_stack_outputs, f"Missing {output} output"

    def test_curated_bucket_in_storage_stack(self, storage_stack_outputs):
        """CuratedBucketName should be in storage-ingestion stack."""
        assert "CuratedBucketName" in storage_stack_outputs, \
            "CuratedBucketName should be exported from storage-ingestion stack"


@pytest.mark.phase2
class TestGlueJob:
    """Test Glue ETL job configuration."""

    def test_glue_job_exists(self, glue_client, environment_name):
        """Glue ETL job should exist."""
        job_name = f"{environment_name}-etl-job"
        response = glue_client.get_job(JobName=job_name)
        assert response["Job"]["Name"] == job_name

    def test_glue_job_version(self, glue_client, environment_name):
        """Glue job should use version 4.0."""
        job_name = f"{environment_name}-etl-job"
        response = glue_client.get_job(JobName=job_name)
        assert response["Job"]["GlueVersion"] == "4.0", "Job should use Glue 4.0"

    def test_glue_job_worker_config(self, glue_client, environment_name):
        """Glue job should have correct worker configuration."""
        job_name = f"{environment_name}-etl-job"
        response = glue_client.get_job(JobName=job_name)

        job = response["Job"]
        assert job["WorkerType"] == "G.1X", "Expected G.1X worker type"
        assert job["NumberOfWorkers"] >= 2, "Should have at least 2 workers"

    def test_glue_job_has_required_arguments(self, glue_client, environment_name):
        """Glue job should have all required default arguments."""
        job_name = f"{environment_name}-etl-job"
        response = glue_client.get_job(JobName=job_name)

        args = response["Job"]["DefaultArguments"]
        required_args = [
            "--RAW_BUCKET",
            "--CURATED_BUCKET",
            "--QUARANTINE_BUCKET",
            "--SECRET_ARN",
            "--KMS_KEY_ARN",
            "--DATABASE_NAME",
            "--TABLE_NAME"
        ]

        for arg in required_args:
            assert arg in args, f"Missing required argument: {arg}"

    def test_glue_job_has_redshift_arguments(self, glue_client, environment_name):
        """Glue job should have Redshift arguments (Phase 3 requirement)."""
        job_name = f"{environment_name}-etl-job"
        response = glue_client.get_job(JobName=job_name)

        args = response["Job"]["DefaultArguments"]
        redshift_args = [
            "--REDSHIFT_CONNECTION",
            "--REDSHIFT_IAM_ROLE",
            "--REDSHIFT_TEMP_DIR"
        ]

        for arg in redshift_args:
            assert arg in args, f"Missing Redshift argument: {arg}"

    def test_glue_job_has_connection(self, glue_client, environment_name):
        """Glue job should have Redshift connection attached."""
        job_name = f"{environment_name}-etl-job"
        response = glue_client.get_job(JobName=job_name)

        connections = response["Job"].get("Connections", {}).get("Connections", [])
        assert len(connections) > 0, "No connections attached to job"
        assert any("redshift" in c.lower() for c in connections), "Redshift connection not found"

    def test_glue_job_max_concurrent_runs(self, glue_client, environment_name):
        """Glue job should limit concurrent runs to 1."""
        job_name = f"{environment_name}-etl-job"
        response = glue_client.get_job(JobName=job_name)

        max_runs = response["Job"]["ExecutionProperty"]["MaxConcurrentRuns"]
        assert max_runs == 1, f"MaxConcurrentRuns should be 1, got {max_runs}"


@pytest.mark.phase2
class TestGlueJobRole:
    """Test Glue job IAM role configuration."""

    def test_glue_role_exists(self, iam_client, environment_name):
        """Glue ETL role should exist."""
        role_name = f"{environment_name}-glue-etl-role"
        response = iam_client.get_role(RoleName=role_name)
        assert response["Role"]["RoleName"] == role_name

    def test_glue_role_trust_policy(self, iam_client, environment_name):
        """Glue role should trust glue.amazonaws.com."""
        role_name = f"{environment_name}-glue-etl-role"
        response = iam_client.get_role(RoleName=role_name)

        trust_policy = response["Role"]["AssumeRolePolicyDocument"]
        principals = []
        for statement in trust_policy.get("Statement", []):
            principal = statement.get("Principal", {})
            if isinstance(principal.get("Service"), str):
                principals.append(principal["Service"])
            elif isinstance(principal.get("Service"), list):
                principals.extend(principal["Service"])

        assert "glue.amazonaws.com" in principals, "Glue service not in trust policy"

    def test_glue_role_has_managed_policy(self, iam_client, environment_name):
        """Glue role should have AWSGlueServiceRole attached."""
        role_name = f"{environment_name}-glue-etl-role"
        response = iam_client.list_attached_role_policies(RoleName=role_name)

        policy_arns = [p["PolicyArn"] for p in response["AttachedPolicies"]]
        glue_policy = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        assert glue_policy in policy_arns, "AWSGlueServiceRole not attached"

    def test_glue_role_has_s3_permissions(self, iam_client, environment_name):
        """Glue role should have S3 read/write permissions."""
        role_name = f"{environment_name}-glue-etl-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        has_s3 = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "s3:GetObject" in policy_str and "s3:PutObject" in policy_str:
                has_s3 = True
                break

        assert has_s3, "Glue role missing S3 permissions"

    def test_glue_role_has_kms_permissions(self, iam_client, environment_name):
        """Glue role should have KMS permissions."""
        role_name = f"{environment_name}-glue-etl-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        has_kms = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "kms:Decrypt" in policy_str and "kms:Encrypt" in policy_str:
                has_kms = True
                break

        assert has_kms, "Glue role missing KMS permissions"

    def test_glue_role_has_secrets_manager_permissions(self, iam_client, environment_name):
        """Glue role should have Secrets Manager permissions."""
        role_name = f"{environment_name}-glue-etl-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        has_secrets = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "secretsmanager:GetSecretValue" in policy_str:
                has_secrets = True
                break

        assert has_secrets, "Glue role missing Secrets Manager permissions"

    def test_glue_role_has_ec2_network_permissions(self, iam_client, environment_name):
        """Glue role should have EC2 network permissions for VPC connections."""
        role_name = f"{environment_name}-glue-etl-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        has_ec2 = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "ec2:CreateNetworkInterface" in policy_str:
                has_ec2 = True
                break

        assert has_ec2, "Glue role missing EC2 network permissions"


@pytest.mark.phase2
class TestGlueDatabase:
    """Test Glue Data Catalog database."""

    def test_glue_database_exists(self, glue_client, environment_name):
        """Glue database should exist."""
        db_name = f"{environment_name}_db"
        response = glue_client.get_database(Name=db_name)
        assert response["Database"]["Name"] == db_name

    def test_glue_database_has_description(self, glue_client, environment_name):
        """Glue database should have a description."""
        db_name = f"{environment_name}_db"
        response = glue_client.get_database(Name=db_name)
        assert response["Database"].get("Description"), "Database missing description"


@pytest.mark.phase2
class TestGlueCrawler:
    """Test Glue crawler configuration."""

    def test_glue_crawler_exists(self, glue_client, environment_name):
        """Glue crawler should exist."""
        crawler_name = f"{environment_name}-curated-crawler"
        response = glue_client.get_crawler(Name=crawler_name)
        assert response["Crawler"]["Name"] == crawler_name

    def test_glue_crawler_targets_curated_bucket(self, glue_client, environment_name, storage_stack_outputs):
        """Glue crawler should target curated bucket."""
        crawler_name = f"{environment_name}-curated-crawler"
        curated_bucket = storage_stack_outputs.get("CuratedBucketName")

        response = glue_client.get_crawler(Name=crawler_name)
        targets = response["Crawler"]["Targets"].get("S3Targets", [])

        target_paths = [t["Path"] for t in targets]
        assert any(curated_bucket in p for p in target_paths), \
            f"Crawler not targeting curated bucket: {target_paths}"


@pytest.mark.phase2
class TestCuratedBucket:
    """Test curated data bucket configuration."""

    def test_curated_bucket_exists(self, s3_client, storage_stack_outputs):
        """Curated bucket should exist."""
        bucket_name = storage_stack_outputs.get("CuratedBucketName")
        response = s3_client.head_bucket(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_curated_bucket_kms_encryption(self, s3_client, storage_stack_outputs):
        """Curated bucket should use KMS encryption."""
        bucket_name = storage_stack_outputs.get("CuratedBucketName")
        response = s3_client.get_bucket_encryption(Bucket=bucket_name)

        rules = response["ServerSideEncryptionConfiguration"]["Rules"]
        sse_algo = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
        assert sse_algo == "aws:kms", f"Expected aws:kms, got {sse_algo}"

    def test_curated_bucket_blocks_public_access(self, s3_client, storage_stack_outputs):
        """Curated bucket should block public access."""
        bucket_name = storage_stack_outputs.get("CuratedBucketName")
        response = s3_client.get_public_access_block(Bucket=bucket_name)

        config = response["PublicAccessBlockConfiguration"]
        assert all([
            config["BlockPublicAcls"],
            config["BlockPublicPolicy"],
            config["IgnorePublicAcls"],
            config["RestrictPublicBuckets"]
        ]), "Public access not fully blocked"


@pytest.mark.phase2
class TestQuarantineBucket:
    """Test quarantine data bucket configuration."""

    def test_quarantine_bucket_exists(self, s3_client, processing_stack_outputs):
        """Quarantine bucket should exist."""
        bucket_name = processing_stack_outputs.get("QuarantineBucketName")
        response = s3_client.head_bucket(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_quarantine_bucket_has_lifecycle_rule(self, s3_client, processing_stack_outputs):
        """Quarantine bucket should have expiration lifecycle rule."""
        bucket_name = processing_stack_outputs.get("QuarantineBucketName")
        try:
            response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
            rules = response.get("Rules", [])

            has_expiration = any(
                rule.get("Expiration", {}).get("Days") is not None
                for rule in rules
            )
            assert has_expiration, "No expiration rule found"
        except s3_client.exceptions.ClientError as e:
            if "NoSuchLifecycleConfiguration" in str(e):
                pytest.fail("No lifecycle configuration on quarantine bucket")
            raise
