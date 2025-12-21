"""
Phase 4 Tests - GDPR Article 17 Erasure Handler

Tests for:
- DynamoDB table configuration (stream, encryption, PITR, sparse GSI)
- Lambda function configuration (runtime, timeout, VPC, env vars, trigger)
- Athena workgroup (exists, KMS encryption)
- IAM role permissions
- Integration tests for erasure flow
"""

import hashlib
import json
import pytest
from tests.conftest import get_stack_status, get_stack_outputs


# =============================================================================
# Unit Tests - Erasure Logic
# =============================================================================

@pytest.mark.phase4
class TestErasureLogic:
    """Unit tests for erasure handler logic."""

    def test_patient_hash_format_valid(self):
        """Patient ID hash should be 64 hex characters (SHA256)."""
        patient_id = "patient-12345"
        salt = "test-salt"
        combined = f"{patient_id}{salt}"
        patient_hash = hashlib.sha256(combined.encode()).hexdigest()

        assert len(patient_hash) == 64
        assert all(c in '0123456789abcdef' for c in patient_hash)

    def test_request_status_values(self):
        """Verify valid status values for erasure requests."""
        valid_statuses = ['PENDING', 'APPROVED', 'PROCESSING', 'COMPLETED', 'FAILED']
        # Status transition: PENDING -> APPROVED -> PROCESSING -> COMPLETED/FAILED
        assert 'APPROVED' in valid_statuses
        assert 'PROCESSING' in valid_statuses

    def test_athena_query_escaping(self):
        """Ensure patient hash is safe for SQL queries (no injection risk)."""
        # SHA256 hex output should never contain SQL injection characters
        test_hash = hashlib.sha256(b"test").hexdigest()
        dangerous_chars = ["'", '"', ';', '--', '/*', '*/']

        for char in dangerous_chars:
            assert char not in test_hash, f"Hash should not contain {char}"


# =============================================================================
# Infrastructure Tests - Compliance Stack
# =============================================================================

@pytest.mark.phase4
class TestComplianceStack:
    """Test compliance CloudFormation stack deployment."""

    def test_compliance_stack_exists(self, cloudformation_client, environment_name):
        """Compliance stack should be deployed."""
        status = get_stack_status(cloudformation_client, f"{environment_name}-compliance")
        assert status is not None, "Compliance stack not found"
        assert status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"], f"Stack status: {status}"

    def test_stack_has_required_outputs(self, compliance_stack_outputs):
        """Stack should export required resources."""
        required = [
            "GdprRequestsTableName",
            "GdprRequestsTableArn",
            "ErasureHandlerArn",
            "ErasureAthenaWorkgroupName"
        ]
        for output in required:
            assert output in compliance_stack_outputs, f"Missing output: {output}"


# =============================================================================
# Infrastructure Tests - DynamoDB Table
# =============================================================================

@pytest.mark.phase4
class TestDynamoDBTable:
    """Test GDPR requests DynamoDB table configuration."""

    def test_table_exists(self, dynamodb_client, environment_name):
        """GDPR requests table should exist."""
        table_name = f"{environment_name}-gdpr-requests"
        response = dynamodb_client.describe_table(TableName=table_name)
        assert response["Table"]["TableName"] == table_name

    def test_table_has_stream_enabled(self, dynamodb_client, environment_name):
        """Table should have DynamoDB Streams enabled."""
        table_name = f"{environment_name}-gdpr-requests"
        response = dynamodb_client.describe_table(TableName=table_name)

        stream_spec = response["Table"].get("StreamSpecification", {})
        assert stream_spec.get("StreamEnabled") is True, "Stream not enabled"
        assert stream_spec.get("StreamViewType") == "NEW_AND_OLD_IMAGES"

    def test_table_encrypted_with_kms(self, dynamodb_client, environment_name):
        """Table should be encrypted with KMS."""
        table_name = f"{environment_name}-gdpr-requests"
        response = dynamodb_client.describe_table(TableName=table_name)

        sse = response["Table"].get("SSEDescription", {})
        assert sse.get("Status") == "ENABLED", "SSE not enabled"
        assert sse.get("SSEType") == "KMS", "Should use KMS encryption"

    def test_table_pitr_enabled(self, dynamodb_client, environment_name):
        """Point-in-time recovery should be enabled for compliance."""
        table_name = f"{environment_name}-gdpr-requests"
        response = dynamodb_client.describe_continuous_backups(TableName=table_name)

        pitr = response["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
        assert pitr["PointInTimeRecoveryStatus"] == "ENABLED"

    def test_table_has_status_gsi(self, dynamodb_client, environment_name):
        """Table should have GSI on status for querying pending/failed requests."""
        table_name = f"{environment_name}-gdpr-requests"
        response = dynamodb_client.describe_table(TableName=table_name)

        gsis = response["Table"].get("GlobalSecondaryIndexes", [])
        gsi_names = [gsi["IndexName"] for gsi in gsis]
        assert "status-index" in gsi_names, "Missing status-index GSI"

    def test_table_billing_mode(self, dynamodb_client, environment_name):
        """Table should use on-demand billing."""
        table_name = f"{environment_name}-gdpr-requests"
        response = dynamodb_client.describe_table(TableName=table_name)

        billing = response["Table"].get("BillingModeSummary", {})
        assert billing.get("BillingMode") == "PAY_PER_REQUEST", "Should use on-demand billing"


# =============================================================================
# Infrastructure Tests - Lambda Function
# =============================================================================

@pytest.mark.phase4
class TestErasureLambda:
    """Test erasure handler Lambda configuration."""

    def test_lambda_function_exists(self, lambda_client, environment_name):
        """Erasure handler Lambda should exist."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.get_function(FunctionName=function_name)
        assert response["Configuration"]["FunctionName"] == function_name

    def test_lambda_runtime_is_python312(self, lambda_client, environment_name):
        """Lambda should use Python 3.12 runtime."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.get_function(FunctionName=function_name)
        assert response["Configuration"]["Runtime"] == "python3.12"

    def test_lambda_timeout_adequate(self, lambda_client, environment_name):
        """Lambda timeout should be at least 5 minutes for large partitions."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.get_function(FunctionName=function_name)
        timeout = response["Configuration"]["Timeout"]
        assert timeout >= 300, f"Timeout too short: {timeout}s (need >= 300s)"

    def test_lambda_memory_adequate(self, lambda_client, environment_name):
        """Lambda should have at least 512MB memory."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.get_function(FunctionName=function_name)
        memory = response["Configuration"]["MemorySize"]
        assert memory >= 512, f"Memory too low: {memory}MB (need >= 512MB)"

    def test_lambda_in_vpc(self, lambda_client, environment_name):
        """Lambda should be deployed in VPC for private connectivity."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.get_function(FunctionName=function_name)

        vpc_config = response["Configuration"].get("VpcConfig", {})
        assert vpc_config.get("SubnetIds"), "Lambda not in VPC (no subnets)"
        assert vpc_config.get("SecurityGroupIds"), "Lambda missing security groups"

    def test_lambda_has_required_env_vars(self, lambda_client, environment_name):
        """Lambda should have required environment variables."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.get_function(FunctionName=function_name)

        env_vars = response["Configuration"].get("Environment", {}).get("Variables", {})
        required_vars = [
            "ENVIRONMENT_NAME",
            "CURATED_BUCKET",
            "GLUE_DATABASE",
            "GLUE_TABLE",
            "ATHENA_WORKGROUP",
            "REDSHIFT_WORKGROUP",
            "REDSHIFT_DATABASE",
            "REQUESTS_TABLE"
        ]

        for var in required_vars:
            assert var in env_vars, f"Missing env var: {var}"

    def test_lambda_has_dynamodb_trigger(self, lambda_client, environment_name):
        """Lambda should have DynamoDB Streams trigger."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.list_event_source_mappings(FunctionName=function_name)

        ddb_triggers = [
            m for m in response["EventSourceMappings"]
            if "dynamodb" in m["EventSourceArn"]
        ]
        assert len(ddb_triggers) >= 1, "No DynamoDB trigger found"

    def test_lambda_trigger_has_filter(self, lambda_client, environment_name):
        """Lambda trigger should filter for APPROVED status only."""
        function_name = f"{environment_name}-erasure-handler"
        response = lambda_client.list_event_source_mappings(FunctionName=function_name)

        for mapping in response["EventSourceMappings"]:
            if "dynamodb" in mapping["EventSourceArn"]:
                filter_criteria = mapping.get("FilterCriteria", {})
                filters = filter_criteria.get("Filters", [])
                assert len(filters) > 0, "Trigger should have filter criteria"
                # Verify filter looks for APPROVED status
                filter_pattern = filters[0].get("Pattern", "")
                assert "APPROVED" in filter_pattern, "Filter should match APPROVED status"


# =============================================================================
# Infrastructure Tests - Athena Workgroup
# =============================================================================

@pytest.mark.phase4
class TestAthenaWorkgroup:
    """Test Athena workgroup for erasure operations."""

    def test_workgroup_exists(self, athena_client, environment_name):
        """Erasure Athena workgroup should exist."""
        workgroup_name = f"{environment_name}-erasure-workgroup"
        response = athena_client.get_work_group(WorkGroup=workgroup_name)
        assert response["WorkGroup"]["Name"] == workgroup_name

    def test_workgroup_kms_encrypted(self, athena_client, environment_name):
        """Workgroup results should be KMS encrypted."""
        workgroup_name = f"{environment_name}-erasure-workgroup"
        response = athena_client.get_work_group(WorkGroup=workgroup_name)

        config = response["WorkGroup"]["Configuration"]["ResultConfiguration"]
        encryption = config.get("EncryptionConfiguration", {})
        assert encryption.get("EncryptionOption") == "SSE_KMS", \
            "Workgroup should use SSE_KMS encryption"

    def test_workgroup_enforces_configuration(self, athena_client, environment_name):
        """Workgroup should enforce its configuration."""
        workgroup_name = f"{environment_name}-erasure-workgroup"
        response = athena_client.get_work_group(WorkGroup=workgroup_name)

        config = response["WorkGroup"]["Configuration"]
        assert config.get("EnforceWorkGroupConfiguration") is True, \
            "Should enforce workgroup configuration"


# =============================================================================
# Infrastructure Tests - IAM Permissions
# =============================================================================

@pytest.mark.phase4
class TestIAMPermissions:
    """Test IAM role has required permissions."""

    def test_erasure_role_exists(self, iam_client, environment_name):
        """Erasure handler IAM role should exist."""
        role_name = f"{environment_name}-erasure-handler-role"
        response = iam_client.get_role(RoleName=role_name)
        assert response["Role"]["RoleName"] == role_name

    def test_role_trusts_lambda(self, iam_client, environment_name):
        """Role should trust Lambda service."""
        role_name = f"{environment_name}-erasure-handler-role"
        response = iam_client.get_role(RoleName=role_name)

        trust = response["Role"]["AssumeRolePolicyDocument"]
        principals = []
        for stmt in trust.get("Statement", []):
            svc = stmt.get("Principal", {}).get("Service")
            if isinstance(svc, str):
                principals.append(svc)
            elif isinstance(svc, list):
                principals.extend(svc)

        assert "lambda.amazonaws.com" in principals, "Role should trust lambda.amazonaws.com"

    def test_role_has_athena_permissions(self, iam_client, environment_name):
        """Role should have Athena permissions."""
        role_name = f"{environment_name}-erasure-handler-role"
        policies = iam_client.list_role_policies(RoleName=role_name)

        has_athena = False
        for policy_name in policies["PolicyNames"]:
            doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            if "athena:" in json.dumps(doc["PolicyDocument"]):
                has_athena = True
                break

        assert has_athena, "Role missing Athena permissions"

    def test_role_has_redshift_data_permissions(self, iam_client, environment_name):
        """Role should have Redshift Data API permissions."""
        role_name = f"{environment_name}-erasure-handler-role"
        policies = iam_client.list_role_policies(RoleName=role_name)

        has_redshift = False
        for policy_name in policies["PolicyNames"]:
            doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            if "redshift-data:" in json.dumps(doc["PolicyDocument"]):
                has_redshift = True
                break

        assert has_redshift, "Role missing Redshift Data API permissions"

    def test_role_has_s3_permissions(self, iam_client, environment_name):
        """Role should have S3 permissions for curated bucket."""
        role_name = f"{environment_name}-erasure-handler-role"
        policies = iam_client.list_role_policies(RoleName=role_name)

        has_s3 = False
        for policy_name in policies["PolicyNames"]:
            doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(doc["PolicyDocument"])
            if "s3:GetObject" in policy_str and "s3:DeleteObject" in policy_str:
                has_s3 = True
                break

        assert has_s3, "Role missing S3 read/delete permissions"

    def test_role_has_kms_permissions(self, iam_client, environment_name):
        """Role should have KMS permissions."""
        role_name = f"{environment_name}-erasure-handler-role"
        policies = iam_client.list_role_policies(RoleName=role_name)

        has_kms = False
        for policy_name in policies["PolicyNames"]:
            doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            if "kms:" in json.dumps(doc["PolicyDocument"]):
                has_kms = True
                break

        assert has_kms, "Role missing KMS permissions"


# =============================================================================
# Infrastructure Tests - VPC Endpoints
# =============================================================================

@pytest.mark.phase4
class TestVPCEndpoints:
    """Test VPC endpoints required for compliance Lambda."""

    def test_dynamodb_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """DynamoDB VPC endpoint should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        assert vpc_id, "VpcId not found in networking outputs"

        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.dynamodb"]}
            ]
        )

        endpoints = response.get("VpcEndpoints", [])
        assert len(endpoints) >= 1, "DynamoDB VPC endpoint not found"
        assert endpoints[0]["State"] == "available", "DynamoDB endpoint not available"

    def test_athena_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """Athena VPC endpoint should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        assert vpc_id, "VpcId not found in networking outputs"

        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.athena"]}
            ]
        )

        endpoints = response.get("VpcEndpoints", [])
        assert len(endpoints) >= 1, "Athena VPC endpoint not found"
        assert endpoints[0]["State"] == "available", "Athena endpoint not available"

    def test_redshift_data_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """Redshift Data API VPC endpoint should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        assert vpc_id, "VpcId not found in networking outputs"

        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.redshift-data"]}
            ]
        )

        endpoints = response.get("VpcEndpoints", [])
        assert len(endpoints) >= 1, "Redshift Data API VPC endpoint not found"
        assert endpoints[0]["State"] == "available", "Redshift Data endpoint not available"


# =============================================================================
# Infrastructure Tests - CloudWatch
# =============================================================================

@pytest.mark.phase4
class TestCloudWatchResources:
    """Test CloudWatch resources for monitoring and audit."""

    def test_log_group_exists(self, logs_client, environment_name):
        """Lambda log group should exist."""
        log_group_name = f"/aws/lambda/{environment_name}-erasure-handler"
        response = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)

        log_groups = response.get("logGroups", [])
        matching = [lg for lg in log_groups if lg["logGroupName"] == log_group_name]
        assert len(matching) >= 1, f"Log group {log_group_name} not found"

    def test_log_group_retention(self, logs_client, environment_name):
        """Log group should have 365-day retention for compliance."""
        log_group_name = f"/aws/lambda/{environment_name}-erasure-handler"
        response = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)

        log_groups = response.get("logGroups", [])
        matching = [lg for lg in log_groups if lg["logGroupName"] == log_group_name]
        assert len(matching) >= 1, "Log group not found"

        retention = matching[0].get("retentionInDays")
        assert retention == 365, f"Expected 365-day retention, got {retention}"

    def test_failure_alarm_exists(self, cloudwatch_client, environment_name):
        """Erasure failure alarm should exist."""
        alarm_name = f"{environment_name}-erasure-failures"
        response = cloudwatch_client.describe_alarms(AlarmNames=[alarm_name])

        alarms = response.get("MetricAlarms", [])
        assert len(alarms) >= 1, f"Alarm {alarm_name} not found"
