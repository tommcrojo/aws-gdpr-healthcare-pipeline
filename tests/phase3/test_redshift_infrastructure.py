"""
Phase 3 Tests - Redshift Infrastructure

Tests for Redshift Serverless deployment and configuration.
"""

import json
import pytest
from tests.conftest import get_stack_status


@pytest.mark.phase3
class TestRedshiftStack:
    """Test Redshift CloudFormation stack deployment."""

    def test_redshift_stack_exists(self, cloudformation_client, environment_name):
        """Redshift stack should be deployed."""
        status = get_stack_status(cloudformation_client, f"{environment_name}-redshift")
        assert status is not None, "Redshift stack not found"
        assert status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"], f"Redshift stack status: {status}"

    def test_redshift_stack_has_required_outputs(self, redshift_stack_outputs):
        """Redshift stack should export workgroup and connection names."""
        required = [
            "RedshiftWorkgroupName",
            "RedshiftEndpoint",
            "RedshiftS3RoleArn",
            "GlueConnectionName",
            "GlueConnectionSecurityGroupId"
        ]
        for output in required:
            assert output in redshift_stack_outputs, f"Missing {output} output"


@pytest.mark.phase3
class TestRedshiftServerless:
    """Test Redshift Serverless workgroup configuration."""

    def test_redshift_namespace_exists(self, redshift_serverless_client, environment_name):
        """Redshift namespace should exist."""
        namespace_name = f"{environment_name}-namespace"
        response = redshift_serverless_client.get_namespace(namespaceName=namespace_name)
        assert response["namespace"]["namespaceName"] == namespace_name

    def test_redshift_namespace_has_database(self, redshift_serverless_client, environment_name):
        """Namespace should have healthcare_analytics database."""
        namespace_name = f"{environment_name}-namespace"
        response = redshift_serverless_client.get_namespace(namespaceName=namespace_name)
        assert response["namespace"]["dbName"] == "healthcare_analytics", \
            "Database should be healthcare_analytics"

    def test_redshift_namespace_kms_encrypted(self, redshift_serverless_client, environment_name, kms_stack_outputs):
        """Namespace should be encrypted with customer KMS key."""
        namespace_name = f"{environment_name}-namespace"
        response = redshift_serverless_client.get_namespace(namespaceName=namespace_name)

        kms_key_id = response["namespace"].get("kmsKeyId")
        assert kms_key_id, "Namespace not KMS encrypted"

        expected_key = kms_stack_outputs.get("KmsKeyArn")
        assert expected_key in kms_key_id or kms_stack_outputs.get("KmsKeyId") in kms_key_id, \
            "Namespace using wrong KMS key"

    def test_redshift_namespace_has_iam_role(self, redshift_serverless_client, environment_name):
        """Namespace should have IAM role for S3 access."""
        namespace_name = f"{environment_name}-namespace"
        response = redshift_serverless_client.get_namespace(namespaceName=namespace_name)

        iam_roles = response["namespace"].get("iamRoles", [])
        assert len(iam_roles) > 0, "No IAM roles attached to namespace"

    def test_redshift_workgroup_exists(self, redshift_serverless_client, environment_name):
        """Redshift workgroup should exist and be available."""
        workgroup_name = f"{environment_name}-workgroup"
        response = redshift_serverless_client.get_workgroup(workgroupName=workgroup_name)

        status = response["workgroup"]["status"]
        assert status == "AVAILABLE", f"Workgroup status: {status}"

    def test_redshift_workgroup_not_publicly_accessible(self, redshift_serverless_client, environment_name):
        """Workgroup should not be publicly accessible."""
        workgroup_name = f"{environment_name}-workgroup"
        response = redshift_serverless_client.get_workgroup(workgroupName=workgroup_name)

        assert not response["workgroup"]["publiclyAccessible"], \
            "Workgroup should not be publicly accessible"

    def test_redshift_workgroup_enhanced_vpc_routing(self, redshift_serverless_client, environment_name):
        """Workgroup should have enhanced VPC routing enabled."""
        workgroup_name = f"{environment_name}-workgroup"
        response = redshift_serverless_client.get_workgroup(workgroupName=workgroup_name)

        assert response["workgroup"]["enhancedVpcRouting"], \
            "Enhanced VPC routing should be enabled"

    def test_redshift_workgroup_in_private_subnets(self, redshift_serverless_client, environment_name, networking_stack_outputs):
        """Workgroup should be in private subnets."""
        workgroup_name = f"{environment_name}-workgroup"
        response = redshift_serverless_client.get_workgroup(workgroupName=workgroup_name)

        workgroup_subnets = response["workgroup"]["subnetIds"]
        expected_subnets = networking_stack_outputs.get("PrivateSubnetIds", "").split(",")

        for subnet in expected_subnets:
            assert subnet in workgroup_subnets, f"Subnet {subnet} not in workgroup"

    def test_redshift_workgroup_has_endpoint(self, redshift_serverless_client, environment_name):
        """Workgroup should have an endpoint."""
        workgroup_name = f"{environment_name}-workgroup"
        response = redshift_serverless_client.get_workgroup(workgroupName=workgroup_name)

        endpoint = response["workgroup"].get("endpoint", {})
        assert endpoint.get("address"), "Workgroup has no endpoint address"
        assert endpoint.get("port") == 5439, f"Expected port 5439, got {endpoint.get('port')}"


@pytest.mark.phase3
class TestRedshiftSecurityGroups:
    """Test Redshift security group configuration."""

    def test_glue_connection_sg_exists(self, ec2_client, redshift_stack_outputs):
        """Glue connection security group should exist."""
        sg_id = redshift_stack_outputs.get("GlueConnectionSecurityGroupId")
        assert sg_id, "Security group ID not found"

        response = ec2_client.describe_security_groups(GroupIds=[sg_id])
        assert len(response["SecurityGroups"]) == 1, "Security group not found"

    def test_glue_connection_sg_has_self_reference(self, ec2_client, redshift_stack_outputs):
        """Glue connection SG should allow self-referencing traffic."""
        sg_id = redshift_stack_outputs.get("GlueConnectionSecurityGroupId")
        response = ec2_client.describe_security_groups(GroupIds=[sg_id])

        ingress_rules = response["SecurityGroups"][0]["IpPermissions"]
        has_self_ref = False

        for rule in ingress_rules:
            for group in rule.get("UserIdGroupPairs", []):
                if group.get("GroupId") == sg_id:
                    has_self_ref = True
                    break

        assert has_self_ref, "Glue connection SG missing self-referencing rule"

    def test_redshift_sg_allows_from_glue_sg(self, ec2_client, redshift_stack_outputs, environment_name):
        """Redshift SG should allow traffic from Glue connection SG on port 5439."""
        glue_sg_id = redshift_stack_outputs.get("GlueConnectionSecurityGroupId")

        # Find Redshift security group
        response = ec2_client.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [f"{environment_name}-redshift-sg"]}]
        )

        if not response["SecurityGroups"]:
            pytest.skip("Redshift security group not found by name")

        redshift_sg = response["SecurityGroups"][0]
        ingress_rules = redshift_sg["IpPermissions"]

        has_glue_ingress = False
        for rule in ingress_rules:
            if rule.get("FromPort") == 5439 and rule.get("ToPort") == 5439:
                for group in rule.get("UserIdGroupPairs", []):
                    if group.get("GroupId") == glue_sg_id:
                        has_glue_ingress = True
                        break

        assert has_glue_ingress, "Redshift SG doesn't allow Glue SG on port 5439"


@pytest.mark.phase3
class TestRedshiftIAMRole:
    """Test Redshift S3 access IAM role."""

    def test_redshift_s3_role_exists(self, iam_client, environment_name):
        """Redshift S3 role should exist."""
        role_name = f"{environment_name}-redshift-s3-role"
        response = iam_client.get_role(RoleName=role_name)
        assert response["Role"]["RoleName"] == role_name

    def test_redshift_s3_role_trust_policy(self, iam_client, environment_name):
        """Redshift role should trust redshift.amazonaws.com."""
        role_name = f"{environment_name}-redshift-s3-role"
        response = iam_client.get_role(RoleName=role_name)

        trust_policy = response["Role"]["AssumeRolePolicyDocument"]
        principals = []
        for statement in trust_policy.get("Statement", []):
            principal = statement.get("Principal", {})
            if isinstance(principal.get("Service"), str):
                principals.append(principal["Service"])
            elif isinstance(principal.get("Service"), list):
                principals.extend(principal["Service"])

        assert "redshift.amazonaws.com" in principals, "Redshift service not in trust policy"

    def test_redshift_s3_role_has_s3_permissions(self, iam_client, environment_name):
        """Redshift role should have S3 read permissions."""
        role_name = f"{environment_name}-redshift-s3-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        has_s3 = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "s3:GetObject" in policy_str:
                has_s3 = True
                break

        assert has_s3, "Redshift role missing S3 permissions"

    def test_redshift_s3_role_has_kms_permissions(self, iam_client, environment_name):
        """Redshift role should have KMS decrypt permissions."""
        role_name = f"{environment_name}-redshift-s3-role"
        response = iam_client.list_role_policies(RoleName=role_name)

        has_kms = False
        for policy_name in response["PolicyNames"]:
            policy_doc = iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
            policy_str = json.dumps(policy_doc["PolicyDocument"])
            if "kms:Decrypt" in policy_str:
                has_kms = True
                break

        assert has_kms, "Redshift role missing KMS permissions"


@pytest.mark.phase3
class TestGlueConnection:
    """Test Glue connection for Redshift."""

    def test_glue_connection_exists(self, glue_client, environment_name):
        """Glue connection for Redshift should exist."""
        connection_name = f"{environment_name}-redshift-connection"
        response = glue_client.get_connection(Name=connection_name)
        assert response["Connection"]["Name"] == connection_name

    def test_glue_connection_type_is_jdbc(self, glue_client, environment_name):
        """Glue connection should be JDBC type."""
        connection_name = f"{environment_name}-redshift-connection"
        response = glue_client.get_connection(Name=connection_name)
        assert response["Connection"]["ConnectionType"] == "JDBC", "Connection should be JDBC type"

    def test_glue_connection_has_jdbc_url(self, glue_client, environment_name):
        """Glue connection should have JDBC URL."""
        connection_name = f"{environment_name}-redshift-connection"
        response = glue_client.get_connection(Name=connection_name)

        props = response["Connection"]["ConnectionProperties"]
        jdbc_url = props.get("JDBC_CONNECTION_URL", "")

        assert "jdbc:redshift://" in jdbc_url, "Missing Redshift JDBC URL"
        assert "healthcare_analytics" in jdbc_url, "JDBC URL should reference healthcare_analytics database"
        assert ":5439/" in jdbc_url, "JDBC URL should use port 5439"

    def test_glue_connection_has_vpc_config(self, glue_client, environment_name):
        """Glue connection should have VPC configuration."""
        connection_name = f"{environment_name}-redshift-connection"
        response = glue_client.get_connection(Name=connection_name)

        physical_conn = response["Connection"].get("PhysicalConnectionRequirements", {})
        assert physical_conn.get("SubnetId"), "Connection missing subnet"
        assert physical_conn.get("SecurityGroupIdList"), "Connection missing security groups"
