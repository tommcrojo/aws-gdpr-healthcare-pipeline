"""
Phase 1 Tests - Networking Configuration

Tests for VPC, subnets, and VPC endpoints.
"""

import pytest
from tests.conftest import get_stack_status


@pytest.mark.phase1
class TestNetworkingStack:
    """Test networking CloudFormation stack deployment."""

    def test_networking_stack_exists(self, cloudformation_client, environment_name):
        """Networking stack should be deployed."""
        status = get_stack_status(cloudformation_client, f"{environment_name}-networking")
        assert status is not None, "Networking stack not found"
        assert status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"], f"Networking stack status: {status}"

    def test_networking_stack_has_required_outputs(self, networking_stack_outputs):
        """Networking stack should export VPC and subnet IDs."""
        required_outputs = ["VpcId", "PrivateSubnetIds", "VPCEndpointSecurityGroupId"]
        for output in required_outputs:
            assert output in networking_stack_outputs, f"Missing {output} output"


@pytest.mark.phase1
class TestVPCConfiguration:
    """Test VPC properties."""

    def test_vpc_exists(self, ec2_client, networking_stack_outputs):
        """VPC should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        assert vpc_id, "VPC ID not found"

        response = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        assert len(response["Vpcs"]) == 1, "VPC not found"

    def test_vpc_cidr_block(self, ec2_client, networking_stack_outputs):
        """VPC should have correct CIDR block."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        cidr = response["Vpcs"][0]["CidrBlock"]
        assert cidr == "10.0.0.0/16", f"Unexpected CIDR: {cidr}"

    def test_vpc_dns_support_enabled(self, ec2_client, networking_stack_outputs):
        """VPC should have DNS support enabled."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsSupport")
        assert response["EnableDnsSupport"]["Value"], "DNS support not enabled"

    def test_vpc_dns_hostnames_enabled(self, ec2_client, networking_stack_outputs):
        """VPC should have DNS hostnames enabled."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsHostnames")
        assert response["EnableDnsHostnames"]["Value"], "DNS hostnames not enabled"


@pytest.mark.phase1
class TestPrivateSubnets:
    """Test private subnet configuration."""

    def test_private_subnets_exist(self, ec2_client, networking_stack_outputs):
        """Two private subnets should exist."""
        subnet_ids = networking_stack_outputs.get("PrivateSubnetIds", "").split(",")
        assert len(subnet_ids) == 2, f"Expected 2 subnets, got {len(subnet_ids)}"

        response = ec2_client.describe_subnets(SubnetIds=subnet_ids)
        assert len(response["Subnets"]) == 2, "Subnets not found"

    def test_subnets_in_different_azs(self, ec2_client, networking_stack_outputs):
        """Subnets should be in different availability zones."""
        subnet_ids = networking_stack_outputs.get("PrivateSubnetIds", "").split(",")
        response = ec2_client.describe_subnets(SubnetIds=subnet_ids)

        azs = [s["AvailabilityZone"] for s in response["Subnets"]]
        assert len(set(azs)) == 2, f"Subnets in same AZ: {azs}"

    def test_subnets_no_public_ip(self, ec2_client, networking_stack_outputs):
        """Private subnets should not auto-assign public IPs."""
        subnet_ids = networking_stack_outputs.get("PrivateSubnetIds", "").split(",")
        response = ec2_client.describe_subnets(SubnetIds=subnet_ids)

        for subnet in response["Subnets"]:
            assert not subnet["MapPublicIpOnLaunch"], f"Subnet {subnet['SubnetId']} assigns public IPs"

    def test_subnets_have_correct_cidrs(self, ec2_client, networking_stack_outputs):
        """Subnets should have expected CIDR blocks."""
        subnet_ids = networking_stack_outputs.get("PrivateSubnetIds", "").split(",")
        response = ec2_client.describe_subnets(SubnetIds=subnet_ids)

        cidrs = sorted([s["CidrBlock"] for s in response["Subnets"]])
        expected = ["10.0.1.0/24", "10.0.2.0/24"]
        assert cidrs == expected, f"Unexpected CIDRs: {cidrs}"


@pytest.mark.phase1
class TestVPCEndpoints:
    """Test VPC endpoint configuration."""

    def test_s3_gateway_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """S3 Gateway endpoint should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.s3"]}
            ]
        )
        endpoints = response["VpcEndpoints"]
        assert len(endpoints) >= 1, "S3 Gateway endpoint not found"
        assert endpoints[0]["VpcEndpointType"] == "Gateway", "S3 endpoint is not Gateway type"

    def test_glue_interface_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """Glue Interface endpoint should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.glue"]}
            ]
        )
        endpoints = response["VpcEndpoints"]
        assert len(endpoints) >= 1, "Glue endpoint not found"
        assert endpoints[0]["VpcEndpointType"] == "Interface", "Glue endpoint is not Interface type"
        assert endpoints[0]["PrivateDnsEnabled"], "Private DNS not enabled for Glue endpoint"

    def test_secrets_manager_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """Secrets Manager Interface endpoint should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.secretsmanager"]}
            ]
        )
        endpoints = response["VpcEndpoints"]
        assert len(endpoints) >= 1, "Secrets Manager endpoint not found"

    def test_cloudwatch_logs_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """CloudWatch Logs Interface endpoint should exist."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.logs"]}
            ]
        )
        endpoints = response["VpcEndpoints"]
        assert len(endpoints) >= 1, "CloudWatch Logs endpoint not found"

    def test_redshift_serverless_endpoint_exists(self, ec2_client, networking_stack_outputs):
        """Redshift Serverless Interface endpoint should exist (Phase 3 requirement)."""
        vpc_id = networking_stack_outputs.get("VpcId")
        response = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.eu-central-1.redshift-serverless"]}
            ]
        )
        endpoints = response["VpcEndpoints"]
        assert len(endpoints) >= 1, "Redshift Serverless endpoint not found"


@pytest.mark.phase1
class TestSecurityGroups:
    """Test VPC endpoint security group."""

    def test_vpce_security_group_exists(self, ec2_client, networking_stack_outputs):
        """VPC endpoint security group should exist."""
        sg_id = networking_stack_outputs.get("VPCEndpointSecurityGroupId")
        assert sg_id, "Security group ID not found"

        response = ec2_client.describe_security_groups(GroupIds=[sg_id])
        assert len(response["SecurityGroups"]) == 1, "Security group not found"

    def test_vpce_security_group_allows_https_from_vpc(self, ec2_client, networking_stack_outputs):
        """Security group should allow HTTPS (443) from VPC CIDR."""
        sg_id = networking_stack_outputs.get("VPCEndpointSecurityGroupId")
        response = ec2_client.describe_security_groups(GroupIds=[sg_id])

        ingress_rules = response["SecurityGroups"][0]["IpPermissions"]
        https_rule = None
        for rule in ingress_rules:
            if rule.get("FromPort") == 443 and rule.get("ToPort") == 443:
                https_rule = rule
                break

        assert https_rule, "No HTTPS (443) ingress rule found"

        # Check it allows VPC CIDR
        cidrs = [r["CidrIp"] for r in https_rule.get("IpRanges", [])]
        assert "10.0.0.0/16" in cidrs, "VPC CIDR not in allowed sources"
