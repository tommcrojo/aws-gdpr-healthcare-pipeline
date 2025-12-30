#!/usr/bin/env python3
"""
GDPR Healthcare Pipeline - Erasure Performance Benchmark

Measures end-to-end performance and cost of GDPR Article 17 erasure operations.
Tests various scenarios with different partition counts to determine SLA feasibility.
"""

import argparse
import boto3
import json
import os
import time
from datetime import datetime
from typing import Dict, List
import uuid

# AWS Configuration
REGION = "eu-central-1"
ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME", "gdpr-healthcare")

# AWS Pricing (eu-central-1, December 2025)
PRICING = {
    "athena_per_tb_scanned": 5.00,
    "lambda_per_million_requests": 0.20,
    "lambda_per_gb_second": 0.0000166667,
    "s3_per_1k_put_requests": 0.005,
    "s3_per_1k_delete_requests": 0.000,  # Free
    "dynamodb_per_million_write_units": 1.25
}


class ErasureBenchmark:
    """Benchmarks GDPR erasure performance and cost."""

    def __init__(self, environment_name: str = ENVIRONMENT_NAME):
        self.environment_name = environment_name
        self.dynamodb = boto3.client("dynamodb", region_name=REGION)
        self.athena = boto3.client("athena", region_name=REGION)
        self.redshift_data = boto3.client("redshift-data", region_name=REGION)
        self.logs = boto3.client("logs", region_name=REGION)
        self.cloudwatch = boto3.client("cloudwatch", region_name=REGION)
        self.s3 = boto3.client("s3", region_name=REGION)
        self.cf_client = boto3.client("cloudformation", region_name=REGION)

        # Get infrastructure details
        self.table_name = f"{environment_name}-gdpr-requests"
        self.workgroup_name = f"{environment_name}-workgroup"
        self.athena_workgroup = f"{environment_name}-erasure-workgroup"
        self.database = f"{environment_name}_db"
        self.curated_bucket = self._get_stack_output("storage-ingestion", "CuratedBucketName")

        print(f"Initialized erasure benchmark for environment: {environment_name}")
        print(f"DynamoDB table: {self.table_name}")
        print(f"Redshift workgroup: {self.workgroup_name}")
        print(f"Athena workgroup: {self.athena_workgroup}")

    def _get_stack_output(self, stack_suffix: str, output_key: str) -> str:
        """Get output value from CloudFormation stack."""
        stack_name = f"{self.environment_name}-{stack_suffix}"
        response = self.cf_client.describe_stacks(StackName=stack_name)
        outputs = response["Stacks"][0].get("Outputs", [])
        for output in outputs:
            if output["OutputKey"] == output_key:
                return output["OutputValue"]
        raise ValueError(f"Output {output_key} not found in stack {stack_name}")

    def _load_manifest(self, scenario_id: str) -> Dict:
        """Load patient manifest from S3."""
        raw_bucket = self._get_stack_output("storage-ingestion", "RawBucketName")
        manifest_key = f"benchmark-manifests/scenario-{scenario_id}-manifest.json"

        try:
            response = self.s3.get_object(Bucket=raw_bucket, Key=manifest_key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except Exception as e:
            raise ValueError(f"Failed to load manifest for scenario {scenario_id}: {e}")

    def _insert_erasure_request(self, patient_id_hash: str) -> str:
        """Insert erasure request into DynamoDB."""
        request_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat() + "Z"

        self.dynamodb.put_item(
            TableName=self.table_name,
            Item={
                "request_id": {"S": request_id},
                "patient_id_hash": {"S": patient_id_hash},
                "status": {"S": "APPROVED"},  # Triggers Lambda immediately
                "requested_at": {"S": timestamp},
                "updated_at": {"S": timestamp},
                "requester": {"S": "benchmark"}
            }
        )

        return request_id

    def _poll_request_status(self, request_id: str, timeout_seconds: int = 600) -> Dict:
        """Poll DynamoDB for request completion."""
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            response = self.dynamodb.get_item(
                TableName=self.table_name,
                Key={"request_id": {"S": request_id}}
            )

            if "Item" not in response:
                raise ValueError(f"Request {request_id} not found in DynamoDB")

            item = response["Item"]
            status = item.get("status", {}).get("S", "UNKNOWN")

            if status == "COMPLETED":
                # Extract audit log
                audit_log = {}
                if "audit_log" in item:
                    audit_log = json.loads(item["audit_log"]["S"])

                return {
                    "status": "COMPLETED",
                    "request_id": request_id,
                    "audit_log": audit_log,
                    "completed_at": item.get("updated_at", {}).get("S", "")
                }
            elif status == "FAILED":
                error_message = item.get("error_message", {}).get("S", "Unknown error")
                raise RuntimeError(f"Erasure request failed: {error_message}")

            time.sleep(2)  # Poll every 2 seconds

        raise TimeoutError(f"Erasure request {request_id} did not complete within {timeout_seconds}s")

    def _get_lambda_metrics(self, start_time: datetime, end_time: datetime) -> Dict:
        """Get Lambda execution metrics from CloudWatch."""
        function_name = f"{self.environment_name}-erasure-handler"

        try:
            # Get invocation count
            invocations = self.cloudwatch.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=["Sum"]
            )

            # Get duration
            duration = self.cloudwatch.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Duration",
                Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=["Average", "Maximum"]
            )

            # Get errors
            errors = self.cloudwatch.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Errors",
                Dimensions=[{"Name": "FunctionName", "Value": function_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=["Sum"]
            )

            return {
                "invocations": sum([dp["Sum"] for dp in invocations.get("Datapoints", [])]),
                "avg_duration_ms": duration.get("Datapoints", [{}])[0].get("Average", 0) if duration.get("Datapoints") else 0,
                "max_duration_ms": duration.get("Datapoints", [{}])[0].get("Maximum", 0) if duration.get("Datapoints") else 0,
                "errors": sum([dp["Sum"] for dp in errors.get("Datapoints", [])])
            }
        except Exception as e:
            print(f"Warning: Could not retrieve Lambda metrics: {e}")
            return {}

    def _calculate_cost(self, audit_log: Dict, lambda_duration_ms: float) -> Dict:
        """Calculate cost breakdown for erasure operation."""
        costs = {
            "athena_scan": 0.0,
            "lambda_execution": 0.0,
            "s3_operations": 0.0,
            "dynamodb_writes": 0.0,
            "total": 0.0
        }

        # Athena cost (estimate based on partition count)
        # Each partition scan ~10MB, CTAS also scans data
        partition_count = len(audit_log.get("steps", [{}])[1].get("partitions", []))  if len(audit_log.get("steps", [])) > 1 else 0
        estimated_scan_gb = partition_count * 0.01  # 10MB per partition
        estimated_scan_tb = estimated_scan_gb / 1024
        costs["athena_scan"] = estimated_scan_tb * PRICING["athena_per_tb_scanned"] * 2  # Find + CTAS

        # Lambda cost (512MB memory)
        lambda_gb_seconds = (512 / 1024) * (lambda_duration_ms / 1000)
        costs["lambda_execution"] = (
            lambda_gb_seconds * PRICING["lambda_per_gb_second"] +
            PRICING["lambda_per_million_requests"] / 1_000_000
        )

        # S3 operations (PUTs for rewritten files)
        estimated_s3_puts = partition_count * 5  # ~5 files per partition
        costs["s3_operations"] = (estimated_s3_puts / 1000) * PRICING["s3_per_1k_put_requests"]

        # DynamoDB writes (2 writes per request: PROCESSING + COMPLETED)
        costs["dynamodb_writes"] = (2 / 1_000_000) * PRICING["dynamodb_per_million_write_units"]

        costs["total"] = sum(costs.values())

        return costs

    def benchmark_scenario(self, scenario_id: str) -> Dict:
        """Benchmark erasure for a specific scenario."""
        print(f"\n{'='*70}")
        print(f"Benchmarking Scenario {scenario_id}")
        print(f"{'='*70}")

        # Load manifest
        manifest = self._load_manifest(scenario_id)
        patients = manifest["patients"]

        print(f"Scenario: {manifest['scenario_name']}")
        print(f"Patients: {len(patients)}")
        print(f"Total partitions: {manifest['total_partitions']}")
        print()

        results = []

        for patient in patients[:1]:  # Benchmark first patient only for now
            patient_id_hash = patient["patient_id_hash"]
            partition_count = patient["partition_count"]

            print(f"Testing erasure for patient {patient['patient_id'][:20]}...")
            print(f"  Patient hash: {patient_id_hash[:16]}...")
            print(f"  Affected partitions: {partition_count}")

            # Start timing
            start_time = datetime.utcnow()
            start_timestamp = time.time()

            # Insert erasure request (triggers Lambda)
            request_id = self._insert_erasure_request(patient_id_hash)
            print(f"  Request ID: {request_id}")

            # Poll for completion
            try:
                result = self._poll_request_status(request_id, timeout_seconds=900)
                end_timestamp = time.time()
                end_time = datetime.utcnow()

                total_duration_seconds = end_timestamp - start_timestamp

                print(f"  ✓ Erasure completed in {total_duration_seconds:.1f}s")

                # Get Lambda metrics
                lambda_metrics = self._get_lambda_metrics(start_time, end_time)

                # Calculate costs
                costs = self._calculate_cost(result["audit_log"], lambda_metrics.get("avg_duration_ms", 0))

                # Parse step timings from audit log
                steps = result["audit_log"].get("steps", [])
                step_timings = {}
                for step in steps:
                    step_name = step.get("step", "unknown")
                    step_timings[step_name] = {
                        "completed_at": step.get("completed_at", ""),
                        "details": step.get("partitions_found") or step.get("partitions_rewritten") or step.get("rows_deleted", 0)
                    }

                results.append({
                    "patient_id": patient["patient_id"],
                    "patient_id_hash": patient_id_hash,
                    "request_id": request_id,
                    "total_duration_seconds": total_duration_seconds,
                    "partition_count": partition_count,
                    "status": "COMPLETED",
                    "step_timings": step_timings,
                    "lambda_metrics": lambda_metrics,
                    "costs": costs
                })

                print(f"  Cost estimate: ${costs['total']:.4f}")

            except Exception as e:
                print(f"  ✗ Erasure failed: {e}")
                results.append({
                    "patient_id": patient["patient_id"],
                    "patient_id_hash": patient_id_hash,
                    "request_id": request_id,
                    "status": "FAILED",
                    "error": str(e)
                })

        return {
            "scenario_id": scenario_id,
            "scenario_name": manifest["scenario_name"],
            "total_partitions": manifest["total_partitions"],
            "tested_patients": len(results),
            "results": results
        }


def main():
    parser = argparse.ArgumentParser(description="Benchmark GDPR erasure performance")
    parser.add_argument(
        "--scenario",
        "-s",
        choices=["A", "B", "C", "D", "E", "all"],
        default="A",
        help="Scenario to benchmark (A-E, or 'all')"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="erasure-benchmark-results.json",
        help="Output JSON file for results"
    )
    parser.add_argument(
        "--environment",
        "-e",
        default=ENVIRONMENT_NAME,
        help=f"Environment name (default: {ENVIRONMENT_NAME})"
    )

    args = parser.parse_args()

    # Initialize benchmark
    benchmark = ErasureBenchmark(environment_name=args.environment)

    # Run benchmarks
    if args.scenario == "all":
        scenarios_to_test = ["A", "B", "C", "D"]
    else:
        scenarios_to_test = [args.scenario]

    all_results = []
    for scenario_id in scenarios_to_test:
        result = benchmark.benchmark_scenario(scenario_id)
        all_results.append(result)

    # Save results
    output_data = {
        "benchmark_date": datetime.utcnow().isoformat() + "Z",
        "environment": args.environment,
        "region": REGION,
        "scenarios": all_results
    }

    with open(args.output, "w") as f:
        json.dump(output_data, f, indent=2)

    print(f"\n{'='*70}")
    print(f"Benchmark results saved to: {args.output}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
