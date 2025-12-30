#!/usr/bin/env python3
"""
GDPR Healthcare Pipeline - ETL Performance Benchmark

Measures throughput and cost of the Glue ETL pipeline processing raw data
to curated Parquet and loading into Redshift.
"""

import argparse
import boto3
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict

# AWS Configuration
REGION = "eu-central-1"
ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME", "gdpr-healthcare")

# AWS Pricing (eu-central-1, December 2025)
PRICING = {
    "glue_per_dpu_hour": 0.44,
    "s3_per_gb_storage": 0.024,
    "s3_per_1k_put_requests": 0.005,
    "redshift_per_rpu_hour": 0.375  # Serverless base: 128 RPUs
}


class ETLBenchmark:
    """Benchmarks Glue ETL throughput and cost."""

    def __init__(self, environment_name: str = ENVIRONMENT_NAME):
        self.environment_name = environment_name
        self.glue = boto3.client("glue", region_name=REGION)
        self.s3 = boto3.client("s3", region_name=REGION)
        self.redshift_data = boto3.client("redshift-data", region_name=REGION)
        self.cloudwatch = boto3.client("cloudwatch", region_name=REGION)
        self.cf_client = boto3.client("cloudformation", region_name=REGION)

        # Get infrastructure details
        self.job_name = f"{environment_name}-etl-job"
        self.workgroup_name = f"{environment_name}-workgroup"
        self.database = "healthcare_analytics"
        self.raw_bucket = self._get_stack_output("storage-ingestion", "RawBucketName")
        self.curated_bucket = self._get_stack_output("storage-ingestion", "CuratedBucketName")

        print(f"Initialized ETL benchmark for environment: {environment_name}")
        print(f"Glue job: {self.job_name}")
        print(f"Raw bucket: {self.raw_bucket}")
        print(f"Curated bucket: {self.curated_bucket}")

    def _get_stack_output(self, stack_suffix: str, output_key: str) -> str:
        """Get output value from CloudFormation stack."""
        stack_name = f"{self.environment_name}-{stack_suffix}"
        response = self.cf_client.describe_stacks(StackName=stack_name)
        outputs = response["Stacks"][0].get("Outputs", [])
        for output in outputs:
            if output["OutputKey"] == output_key:
                return output["OutputValue"]
        raise ValueError(f"Output {output_key} not found in stack {stack_name}")

    def _count_raw_records(self, date_from: str, date_to: str) -> int:
        """Count records in raw bucket for date range."""
        # Simplified: List objects and count (actual would parse JSON files)
        # For accurate count, would need to download and count lines
        total_records = 0

        # Parse date strings
        start_date = datetime.fromisoformat(date_from)
        end_date = datetime.fromisoformat(date_to)

        days = (end_date - start_date).days + 1

        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            prefix = f"raw/year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/"

            try:
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.raw_bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        # Download and count lines
                        response = self.s3.get_object(Bucket=self.raw_bucket, Key=obj["Key"])
                        content = response["Body"].read().decode("utf-8")
                        lines = [line for line in content.split("\n") if line.strip()]
                        total_records += len(lines)
            except Exception as e:
                print(f"Warning: Could not count records for {prefix}: {e}")

        return total_records

    def _count_redshift_records(self) -> int:
        """Count total records in Redshift patient_vitals table."""
        sql = "SELECT COUNT(*) FROM patient_data.patient_vitals"

        response = self.redshift_data.execute_statement(
            WorkgroupName=self.workgroup_name,
            Database=self.database,
            Sql=sql
        )

        statement_id = response["Id"]

        # Poll for completion
        for _ in range(60):
            status_response = self.redshift_data.describe_statement(Id=statement_id)
            status = status_response["Status"]

            if status == "FINISHED":
                result = self.redshift_data.get_statement_result(Id=statement_id)
                count = int(result["Records"][0][0]["longValue"])
                return count
            elif status in ["FAILED", "ABORTED"]:
                raise RuntimeError(f"Redshift query failed: {status_response.get('Error', 'Unknown error')}")

            time.sleep(2)

        raise TimeoutError("Redshift query timed out")

    def _trigger_glue_job(self, date_to_process: str) -> str:
        """Trigger Glue ETL job for a specific date."""
        # Parse date
        date_obj = datetime.fromisoformat(date_to_process)

        response = self.glue.start_job_run(
            JobName=self.job_name,
            Arguments={
                "--YEAR": str(date_obj.year),
                "--MONTH": f"{date_obj.month:02d}",
                "--DAY": f"{date_obj.day:02d}",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true"
            }
        )

        return response["JobRunId"]

    def _wait_for_glue_job(self, job_run_id: str, timeout_seconds: int = 600) -> Dict:
        """Wait for Glue job to complete and return metrics."""
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            response = self.glue.get_job_run(
                JobName=self.job_name,
                RunId=job_run_id
            )

            job_run = response["JobRun"]
            state = job_run["JobRunState"]

            if state == "SUCCEEDED":
                execution_time_seconds = job_run.get("ExecutionTime", 0)
                dpu_seconds = execution_time_seconds * job_run.get("AllocatedCapacity", 10)

                started_on = job_run.get("StartedOn", "")
                completed_on = job_run.get("CompletedOn", "")

                return {
                    "status": "SUCCEEDED",
                    "execution_time_seconds": execution_time_seconds,
                    "allocated_dpus": job_run.get("AllocatedCapacity", 10),
                    "dpu_seconds": dpu_seconds,
                    "started_on": started_on.isoformat() if hasattr(started_on, 'isoformat') else str(started_on),
                    "completed_on": completed_on.isoformat() if hasattr(completed_on, 'isoformat') else str(completed_on)
                }
            elif state in ["FAILED", "ERROR", "TIMEOUT"]:
                error_message = job_run.get("ErrorMessage", "Unknown error")
                raise RuntimeError(f"Glue job failed: {error_message}")

            time.sleep(5)

        raise TimeoutError(f"Glue job {job_run_id} did not complete within {timeout_seconds}s")

    def _calculate_etl_cost(self, dpu_seconds: float, records_processed: int) -> Dict:
        """Calculate ETL cost breakdown."""
        dpu_hours = dpu_seconds / 3600

        costs = {
            "glue_processing": dpu_hours * PRICING["glue_per_dpu_hour"],
            "s3_storage": 0.0,  # Will be minimal for test data
            "s3_operations": (records_processed / 1000) * PRICING["s3_per_1k_put_requests"],
            "total": 0.0
        }

        costs["total"] = sum(costs.values())

        # Cost per 1M records
        cost_per_million = (costs["total"] / records_processed) * 1_000_000 if records_processed > 0 else 0

        return {
            **costs,
            "cost_per_million_records": cost_per_million
        }

    def benchmark_scenario(self, scenario_id: str) -> Dict:
        """Benchmark ETL for a specific scenario."""
        print(f"\n{'='*70}")
        print(f"Benchmarking ETL for Scenario {scenario_id}")
        print(f"{'='*70}")

        # Load manifest to get date range
        manifest_key = f"benchmark-manifests/scenario-{scenario_id}-manifest.json"
        response = self.s3.get_object(Bucket=self.raw_bucket, Key=manifest_key)
        manifest = json.loads(response["Body"].read().decode("utf-8"))

        print(f"Scenario: {manifest['scenario_name']}")
        print(f"Total records (expected): {manifest['total_records']}")

        # Get date range from first patient
        date_range = manifest["patients"][0]["date_range"]
        start_date = date_range["start"]
        end_date = date_range["end"]

        print(f"Date range: {start_date} to {end_date}")
        print()

        # Count records in raw bucket
        print("Counting raw records...")
        raw_record_count = self._count_raw_records(start_date, end_date)
        print(f"Raw records found: {raw_record_count}")

        # Get initial Redshift count
        print("Counting Redshift records (before)...")
        redshift_count_before = self._count_redshift_records()
        print(f"Redshift records (before): {redshift_count_before}")

        # Trigger Glue job for each day
        print(f"\nTriggering Glue ETL jobs...")
        job_runs = []

        start_date_obj = datetime.fromisoformat(start_date)
        end_date_obj = datetime.fromisoformat(end_date)
        days = (end_date_obj - start_date_obj).days + 1

        for day_offset in range(days):
            current_date = start_date_obj + timedelta(days=day_offset)
            date_str = current_date.isoformat()

            print(f"  Starting job for {date_str}...")
            job_run_id = self._trigger_glue_job(date_str)
            job_runs.append({"date": date_str, "job_run_id": job_run_id})

        # Wait for all jobs to complete
        print(f"\nWaiting for {len(job_runs)} Glue jobs to complete...")
        total_execution_time = 0
        total_dpu_seconds = 0

        for job_run in job_runs:
            print(f"  Waiting for job {job_run['job_run_id']}...")
            try:
                result = self._wait_for_glue_job(job_run["job_run_id"])
                job_run["result"] = result
                total_execution_time += result["execution_time_seconds"]
                total_dpu_seconds += result["dpu_seconds"]
                print(f"    ✓ Completed in {result['execution_time_seconds']}s")
            except Exception as e:
                print(f"    ✗ Failed: {e}")
                job_run["result"] = {"status": "FAILED", "error": str(e)}

        # Get final Redshift count
        print("\nCounting Redshift records (after)...")
        redshift_count_after = self._count_redshift_records()
        records_loaded = redshift_count_after - redshift_count_before
        print(f"Redshift records (after): {redshift_count_after}")
        print(f"Records loaded: {records_loaded}")

        # Calculate metrics
        throughput_records_per_second = records_loaded / total_execution_time if total_execution_time > 0 else 0
        costs = self._calculate_etl_cost(total_dpu_seconds, records_loaded)

        print(f"\nPerformance:")
        print(f"  Total execution time: {total_execution_time:.1f}s")
        print(f"  Throughput: {throughput_records_per_second:.1f} records/second")
        print(f"  Cost: ${costs['total']:.4f}")
        print(f"  Cost per 1M records: ${costs['cost_per_million_records']:.2f}")

        return {
            "scenario_id": scenario_id,
            "scenario_name": manifest["scenario_name"],
            "raw_record_count": raw_record_count,
            "records_loaded": records_loaded,
            "total_execution_time_seconds": total_execution_time,
            "total_dpu_seconds": total_dpu_seconds,
            "throughput_records_per_second": throughput_records_per_second,
            "costs": costs,
            "job_runs": job_runs
        }


def main():
    parser = argparse.ArgumentParser(description="Benchmark ETL pipeline performance")
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
        default="etl-benchmark-results.json",
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
    benchmark = ETLBenchmark(environment_name=args.environment)

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
    print(f"ETL benchmark results saved to: {args.output}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
