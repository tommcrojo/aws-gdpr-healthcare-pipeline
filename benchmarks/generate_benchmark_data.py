#!/usr/bin/env python3
"""
GDPR Healthcare Pipeline - Benchmark Data Generator

Generates synthetic healthcare data at various scales for performance benchmarking.
Writes data directly to S3 raw bucket (bypassing Firehose for speed).
"""

import argparse
import boto3
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import Dict, List
import uuid

# AWS Configuration
REGION = "eu-central-1"
ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME", "gdpr-healthcare")

# Benchmark Scenarios
SCENARIOS = {
    "A": {
        "name": "Single Partition (Best Case)",
        "patients": 1,
        "days": 1,
        "records_per_day": 100,
        "description": "1 patient, 100 records, 1 day - minimal partition count"
    },
    "B": {
        "name": "Weekly Data (Typical)",
        "patients": 1,
        "days": 7,
        "records_per_day": 143,  # ~1K total records
        "description": "1 patient, 1K records, 7 days - typical use case"
    },
    "C": {
        "name": "Monthly Data (Moderate)",
        "patients": 1,
        "days": 30,
        "records_per_day": 333,  # ~10K total records
        "description": "1 patient, 10K records, 30 days - moderate partition count"
    },
    "D": {
        "name": "Yearly Data (Worst Case)",
        "patients": 1,
        "days": 365,
        "records_per_day": 274,  # ~100K total records
        "description": "1 patient, 100K records, 365 days - maximum partition count"
    },
    "E": {
        "name": "Concurrent Erasures",
        "patients": 5,
        "days": 30,
        "records_per_day": 200,  # ~30K total records (6K per patient)
        "description": "5 patients, 30K records, 30 days - parallel erasure testing"
    }
}


class BenchmarkDataGenerator:
    """Generates synthetic healthcare data for benchmarking."""

    def __init__(self, environment_name: str = ENVIRONMENT_NAME):
        self.environment_name = environment_name
        self.s3_client = boto3.client("s3", region_name=REGION)
        self.secrets_client = boto3.client("secretsmanager", region_name=REGION)
        self.cf_client = boto3.client("cloudformation", region_name=REGION)

        # Get stack outputs
        self.raw_bucket = self._get_stack_output("storage-ingestion", "RawBucketName")
        self.kms_key_id = self._get_stack_output("kms", "KmsKeyId")
        self.salt = self._get_salt()

        print(f"Initialized generator for environment: {environment_name}")
        print(f"Raw bucket: {self.raw_bucket}")
        print(f"KMS Key ID: {self.kms_key_id}")

    def _get_stack_output(self, stack_suffix: str, output_key: str) -> str:
        """Get output value from CloudFormation stack."""
        stack_name = f"{self.environment_name}-{stack_suffix}"
        response = self.cf_client.describe_stacks(StackName=stack_name)
        outputs = response["Stacks"][0].get("Outputs", [])
        for output in outputs:
            if output["OutputKey"] == output_key:
                return output["OutputValue"]
        raise ValueError(f"Output {output_key} not found in stack {stack_name}")

    def _get_salt(self) -> str:
        """Retrieve pseudonymization salt from Secrets Manager."""
        secret_name = f"{self.environment_name}-hashing-salt"
        response = self.secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response["SecretString"])
        return secret_data["salt"]

    def _generate_patient_id_hash(self, patient_id: str) -> str:
        """Generate pseudonymized patient ID hash using salt."""
        combined = f"{patient_id}{self.salt}"
        return hashlib.sha256(combined.encode()).hexdigest()

    def _generate_health_record(self, patient_id: str, timestamp: datetime) -> Dict:
        """Generate a single synthetic health record."""
        record_id = str(uuid.uuid4())

        return {
            "record_id": record_id,
            "patient_id": patient_id,
            "timestamp": timestamp.isoformat() + "Z",
            "event_type": "vitals_reading",
            "data": {
                "heart_rate": 60 + (hash(record_id) % 60),  # 60-120 bpm
                "blood_pressure_systolic": 100 + (hash(record_id + "sys") % 40),  # 100-140
                "blood_pressure_diastolic": 60 + (hash(record_id + "dia") % 30),  # 60-90
                "temperature_celsius": 36.0 + ((hash(record_id + "temp") % 20) / 10.0),  # 36.0-38.0
                "oxygen_saturation": 95 + (hash(record_id + "o2") % 5)  # 95-99%
            },
            "metadata": {
                "source": "benchmark_generator",
                "version": "1.0",
                "is_test": True,
                "scenario": "benchmark"
            }
        }

    def _write_partition_to_s3(self, date: datetime, records: List[Dict]) -> None:
        """Write records for a single partition to S3."""
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")

        # Write as JSON lines (one record per line)
        content = "\n".join(json.dumps(record) for record in records)

        # S3 key follows Firehose partition pattern
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        s3_key = f"raw/year={year}/month={month}/day={day}/benchmark-{timestamp}.json"

        self.s3_client.put_object(
            Bucket=self.raw_bucket,
            Key=s3_key,
            Body=content.encode("utf-8"),
            ContentType="application/json",
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=self.kms_key_id
        )

        print(f"  Wrote {len(records)} records to s3://{self.raw_bucket}/{s3_key}")

    def generate_scenario(self, scenario_id: str) -> Dict:
        """Generate data for a specific benchmark scenario."""
        scenario = SCENARIOS[scenario_id]
        print(f"\n{'='*70}")
        print(f"Generating Scenario {scenario_id}: {scenario['name']}")
        print(f"{'='*70}")
        print(f"Description: {scenario['description']}")
        print(f"Configuration:")
        print(f"  - Patients: {scenario['patients']}")
        print(f"  - Days: {scenario['days']}")
        print(f"  - Records/day: {scenario['records_per_day']}")
        print(f"  - Total records: {scenario['patients'] * scenario['days'] * scenario['records_per_day']}")
        print()

        # Generate patient IDs
        patient_ids = [f"BENCHMARK-{scenario_id}-PATIENT-{i:03d}" for i in range(1, scenario['patients'] + 1)]
        patient_hashes = {pid: self._generate_patient_id_hash(pid) for pid in patient_ids}

        # Generate data for each day
        start_date = datetime.utcnow() - timedelta(days=scenario['days'] - 1)
        total_records = 0
        partitions_created = 0

        for day_offset in range(scenario['days']):
            current_date = start_date + timedelta(days=day_offset)
            day_records = []

            # Generate records for each patient
            for patient_id in patient_ids:
                for record_num in range(scenario['records_per_day']):
                    # Spread records throughout the day
                    timestamp = current_date + timedelta(
                        hours=record_num * 24 // scenario['records_per_day'],
                        minutes=(hash(patient_id + str(record_num)) % 60)
                    )
                    record = self._generate_health_record(patient_id, timestamp)
                    day_records.append(record)

            # Write partition to S3
            self._write_partition_to_s3(current_date, day_records)
            total_records += len(day_records)
            partitions_created += 1

        # Generate manifest
        manifest = {
            "scenario_id": scenario_id,
            "scenario_name": scenario['name'],
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_records": total_records,
            "total_partitions": partitions_created,
            "patients": [
                {
                    "patient_id": pid,
                    "patient_id_hash": patient_hashes[pid],
                    "date_range": {
                        "start": start_date.strftime("%Y-%m-%d"),
                        "end": (start_date + timedelta(days=scenario['days'] - 1)).strftime("%Y-%m-%d")
                    },
                    "partition_count": partitions_created
                }
                for pid in patient_ids
            ]
        }

        # Write manifest to S3
        manifest_key = f"benchmark-manifests/scenario-{scenario_id}-manifest.json"
        self.s3_client.put_object(
            Bucket=self.raw_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=self.kms_key_id
        )

        print(f"\n✓ Scenario {scenario_id} generation complete:")
        print(f"  - Total records: {total_records}")
        print(f"  - Partitions created: {partitions_created}")
        print(f"  - Manifest: s3://{self.raw_bucket}/{manifest_key}")

        return manifest


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark data for GDPR pipeline")
    parser.add_argument(
        "--scenario",
        "-s",
        choices=list(SCENARIOS.keys()) + ["all"],
        default="all",
        help="Scenario to generate (A-E, or 'all')"
    )
    parser.add_argument(
        "--environment",
        "-e",
        default=ENVIRONMENT_NAME,
        help=f"Environment name (default: {ENVIRONMENT_NAME})"
    )

    args = parser.parse_args()

    # Initialize generator
    generator = BenchmarkDataGenerator(environment_name=args.environment)

    # Generate data
    if args.scenario == "all":
        scenarios_to_generate = list(SCENARIOS.keys())
    else:
        scenarios_to_generate = [args.scenario]

    manifests = {}
    for scenario_id in scenarios_to_generate:
        manifest = generator.generate_scenario(scenario_id)
        manifests[scenario_id] = manifest

    # Summary
    print(f"\n{'='*70}")
    print("GENERATION SUMMARY")
    print(f"{'='*70}")
    for scenario_id, manifest in manifests.items():
        print(f"Scenario {scenario_id}: {manifest['total_records']} records, {manifest['total_partitions']} partitions")
    print()


if __name__ == "__main__":
    main()
