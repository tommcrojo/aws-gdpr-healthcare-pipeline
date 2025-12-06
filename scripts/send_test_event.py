#!/usr/bin/env python3
"""
Test script for validating Firehose delivery to S3.
Sends a sample health record to the Firehose delivery stream.

Usage:
    python send_test_event.py [--stream-name STREAM_NAME] [--region REGION]
"""

import argparse
import json
import uuid
from datetime import datetime

import boto3


def create_test_health_record():
    """Create a sample health record for testing."""
    return {
        "record_id": str(uuid.uuid4()),
        "patient_id": f"TEST-{uuid.uuid4().hex[:8].upper()}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event_type": "vitals_reading",
        "data": {
            "heart_rate": 72,
            "blood_pressure_systolic": 120,
            "blood_pressure_diastolic": 80,
            "temperature_celsius": 36.6,
            "oxygen_saturation": 98
        },
        "metadata": {
            "source": "test_script",
            "version": "1.0",
            "is_test": True
        }
    }


def send_to_firehose(stream_name: str, region: str) -> dict:
    """Send a test record to Firehose."""
    client = boto3.client("firehose", region_name=region)

    record = create_test_health_record()
    record_data = json.dumps(record) + "\n"

    print(f"Sending test record to stream: {stream_name}")
    print(f"Record ID: {record['record_id']}")
    print(f"Patient ID: {record['patient_id']}")

    response = client.put_record(
        DeliveryStreamName=stream_name,
        Record={"Data": record_data.encode("utf-8")}
    )

    return response


def main():
    parser = argparse.ArgumentParser(
        description="Send test health records to Firehose"
    )
    parser.add_argument(
        "--stream-name",
        default="gdpr-healthcare-delivery-stream",
        help="Firehose delivery stream name"
    )
    parser.add_argument(
        "--region",
        default="eu-central-1",
        help="AWS region"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of records to send"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("GDPR Healthcare Pipeline - Firehose Test")
    print("=" * 60)
    print()

    for i in range(args.count):
        response = send_to_firehose(args.stream_name, args.region)
        record_id = response.get("RecordId", "N/A")
        encrypted = response.get("Encrypted", False)

        print(f"[{i+1}/{args.count}] Record sent successfully!")
        print(f"  Firehose Record ID: {record_id}")
        print(f"  Encrypted: {encrypted}")
        print()

    print("=" * 60)
    print("Test complete!")
    print()
    print("Next steps:")
    print("  1. Wait up to 5 minutes for buffer to flush")
    print("  2. Check S3 bucket for the record:")
    print(f"     aws s3 ls s3://gdpr-healthcare-raw-<account-id>-{args.region}/raw/ --recursive")
    print("  3. Verify encryption:")
    print("     aws s3api head-object --bucket <bucket> --key <key>")
    print("=" * 60)


if __name__ == "__main__":
    main()
