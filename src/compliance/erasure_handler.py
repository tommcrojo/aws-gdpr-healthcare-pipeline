"""
GDPR Article 17 - Right to Erasure Handler

Triggered by DynamoDB Streams when an erasure request with status=APPROVED is inserted.
Performs three-step erasure:
  1. Find affected S3 partitions via Athena query
  2. Rewrite partitions using Athena CTAS (excluding target patient)
  3. Delete records from Redshift via Data API
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

import boto3
from botocore.exceptions import ClientError

# Configuration from environment
ENVIRONMENT_NAME = os.environ.get('ENVIRONMENT_NAME', 'gdpr-healthcare')
CURATED_BUCKET = os.environ.get('CURATED_BUCKET', '')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', '')
GLUE_TABLE = os.environ.get('GLUE_TABLE', 'curated_health_records')
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', '')
REDSHIFT_WORKGROUP = os.environ.get('REDSHIFT_WORKGROUP', '')
REDSHIFT_DATABASE = os.environ.get('REDSHIFT_DATABASE', 'healthcare_analytics')
REQUESTS_TABLE = os.environ.get('REQUESTS_TABLE', '')

# AWS clients (initialized lazily for Lambda cold start optimization)
_athena = None
_redshift_data = None
_dynamodb = None
_s3 = None
_glue = None
_cloudwatch = None


def get_athena():
    global _athena
    if _athena is None:
        _athena = boto3.client('athena', region_name='eu-central-1')
    return _athena


def get_redshift_data():
    global _redshift_data
    if _redshift_data is None:
        _redshift_data = boto3.client('redshift-data', region_name='eu-central-1')
    return _redshift_data


def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
    return _dynamodb


def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client('s3', region_name='eu-central-1')
    return _s3


def get_glue():
    global _glue
    if _glue is None:
        _glue = boto3.client('glue', region_name='eu-central-1')
    return _glue


def get_cloudwatch():
    global _cloudwatch
    if _cloudwatch is None:
        _cloudwatch = boto3.client('cloudwatch', region_name='eu-central-1')
    return _cloudwatch


# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ErasureError(Exception):
    """Custom exception for erasure operations."""
    pass


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler - processes DynamoDB Stream events.

    Expected event structure:
    {
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "request_id": {"S": "..."},
                    "patient_id_hash": {"S": "..."},
                    "status": {"S": "APPROVED"}
                }
            }
        }]
    }
    """
    logger.info(f"Received event with {len(event.get('Records', []))} records")

    results = []
    for record in event.get('Records', []):
        if record.get('eventName') != 'INSERT':
            continue

        new_image = record.get('dynamodb', {}).get('NewImage', {})
        status = new_image.get('status', {}).get('S')

        if status != 'APPROVED':
            logger.info(f"Skipping non-APPROVED record: status={status}")
            continue

        request_id = new_image.get('request_id', {}).get('S')
        patient_id_hash = new_image.get('patient_id_hash', {}).get('S')

        if not request_id or not patient_id_hash:
            logger.error(f"Missing required fields: request_id={request_id}, "
                        f"patient_id_hash={'present' if patient_id_hash else 'missing'}")
            continue

        try:
            result = process_erasure_request(request_id, patient_id_hash)
            results.append(result)
            emit_metric('ErasureRequestsProcessed', 1)
        except Exception as e:
            logger.error(f"Failed to process request {request_id}: {str(e)}")
            update_request_status(request_id, 'FAILED', error_message=str(e))
            results.append({
                'request_id': request_id,
                'status': 'FAILED',
                'error': str(e)
            })
            emit_metric('ErasureFailures', 1)

    return {'results': results}


def process_erasure_request(request_id: str, patient_id_hash: str) -> Dict[str, Any]:
    """
    Process a single erasure request through all three steps.
    """
    start_time = time.time()
    logger.info(f"Processing erasure request: {request_id} for patient_hash: "
                f"{patient_id_hash[:16]}...")

    # Update status to PROCESSING
    update_request_status(request_id, 'PROCESSING')

    audit_log = {
        'request_id': request_id,
        'started_at': datetime.utcnow().isoformat(),
        'steps': []
    }

    try:
        # Step A: Find affected S3 partitions
        logger.info("Step A: Finding affected partitions...")
        affected_partitions = find_affected_partitions(patient_id_hash)
        audit_log['steps'].append({
            'step': 'find_partitions',
            'partitions_found': len(affected_partitions),
            'partitions': affected_partitions,
            'completed_at': datetime.utcnow().isoformat()
        })

        if not affected_partitions:
            logger.info("No data found for patient in S3/Athena")
        else:
            # Step B: Rewrite partitions without target patient
            logger.info(f"Step B: Rewriting {len(affected_partitions)} partitions...")
            rewrite_results = rewrite_partitions(patient_id_hash, affected_partitions)
            audit_log['steps'].append({
                'step': 'rewrite_partitions',
                'partitions_rewritten': len(affected_partitions),
                'details': rewrite_results,
                'completed_at': datetime.utcnow().isoformat()
            })
            emit_metric('PartitionsRewritten', len(affected_partitions))

        # Step C: Delete from Redshift
        logger.info("Step C: Deleting from Redshift...")
        redshift_rows_deleted = delete_from_redshift(patient_id_hash)
        audit_log['steps'].append({
            'step': 'redshift_delete',
            'rows_deleted': redshift_rows_deleted,
            'completed_at': datetime.utcnow().isoformat()
        })

        # Calculate duration and update status to COMPLETED
        duration = time.time() - start_time
        audit_log['completed_at'] = datetime.utcnow().isoformat()
        audit_log['duration_seconds'] = round(duration, 2)
        update_request_status(request_id, 'COMPLETED', audit_log=audit_log)

        emit_metric('ErasureDuration', duration, 'Seconds')
        logger.info(f"Erasure request {request_id} completed successfully in {duration:.2f}s")

        return {
            'request_id': request_id,
            'status': 'COMPLETED',
            'duration_seconds': round(duration, 2),
            'partitions_affected': len(affected_partitions),
            'redshift_rows_deleted': redshift_rows_deleted
        }

    except Exception as e:
        audit_log['error'] = str(e)
        audit_log['failed_at'] = datetime.utcnow().isoformat()
        raise


def find_affected_partitions(patient_id_hash: str) -> List[Dict[str, str]]:
    """
    Step A: Query Athena to find S3 partitions containing the patient's data.

    Returns list of partition info: [{'year': '2025', 'month': '01', 'day': '15'}, ...]
    """
    query = f"""
    SELECT DISTINCT year, month, day
    FROM "{GLUE_DATABASE}"."{GLUE_TABLE}"
    WHERE patient_id_hash = '{patient_id_hash}'
    """

    execution_id = execute_athena_query(query)
    results = wait_for_athena_results(execution_id)

    partitions = []
    for row in results:
        if len(row) >= 3:
            partitions.append({
                'year': row[0],
                'month': row[1],
                'day': row[2]
            })

    logger.info(f"Found {len(partitions)} affected partitions")
    return partitions


def rewrite_partitions(
    patient_id_hash: str,
    partitions: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Step B: For each affected partition, use Athena CTAS to rewrite
    the partition data excluding the target patient.
    """
    results = []

    for partition in partitions:
        year, month, day = partition['year'], partition['month'], partition['day']
        logger.info(f"Rewriting partition: year={year}, month={month}, day={day}")

        partition_result = {'partition': f"year={year}/month={month}/day={day}"}

        try:
            # Create temp table with data excluding target patient
            timestamp = int(time.time() * 1000)
            temp_table_name = f"temp_erasure_{year}_{month}_{day}_{timestamp}"
            temp_location = f"s3://{CURATED_BUCKET}/temp-erasure/{temp_table_name}/"

            ctas_query = f"""
            CREATE TABLE "{GLUE_DATABASE}"."{temp_table_name}"
            WITH (
                format = 'PARQUET',
                external_location = '{temp_location}',
                parquet_compression = 'SNAPPY'
            ) AS
            SELECT *
            FROM "{GLUE_DATABASE}"."{GLUE_TABLE}"
            WHERE year = '{year}' AND month = '{month}' AND day = '{day}'
              AND patient_id_hash != '{patient_id_hash}'
            """

            execution_id = execute_athena_query(ctas_query)
            wait_for_athena_completion(execution_id)

            # Delete original partition data from S3
            original_prefix = f"curated/year={year}/month={month}/day={day}/"
            deleted_count = delete_s3_prefix(CURATED_BUCKET, original_prefix)
            partition_result['original_files_deleted'] = deleted_count

            # Move temp data to original partition location
            moved_count = move_s3_data(
                source_bucket=CURATED_BUCKET,
                source_prefix=f"temp-erasure/{temp_table_name}/",
                dest_bucket=CURATED_BUCKET,
                dest_prefix=original_prefix
            )
            partition_result['new_files_created'] = moved_count

            # Clean up temp table from Glue catalog
            cleanup_temp_table(temp_table_name)

            partition_result['status'] = 'success'
            logger.info(f"Partition rewritten successfully: year={year}, month={month}, "
                       f"day={day}")

        except Exception as e:
            partition_result['status'] = 'failed'
            partition_result['error'] = str(e)
            logger.error(f"Failed to rewrite partition {partition}: {e}")
            raise ErasureError(f"Partition rewrite failed for {partition}: {e}")

        results.append(partition_result)

    return results


def delete_from_redshift(patient_id_hash: str) -> int:
    """
    Step C: Delete all records for the patient from Redshift using Data API.
    Returns the number of rows deleted.
    """
    delete_sql = f"""
    DELETE FROM patient_data.patient_vitals
    WHERE patient_id_hash = '{patient_id_hash}'
    """

    redshift_data = get_redshift_data()

    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=delete_sql
    )

    statement_id = response['Id']
    logger.info(f"Redshift delete statement submitted: {statement_id}")

    # Wait for completion with timeout
    timeout = 120
    start_time = time.time()

    while time.time() - start_time < timeout:
        status_response = redshift_data.describe_statement(Id=statement_id)
        status = status_response['Status']

        if status == 'FINISHED':
            rows_deleted = status_response.get('ResultRows', 0)
            logger.info(f"Redshift delete completed: {rows_deleted} rows affected")
            return rows_deleted
        elif status in ['FAILED', 'ABORTED']:
            error = status_response.get('Error', 'Unknown error')
            raise ErasureError(f"Redshift delete failed: {error}")

        time.sleep(2)

    raise ErasureError(f"Redshift delete timed out after {timeout} seconds")


def execute_athena_query(query: str) -> str:
    """Execute an Athena query and return the execution ID."""
    athena = get_athena()

    response = athena.start_query_execution(
        QueryString=query,
        WorkGroup=ATHENA_WORKGROUP
    )

    execution_id = response['QueryExecutionId']
    logger.info(f"Started Athena query: {execution_id}")
    return execution_id


def wait_for_athena_completion(execution_id: str, timeout: int = 300) -> str:
    """Wait for Athena query to complete."""
    athena = get_athena()
    start_time = time.time()

    while time.time() - start_time < timeout:
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        state = response['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            return state
        elif state in ['FAILED', 'CANCELLED']:
            reason = response['QueryExecution']['Status'].get(
                'StateChangeReason', 'Unknown'
            )
            raise ErasureError(f"Athena query {state}: {reason}")

        time.sleep(2)

    raise ErasureError(f"Athena query timeout after {timeout} seconds")


def wait_for_athena_results(execution_id: str) -> List[List[str]]:
    """Wait for query completion and return results."""
    wait_for_athena_completion(execution_id)

    athena = get_athena()
    results = []
    paginator = athena.get_paginator('get_query_results')

    for page in paginator.paginate(QueryExecutionId=execution_id):
        rows = page['ResultSet']['Rows']
        # Skip header row (first row in first page)
        start_idx = 1 if not results else 0
        for row in rows[start_idx:]:
            results.append([
                col.get('VarCharValue', '') for col in row.get('Data', [])
            ])

    return results


def delete_s3_prefix(bucket: str, prefix: str) -> int:
    """Delete all objects under an S3 prefix. Returns count of deleted objects."""
    s3 = get_s3()
    paginator = s3.get_paginator('list_objects_v2')
    deleted_count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get('Contents', [])
        if objects:
            delete_keys = [{'Key': obj['Key']} for obj in objects]
            s3.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
            deleted_count += len(delete_keys)
            logger.info(f"Deleted {len(delete_keys)} objects from s3://{bucket}/{prefix}")

    return deleted_count


def move_s3_data(
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_prefix: str
) -> int:
    """Move S3 data from source to destination prefix. Returns count of moved objects."""
    s3 = get_s3()
    paginator = s3.get_paginator('list_objects_v2')
    moved_count = 0

    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
        for obj in page.get('Contents', []):
            source_key = obj['Key']
            # Replace source prefix with destination prefix
            relative_key = source_key[len(source_prefix):]
            dest_key = dest_prefix + relative_key

            # Copy to destination
            s3.copy_object(
                Bucket=dest_bucket,
                Key=dest_key,
                CopySource={'Bucket': source_bucket, 'Key': source_key}
            )

            # Delete from source
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            moved_count += 1

    logger.info(f"Moved {moved_count} objects from {source_prefix} to {dest_prefix}")
    return moved_count


def cleanup_temp_table(table_name: str) -> None:
    """Remove temporary table from Glue catalog."""
    glue = get_glue()

    try:
        glue.delete_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        logger.info(f"Deleted temp table: {table_name}")
    except ClientError as e:
        logger.warning(f"Could not delete temp table {table_name}: {e}")


def update_request_status(
    request_id: str,
    status: str,
    error_message: Optional[str] = None,
    audit_log: Optional[Dict] = None
) -> None:
    """Update the status of an erasure request in DynamoDB."""
    dynamodb = get_dynamodb()
    table = dynamodb.Table(REQUESTS_TABLE)

    update_expr = "SET #status = :status, updated_at = :updated_at"
    expr_values = {
        ':status': status,
        ':updated_at': datetime.utcnow().isoformat()
    }
    expr_names = {'#status': 'status'}

    if status == 'COMPLETED':
        update_expr += ", completed_at = :completed_at"
        expr_values[':completed_at'] = datetime.utcnow().isoformat()

    if error_message:
        update_expr += ", error_message = :error"
        expr_values[':error'] = error_message

    if audit_log:
        update_expr += ", audit_log = :audit"
        expr_values[':audit'] = json.dumps(audit_log)

    table.update_item(
        Key={'request_id': request_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values
    )

    logger.info(f"Updated request {request_id} status to {status}")


def emit_metric(metric_name: str, value: float, unit: str = 'Count') -> None:
    """Emit a CloudWatch metric for monitoring."""
    try:
        cloudwatch = get_cloudwatch()
        cloudwatch.put_metric_data(
            Namespace='GDPR/Erasure',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Dimensions': [
                    {'Name': 'Environment', 'Value': ENVIRONMENT_NAME}
                ]
            }]
        )
    except Exception as e:
        # Don't fail erasure if metrics fail
        logger.warning(f"Failed to emit metric {metric_name}: {e}")
