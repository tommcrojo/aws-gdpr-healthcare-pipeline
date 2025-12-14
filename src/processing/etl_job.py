"""
GDPR Healthcare Pipeline - ETL Job

Reads raw health records from S3, pseudonymizes patient IDs using
SHA256 with salt from Secrets Manager, validates data quality,
and writes to curated/quarantine buckets.
"""

import sys
import json
import hashlib
from datetime import datetime

import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# Configuration
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RAW_BUCKET',
    'CURATED_BUCKET',
    'QUARANTINE_BUCKET',
    'SECRET_ARN',
    'DATABASE_NAME',
    'TABLE_NAME',
    'KMS_KEY_ARN',
    'REDSHIFT_CONNECTION',
    'REDSHIFT_IAM_ROLE',
    'REDSHIFT_TEMP_DIR'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure S3 to use SSE-KMS encryption
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.enableServerSideEncryption", "true")
hadoop_conf.set("fs.s3.serverSideEncryption.kms.keyId", args['KMS_KEY_ARN'])

logger = glueContext.get_logger()


def get_salt_from_secrets_manager(secret_arn: str) -> str:
    """Retrieve hashing salt from AWS Secrets Manager."""
    client = boto3.client('secretsmanager', region_name='eu-central-1')
    response = client.get_secret_value(SecretId=secret_arn)
    secret_dict = json.loads(response['SecretString'])
    return secret_dict['salt']


def create_hash_udf(salt: str):
    """Create a UDF for SHA256 hashing with salt."""
    def hash_patient_id(patient_id: str) -> str:
        if patient_id is None:
            return None
        combined = f"{patient_id}{salt}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()

    return F.udf(hash_patient_id, StringType())


def load_to_redshift(df_curated, glue_context, year: str, month: str, day: str):
    """
    Load curated data to Redshift using native Glue JDBC connector.
    Uses preactions to ensure idempotency (delete-before-load).
    """
    curated_dyf = DynamicFrame.fromDF(df_curated, glue_context, "redshift_load")

    # Preaction: Delete existing partition data for idempotency
    preaction_sql = f"DELETE FROM patient_data.patient_vitals WHERE year='{year}' AND month='{month}' AND day='{day}'"

    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=curated_dyf,
        catalog_connection=args['REDSHIFT_CONNECTION'],
        connection_options={
            "dbtable": "patient_data.patient_vitals",
            "database": "healthcare_analytics",
            "preactions": preaction_sql
        },
        redshift_tmp_dir=args['REDSHIFT_TEMP_DIR'],
        transformation_ctx="redshift_sink"
    )


def main():
    logger.info("Starting ETL job")

    # Get salt from Secrets Manager
    logger.info("Retrieving salt from Secrets Manager")
    salt = get_salt_from_secrets_manager(args['SECRET_ARN'])
    hash_udf = create_hash_udf(salt)

    # Determine date partition to process (today's data)
    today = datetime.utcnow()
    year = today.strftime('%Y')
    month = today.strftime('%m')
    day = today.strftime('%d')

    raw_path = f"s3://{args['RAW_BUCKET']}/raw/year={year}/month={month}/day={day}/"
    logger.info(f"Reading from: {raw_path}")

    # Read raw JSON data
    try:
        raw_dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [raw_path],
                "recurse": True
            },
            format="json",
            format_options={
                "multiline": False
            },
            transformation_ctx="raw_dyf"
        )
    except Exception as e:
        logger.error(f"Error reading raw data: {str(e)}")
        job.commit()
        return

    record_count = raw_dyf.count()
    logger.info(f"Read {record_count} records from raw bucket")

    if record_count == 0:
        logger.info("No records to process")
        job.commit()
        return

    # Convert to DataFrame for transformations
    df = raw_dyf.toDF()

    # Flatten nested structure and convert timestamp to proper type
    df_flattened = df.select(
        F.col("record_id"),
        F.col("patient_id"),
        F.to_timestamp(F.col("timestamp")).alias("timestamp"),
        F.col("event_type"),
        F.col("data.heart_rate").alias("heart_rate"),
        F.col("data.blood_pressure_systolic").alias("blood_pressure_systolic"),
        F.col("data.blood_pressure_diastolic").alias("blood_pressure_diastolic"),
        F.col("data.temperature_celsius").alias("temperature_celsius"),
        F.col("data.oxygen_saturation").alias("oxygen_saturation"),
        F.col("metadata.source").alias("source"),
        F.col("metadata.version").alias("version"),
        F.col("metadata.is_test").alias("is_test")
    )

    # Add pseudonymized patient ID
    df_with_hash = df_flattened.withColumn(
        "patient_id_hash",
        hash_udf(F.col("patient_id"))
    )

    # Separate valid and quarantine records
    # Valid: heart_rate exists and is between 0-300
    valid_condition = (
        F.col("heart_rate").isNotNull() &
        (F.col("heart_rate") >= 0) &
        (F.col("heart_rate") <= 300)
    )

    df_valid = df_with_hash.filter(valid_condition)
    df_quarantine = df_with_hash.filter(~valid_condition)

    valid_count = df_valid.count()
    quarantine_count = df_quarantine.count()

    logger.info(f"Valid records: {valid_count}")
    logger.info(f"Quarantine records: {quarantine_count}")

    # Prepare curated data (remove original patient_id for privacy)
    df_curated = df_valid.drop("patient_id")

    # Add processing metadata
    df_curated = df_curated.withColumn(
        "processed_at",
        F.current_timestamp()
    ).withColumn(
        "year",
        F.lit(year)
    ).withColumn(
        "month",
        F.lit(month)
    ).withColumn(
        "day",
        F.lit(day)
    )

    # Add quarantine reason
    df_quarantine_with_reason = df_quarantine.withColumn(
        "quarantine_reason",
        F.when(F.col("heart_rate").isNull(), "missing_heart_rate")
         .when(F.col("heart_rate") < 0, "heart_rate_below_minimum")
         .when(F.col("heart_rate") > 300, "heart_rate_above_maximum")
         .otherwise("unknown")
    ).withColumn(
        "quarantined_at",
        F.lit(datetime.utcnow().isoformat())
    ).withColumn(
        "year",
        F.lit(year)
    ).withColumn(
        "month",
        F.lit(month)
    ).withColumn(
        "day",
        F.lit(day)
    )

    # Write curated data to S3 as Parquet
    if valid_count > 0:
        curated_path = f"s3://{args['CURATED_BUCKET']}/curated/"
        logger.info(f"Writing curated data to: {curated_path}")

        curated_dyf = DynamicFrame.fromDF(
            df_curated,
            glueContext,
            "curated_dyf"
        )

        glueContext.write_dynamic_frame.from_options(
            frame=curated_dyf,
            connection_type="s3",
            connection_options={
                "path": curated_path,
                "partitionKeys": ["year", "month", "day"]
            },
            format="parquet",
            format_options={
                "compression": "snappy"
            },
            transformation_ctx="curated_sink"
        )
        logger.info("Curated data written successfully")

        # Load to Redshift using native JDBC connector
        logger.info("Loading curated data to Redshift...")
        load_to_redshift(df_curated, glueContext, year, month, day)
        logger.info("Redshift load completed successfully")

    # Write quarantine data to S3 as Parquet
    if quarantine_count > 0:
        quarantine_path = f"s3://{args['QUARANTINE_BUCKET']}/quarantine/"
        logger.info(f"Writing quarantine data to: {quarantine_path}")

        quarantine_dyf = DynamicFrame.fromDF(
            df_quarantine_with_reason,
            glueContext,
            "quarantine_dyf"
        )

        glueContext.write_dynamic_frame.from_options(
            frame=quarantine_dyf,
            connection_type="s3",
            connection_options={
                "path": quarantine_path,
                "partitionKeys": ["year", "month", "day"]
            },
            format="parquet",
            format_options={
                "compression": "snappy"
            },
            transformation_ctx="quarantine_sink"
        )
        logger.info("Quarantine data written successfully")

    # Log summary
    logger.info("=" * 50)
    logger.info("ETL Job Summary")
    logger.info(f"  Total records processed: {record_count}")
    logger.info(f"  Valid records: {valid_count}")
    logger.info(f"  Quarantine records: {quarantine_count}")
    if record_count > 0:
        logger.info(f"  Validation rate: {valid_count/record_count*100:.2f}%")
    logger.info("=" * 50)

    job.commit()
    logger.info("ETL job completed successfully")


if __name__ == "__main__":
    main()
