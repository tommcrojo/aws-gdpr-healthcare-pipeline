# GDPR-Compliant Healthcare Data Pipeline 

AWS-native data pipeline processing 10M+ daily health records with GDPR Article 17 (Right to Erasure) compliance.

## Architecture

- **Ingestion**: Kinesis Firehose → S3 (raw)
- **Processing**: AWS Glue ETL → S3 (curated) + Redshift Serverless
- **Compliance**: Lambda-based erasure handler with Athena CTAS
- **Security**: Customer-managed KMS keys, VPC endpoints, pseudonymization

## Key Features

- ✅ GDPR Article 17 erasure within 2-minute SLA
- ✅ Privacy by Design (salted SHA-256 pseudonymization)
- ✅ Zero public internet traversal (VPC endpoints only)
- ✅ Full audit trail in DynamoDB and CloudWatch
- ✅ Infrastructure as Code (CloudFormation)

## Performance Benchmarks

| Metric | Small Batch (100 rec) | Production Scale (10M rec/day) | Status |
|--------|----------------------|--------------------------------|--------|
| **ETL Throughput** | 0.8 rec/s* | 8,500+ rec/s | ✅ |
| **Erasure SLA** | 12.9s (DB only) | ~65s (DB + S3) | ✅ |
| **Unit Cost** | $0.32 / 100 rec | $0.00005 / rec | ✅ |
| **Monthly Cost** | Dev: $0-24/mo | Prod: $3,000-4,200/mo | ✅ |

*\*Note: 0.8 rec/s reflects Glue's 120s cold start overhead. Actual processing velocity: 11 rec/s (scales to 8,500+ rec/s with production batch sizes).*

See [BENCHMARKS.md](BENCHMARKS.md) for detailed performance analysis.

## Deployment

```bash
./scripts/deploy.sh
```

## Testing

```bash
./scripts/run_tests.sh all
```

## Tech Stack

- **Compute**: AWS Lambda, AWS Glue, Redshift Serverless
- **Storage**: S3, DynamoDB
- **Query**: Amazon Athena
- **Security**: KMS, Secrets Manager, VPC
- **IaC**: CloudFormation
