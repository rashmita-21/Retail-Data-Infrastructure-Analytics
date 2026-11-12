# AWS Cloud Deployment Guide

> **Verification Status**: All AWS artifacts in `infrastructure/aws/` are prepared and reviewed as infrastructure-as-code and job definitions, **not deployed** to a live AWS account in this local environment (no cloud charges incurred).

## Prepared AWS Artifacts

| Component | File | Purpose |
| --- | --- | --- |
| CloudFormation Stack | [`retail_platform_stack.yaml`](../infrastructure/aws/cloudformation/retail_platform_stack.yaml) | Provisions encrypted S3 buckets (`raw`, `processed`, `rejected`, `curated`), Glue Data Catalog DB, Glue Crawler, Glue ETL Jobs, Redshift S3 Read Role, and Lambda IAM Role |
| Glue Job 1 (DQ) | [`bronze_to_silver_dq.py`](../infrastructure/aws/glue_jobs/bronze_to_silver_dq.py) | PySpark Glue 4.0 job reading Bronze CSV and writing Silver + Rejected Parquet |
| Glue Job 2 (ETL) | [`silver_to_gold_curated.py`](../infrastructure/aws/glue_jobs/silver_to_gold_curated.py) | PySpark Glue 4.0 job computing metrics, SHA-256 keys, and partitioned Gold Parquet |
| Lambda Trigger | [`handler.py`](../infrastructure/aws/lambda/ingestion_trigger/handler.py) | S3 `ObjectCreated` event handler launching `retail-bronze-to-silver-dq` |
| IAM Policies | [`infrastructure/aws/iam/`](../infrastructure/aws/iam/) | Least-privilege templates for Glue, Lambda, and Redshift |
| Redshift DDL & Load | [`01_create_schemas_and_tables.sql`](../infrastructure/aws/redshift/01_create_schemas_and_tables.sql), [`02_copy_from_s3.sql`](../infrastructure/aws/redshift/02_copy_from_s3.sql) | Redshift `DISTKEY`/`SORTKEY` star schema and idempotent `COPY` + `MERGE` load |

## Deployment Steps (When Authorized)

1. **Create a scripts bucket and upload Glue jobs**:
   ```bash
   aws s3 mb s3://<your-glue-scripts-bucket> --region us-east-1
   aws s3 cp infrastructure/aws/glue_jobs/ s3://<your-glue-scripts-bucket>/glue_jobs/ --recursive
   ```
2. **Deploy the CloudFormation stack**:
   ```bash
   aws cloudformation deploy \
     --template-file infrastructure/aws/cloudformation/retail_platform_stack.yaml \
     --stack-name retail-data-platform-dev \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides EnvironmentName=dev GlueScriptsBucket=<your-glue-scripts-bucket>
   ```
3. **Package and deploy the Lambda ingestion trigger**:
   Create the Lambda function from `infrastructure/aws/lambda/ingestion_trigger/handler.py` using the `IngestionLambdaRole` output, configure `S3_PROCESSED_BUCKET` and `S3_REJECTED_BUCKET` environment variables, and attach an S3 `ObjectCreated` notification on `batches/*.csv`.
4. **Provision Amazon Redshift Serverless (or Cluster) & initialize schema**:
   Execute `infrastructure/aws/redshift/01_create_schemas_and_tables.sql` in Redshift Query Editor v2, then load curated Parquet via `02_copy_from_s3.sql` using the `RedshiftS3ReadRoleArn` stack output.
5. **Upload historical batches to trigger ingestion**:
   ```bash
   aws s3 cp data/batches/ s3://<raw-bucket-name>/batches/ --recursive --exclude "*" --include "*.csv"
   ```

## Cost Considerations

- **Amazon S3**: ~$0.023/GB-month; lifecycle rule transitions raw objects to Glacier Instant Retrieval after 90 days and expires quarantine objects after 180 days.
- **AWS Glue 4.0**: $0.44 per DPU-hour (2 × `G.1X` workers = 2 DPUs = ~$0.88/hour while running; a ~2-minute batch run costs ~$0.03).
- **AWS Lambda**: Free tier covers 1M requests and 400,000 GB-seconds/month.
- **Amazon Redshift Serverless**: Charged per RPU-hour only while active (minimum 8 RPUs when serving queries); idle auto-pause prevents ongoing charges outside query windows.
- **Safety Reminder**: Always tear down the stack (`aws cloudformation delete-stack --stack-name retail-data-platform-dev`) and delete Redshift workgroups after testing to avoid recurring charges.
