import json
import boto3
import pandas as pd
import os
import io
import re
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3", region_name="eu-north-1")
sns = boto3.client("sns", region_name="eu-north-1")

DEST_BUCKET = os.environ.get("DEST_BUCKET", "csv-news-etl-bucket")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

def build_output_key(original_key: str) -> str:
    """Generate the output key for processed files."""
    cleaned_key = re.sub(r"^(raw/|processed/)+", "", original_key)
    return f"processed/{cleaned_key}"

def lambda_handler(event, context):
    """AWS Lambda handler function for processing CSV files."""
    try:
        logger.info("Event received: %s", json.dumps(event))

        # Extract bucket and key from the event
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        logger.info("Processing file: s3://%s/%s", bucket, key)

        # Download the CSV file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response["Body"].read().decode("utf-8")

        # Load into a pandas DataFrame
        try:
            df = pd.read_csv(io.StringIO(csv_content))
        except pd.errors.EmptyDataError:
            raise ValueError("CSV file is empty")

        if df.empty:
            raise ValueError("Invalid CSV format")

        logger.info("Original DataFrame:\n%s", df.head())

        # Deduplicate the data
        df_clean = df.drop_duplicates()
        logger.info("Cleaned DataFrame:\n%s", df_clean.head())

        # Create the correct output key
        output_key = build_output_key(key)
        logger.info("Output Key: %s", output_key)

        # Convert the cleaned DataFrame to CSV
        output_buffer = io.StringIO()
        df_clean.to_csv(output_buffer, index=False)

        # Upload to the destination bucket
        s3.put_object(
            Bucket=DEST_BUCKET,
            Key=output_key,
            Body=output_buffer.getvalue(),
            ContentType="text/csv"
        )
        logger.info("Uploaded cleaned file to: s3://%s/%s", DEST_BUCKET, output_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "File processed successfully",
                "output_key": output_key
            })
        }

    except Exception as e:
        error_message = str(e)
        logger.error("CSV ETL Lambda failed: %s", error_message)

        if SNS_TOPIC_ARN:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=error_message,
                Subject="ETL Pipeline Failed"
            )

        return {
            "statusCode": 500,
            "body": json.dumps({"error": error_message})
        }
