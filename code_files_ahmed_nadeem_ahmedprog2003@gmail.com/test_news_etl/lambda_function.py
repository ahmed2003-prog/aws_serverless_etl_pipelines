import json
import boto3
import requests
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Fetching news...")

s3 = boto3.client('s3', region_name='eu-north-1')
ssm = boto3.client('ssm', region_name='eu-north-1')
sns = boto3.client('sns', region_name='eu-north-1')

# Environment Variables
NEWS_BUCKET = os.environ.get("NEWS_BUCKET", "news-etl-bucket")  # News bucket
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")  # SNS Alert Topic ARN
PARAMETER_NAME = os.environ.get("PARAMETER_NAME", "/news-api-key")  # API Key Parameter Store Name

def get_api_key():
    """Retrieve the API key from AWS Systems Manager Parameter Store."""
    try:
        response = ssm.get_parameter(Name=PARAMETER_NAME, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        send_alert(f"Failed to retrieve API key: {str(e)}")
        raise

def fetch_news():
    """Fetch top news articles from News API."""
    try:
        api_key = get_api_key()
        url = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            articles = response.json().get('articles', [])
            if not articles:
                raise ValueError("API request failed: No articles returned")
            return articles
        else:
            raise ValueError(f"API request failed: {response.text}")

    except Exception as e:
        error_message = f"News API Fetch Error: {str(e)}"
        send_alert(error_message)
        raise ValueError(error_message)

def remove_duplicates(articles):
    """Remove duplicate news articles based on the title."""
    seen_titles = set()
    unique_articles = []

    for article in articles:
        title = article.get('title', '').strip()
        if not title:
            logger.warning(f"Skipping article without title: {article}")
            continue

        if title not in seen_titles:
            seen_titles.add(title)
            unique_articles.append(article)

    return unique_articles

def upload_to_s3(data):
    """Upload news articles to S3 as a JSON file."""
    try:
        file_name = f"news_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        key = f"raw/{file_name}"

        s3.put_object(
            Bucket=NEWS_BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        logger.info(f"Uploaded {file_name} to {NEWS_BUCKET}/raw/")
    except Exception as e:
        send_alert(f"S3 Upload Failed: {str(e)}")
        raise

def send_alert(message):
    """Send SNS alert on failure."""
    logger.error(f"Error: {message}")
    if SNS_TOPIC_ARN:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject="News ETL Failure Alert 🚨")

def lambda_handler(event, context):
    """AWS Lambda handler for news ETL process."""
    try:
        logger.info("Fetching news...")
        news_data = fetch_news()

        if not news_data:
            raise Exception("News API returned no data")

        logger.info("Removing duplicates...")
        cleaned_data = remove_duplicates(news_data)

        logger.info("Uploading to S3...")
        upload_to_s3(cleaned_data)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "News ETL completed"})
        }
    except Exception as e:
        logger.info(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
