import json
import io
import pytest
from lambda_function import lambda_handler, remove_duplicates

# Dummy response class to simulate requests responses.
class DummyResponse:
    def __init__(self, json_data, status_code, text=""):
        self._json_data = json_data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._json_data

# Dummy functions to override dependencies.
def dummy_get_api_key():
    return "dummy_api_key"

def dummy_requests_get(url):
    # Simulate a successful API call returning duplicate articles.
    dummy_data = {
        "articles": [
            {"title": "Article 1", "description": "Desc 1"},
            {"title": "Article 1", "description": "Desc 1"},
            {"title": "Article 2", "description": "Desc 2"}
        ]
    }
    return DummyResponse(dummy_data, 200)

def dummy_requests_get_failure(url):
    # Simulate an API call that fails (returns 500).
    dummy_data = {"error": "API failure"}
    return DummyResponse(dummy_data, 500, text="API failure")

def dummy_upload_to_s3(data):
    # Dummy upload function; simply print the data.
    print("Dummy upload_to_s3 called with data:")
    print(json.dumps(data, indent=2))
    return

def dummy_upload_to_s3_failure(data):
    # Simulate a failure in S3 upload by raising an exception.
    raise Exception("S3 Upload Failed: Simulated S3 failure")

# Test successful execution of news_etl_lambda.
def test_news_etl_lambda_success(monkeypatch):
    # Override get_api_key, requests.get, and upload_to_s3 in the lambda module.
    monkeypatch.setattr("lambda_function.get_api_key", dummy_get_api_key)
    monkeypatch.setattr("lambda_function.requests.get", dummy_requests_get)
    monkeypatch.setattr("lambda_function.upload_to_s3", dummy_upload_to_s3)

    # Create a dummy event.
    event = {"source": "manual_test"}

    # Call the lambda handler.
    response = lambda_handler(event, None)

    # Check that the response indicates success.
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert "News ETL completed" in body["message"]

# Test deduplication logic directly.
def test_remove_duplicates():
    sample_articles = [
        {"title": "A"},
        {"title": "A"},
        {"title": "B"}
    ]
    deduped = remove_duplicates(sample_articles)
    # Expect two unique articles.
    assert len(deduped) == 2
    titles = {article["title"] for article in deduped}
    assert titles == {"A", "B"}

# Test error handling when API call fails.
def test_news_etl_lambda_failure_api(monkeypatch):
    # Override get_api_key and set requests.get to simulate failure.
    monkeypatch.setattr("lambda_function.get_api_key", dummy_get_api_key)
    monkeypatch.setattr("lambda_function.requests.get", dummy_requests_get_failure)
    # Use the normal dummy upload function.
    monkeypatch.setattr("lambda_function.upload_to_s3", dummy_upload_to_s3)
    
    event = {"source": "manual_test"}
    response = lambda_handler(event, None)
    
    # Expect a failure response (status code 500).
    assert response["statusCode"] == 500
    body = json.loads(response["body"])
    # Check that the error message mentions the API failure.
    assert "API request failed" in body["error"]

# Test error handling when S3 upload fails.
def test_news_etl_lambda_failure_upload(monkeypatch):
    # Override get_api_key and requests.get with normal behavior.
    monkeypatch.setattr("lambda_function.get_api_key", dummy_get_api_key)
    monkeypatch.setattr("lambda_function.requests.get", dummy_requests_get)
    # Override upload_to_s3 to simulate an upload failure.
    monkeypatch.setattr("lambda_function.upload_to_s3", dummy_upload_to_s3_failure)

    event = {"source": "manual_test"}
    response = lambda_handler(event, None)

    # Expect a failure response (status code 500).
    assert response["statusCode"] == 500
    body = json.loads(response["body"])
    # Check that the error message includes "S3 Upload Failed".
    assert "S3 Upload Failed" in body["error"]

if __name__ == '__main__':
    import pytest
    pytest.main()
