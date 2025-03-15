import json
import pytest
from lambda_function import lambda_handler

# Dummy response class to simulate s3.get_object.
class DummyS3Response:
    def __init__(self, body):
        self._body = body

    def read(self):
        return self._body

# Dummy function to simulate s3.get_object returning an empty CSV file.
def dummy_get_object_empty(Bucket, Key):
    return {"Body": DummyS3Response("".encode("utf-8"))}

# Dummy function to simulate s3.get_object returning invalid CSV content.
def dummy_get_object_invalid_csv(Bucket, Key):
    return {"Body": DummyS3Response("invalid_data_without_headers".encode("utf-8"))}

# Dummy function to simulate s3.get_object failing due to a missing file.
def dummy_get_object_missing(Bucket, Key):
    raise Exception("S3 file not found")

# Dummy function to simulate s3.put_object.
def dummy_put_object(Bucket, Key, Body, ContentType):
    print(f"Dummy put_object called. Bucket: {Bucket}, Key: {Key}, ContentType: {ContentType}")

# Test case: Handling an empty CSV file.
def test_file_etl_lambda_empty_csv(monkeypatch):
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "csv-etl-bucket"},
                    "object": {"key": "raw/empty.csv"}
                }
            }
        ]
    }
    monkeypatch.setattr("lambda_function.s3.get_object", dummy_get_object_empty)
    monkeypatch.setattr("lambda_function.s3.put_object", dummy_put_object)

    response = lambda_handler(event, None)

    assert response["statusCode"] == 500
    body = json.loads(response["body"])
    assert "CSV file is empty" in body["error"]

# Test case: Handling an invalid CSV format.
def test_file_etl_lambda_invalid_csv(monkeypatch):
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "csv-etl-bucket"},
                    "object": {"key": "raw/invalid.csv"}
                }
            }
        ]
    }
    monkeypatch.setattr("lambda_function.s3.get_object", dummy_get_object_invalid_csv)
    monkeypatch.setattr("lambda_function.s3.put_object", dummy_put_object)

    response = lambda_handler(event, None)

    assert response["statusCode"] == 500
    body = json.loads(response["body"])
    assert "Invalid CSV format" in body["error"]

# Test case: Handling missing S3 file.
def test_file_etl_lambda_missing_file(monkeypatch):
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "csv-etl-bucket"},
                    "object": {"key": "raw/missing.csv"}
                }
            }
        ]
    }
    monkeypatch.setattr("lambda_function.s3.get_object", dummy_get_object_missing)
    monkeypatch.setattr("lambda_function.s3.put_object", dummy_put_object)

    response = lambda_handler(event, None)

    assert response["statusCode"] == 500
    body = json.loads(response["body"])
    assert "S3 file not found" in body["error"]

if __name__ == "__main__":
    pytest.main()
