from app.services.dynamodb import get_dynamodb_client, get_dynamodb_resource


def test_get_dynamodb_resource():
    resource = get_dynamodb_resource()
    assert resource is not None
    # Cached: calling again should return the same instance.
    assert resource is get_dynamodb_resource()


def test_get_dynamodb_client():
    client = get_dynamodb_client()
    assert client is not None
    assert client is get_dynamodb_client()
