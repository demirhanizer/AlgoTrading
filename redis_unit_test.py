import redis
import pytest

@pytest.fixture
def redis_client():
    client = redis.Redis(host="redis", port=6379, db=0)
    yield client
    client.flushdb()  # Clean up Redis after test

def test_redis_connection(redis_client):
    assert redis_client.ping() is True

def test_redis_write_read(redis_client):
    redis_client.set("test_key", "test_value")
    value = redis_client.get("test_key")
    assert value.decode("utf-8") == "test_value"
