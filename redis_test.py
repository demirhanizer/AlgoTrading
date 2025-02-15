import redis

try:
    redis_client = redis.Redis(host="redis", port=6379, db=0)
    redis_client.ping()
    print("✅ Redis is running and accessible!")
except redis.ConnectionError:
    print("❌ Redis connection failed!")


redis_client = redis.Redis(host="redis", port=6379, db=0)

# ✅ Write data to Redis
redis_client.set("test_key", "Hello, Redis!")

# ✅ Read data from Redis
value = redis_client.get("test_key")

if value:
    print(f"✅ Redis Read Test Passed! Value: {value.decode('utf-8')}")
else:
    print("❌ Redis Read Test Failed!")