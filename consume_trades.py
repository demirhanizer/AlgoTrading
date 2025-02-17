import asyncio
import json
import logging
from pymongo import MongoClient
import redis
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(
    filename="/app/logs/consume_trades.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

try:
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["trading_db"]
    collection = db["trades"]
    logger.info("✅ Successfully connected to MongoDB.")
except Exception as e:
    logger.error(f"❌ MongoDB Connection Failed: {e}")
    exit(1)

try:
    redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
    logger.info("✅ Successfully connected to Redis.")
except Exception as e:
    logger.error(f"❌ Redis Connection Failed: {e}")
    exit(1)

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "trade_data"
KAFKA_GROUP_ID = "trade_consumer_group"

consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "latest",
}

consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])


async def consume_trades():
    while True:
        try:
            msg = consumer.poll(1.0)

            if msg is None:
                await asyncio.sleep(1)
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logger.warning(f"⚠️ Reached end of partition: {msg.error()}")
                else:
                    logger.error(f"❌ Kafka Error: {msg.error()}")
                continue

            trade_data = json.loads(msg.value().decode("utf-8"))

            trade_record = {
                "symbol": trade_data["symbol"],
                "price": trade_data["price"],
                "quantity": trade_data["quantity"],
                "timestamp": trade_data["timestamp"]
            }

            collection.insert_one(trade_record)

            redis_client.publish("raw_trades", json.dumps(trade_record))

            logger.info(f"💾 Trade saved & published: {trade_record}")

        except Exception as e:
            logger.error(f"❌ Kafka Consumer Error: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    logger.info("🚀 Starting Kafka Consumer Service...")
    asyncio.run(consume_trades())
