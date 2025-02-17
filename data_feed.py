import asyncio
import websockets
import json
import logging
from pymongo import MongoClient
from datetime import datetime
import redis
from bson import ObjectId

logging.basicConfig(
    filename="/app/logs/data_feed.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

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


async def stream_data():
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as websocket:
                logger.info("📡 Connected to Binance WebSocket")
                while True:
                    response = await websocket.recv()
                    trade_data = json.loads(response)

                    trade_record = {
                        "symbol": trade_data["s"],
                        "price": float(trade_data["p"]),
                        "quantity": float(trade_data["q"]),
                        "timestamp": datetime.utcnow().isoformat()
                    }

                    inserted_trade = collection.insert_one(trade_record)

                    trade_record["_id"] = str(inserted_trade.inserted_id)

                    redis_client.publish("raw_trades", json.dumps(trade_record))

                    logger.info(f"💾 Trade saved & published: {trade_record}")

        except websockets.exceptions.ConnectionClosedError:
            logger.warning("⚠️ Connection lost... Reconnecting in 5 seconds")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            await asyncio.sleep(10)
if __name__ == "__main__":
    logger.info("🚀 Starting Data Feed Service...")
    asyncio.run(stream_data())
