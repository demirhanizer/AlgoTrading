import asyncio
import pandas as pd
from pymongo import MongoClient
import logging
import redis
import json
import time
from collections import deque
from bson import ObjectId
from datetime import timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/strategy.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

client = MongoClient("mongodb://mongodb:27017/")
db = client["trading_db"]
collection = db["trades"]
signals_collection = db["trade_signals"]

redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
REDIS_SIGNAL_KEY = "latest_trade_signal"
REDIS_PRICE_HISTORY = "price_history"

EXECUTION_MODE = "TIME_BASED"  # Options: "HFT", "TIME_BASED"
TIME_UNIT = "seconds"  # Options: "seconds", "minutes"
DATA_COLLECTION_MODE = "STRICT"  # Options: "STRICT", "FLEXIBLE"

# SMA Settings
SHORT_WINDOW = 50
LONG_WINDOW = 200

price_buffer = deque(maxlen=LONG_WINDOW)


def convert_mongo_document(doc):
    if isinstance(doc, dict):
        return {k: (str(v) if isinstance(v, ObjectId) else v) for k, v in doc.items()}
    return doc


async def fetch_price_data():
    try:
        if EXECUTION_MODE == "HFT":
            limit = 1000  # FOR HFT MODE
            cursor = collection.find({}, {"price": 1, "timestamp": 1}).sort("timestamp", -1).limit(limit)

        else:  # TIME_BASED strategy
            if TIME_UNIT == "seconds":
                time_range = LONG_WINDOW
            elif TIME_UNIT == "minutes":
                time_range = LONG_WINDOW * 60
            else:
                time_range = LONG_WINDOW * 60  # DEFAULT => MINUTES

            latest_timestamp_entry = collection.find_one({}, {"timestamp": 1}, sort=[("timestamp", -1)])

            if latest_timestamp_entry:
                latest_timestamp = latest_timestamp_entry["timestamp"]
                start_time = (pd.to_datetime(latest_timestamp) - timedelta(seconds=time_range)).isoformat()

                query = {"timestamp": {"$gte": start_time}}
                logger.info(f"🔍 Querying MongoDB with: {query}")

                cursor = collection.find(query, {"price": 1, "timestamp": 1}).sort("timestamp", 1)

            else:
                logger.warning("⚠️ No recent trade data found in MongoDB. Skipping execution.")
                return None

        data = [convert_mongo_document(doc) for doc in cursor]
        df = pd.DataFrame(data)

        if df.empty:
            logger.warning("⚠️ No trade data found in MongoDB. Skipping strategy execution.")
            return None

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")


        df.set_index("timestamp", inplace=True)
        df = df.resample(f"{1 if TIME_UNIT == 'seconds' else 60}S").ffill()  # FORWARD FILLING METHODOLOGY

        if len(df) < LONG_WINDOW:
            logger.warning(f"⚠️ Not enough data ({len(df)}/{LONG_WINDOW}). Waiting for more trades.")
            return None

        logger.info(f"📊 Successfully fetched {len(df)} price data points from MongoDB.")
        return df.reset_index()

    except Exception as e:
        logger.error(f"❌ Error fetching price data: {e}")
        return None


async def calculate_sma():
    if len(price_buffer) < LONG_WINDOW:
        logger.warning("⚠️ Not enough data for SMA calculation.")
        return None, None

    short_sma = sum(list(price_buffer)[-SHORT_WINDOW:]) / SHORT_WINDOW
    long_sma = sum(list(price_buffer)[-LONG_WINDOW:]) / LONG_WINDOW
    logger.info(f"📉 SMA Calculated - Short SMA: {short_sma}, Long SMA: {long_sma}")
    return short_sma, long_sma


async def generate_trade_signal(price, short_sma, long_sma, timestamp):
    last_signal = redis_client.get(REDIS_SIGNAL_KEY)
    last_signal = json.loads(last_signal) if last_signal else {}

    if short_sma > long_sma and last_signal.get("signal") != "BUY":
        logger.info(f"📈 BUY Signal Detected at {price} USDT")
        await store_signal("BUY", price, timestamp)

    elif short_sma < long_sma and last_signal.get("signal") != "SELL":
        logger.info(f"📉 SELL Signal Detected at {price} USDT")
        await store_signal("SELL", price, timestamp)


async def store_signal(signal, price, timestamp):
    signal_data = {
        "timestamp": str(timestamp),
        "signal": signal,
        "price": price,
        "status": "pending"
    }

    result = signals_collection.insert_one(signal_data)
    signal_data["_id"] = str(result.inserted_id)
    redis_client.set(REDIS_SIGNAL_KEY, json.dumps(signal_data))
    redis_client.publish("trade_signals", json.dumps(signal_data))
    logger.info(f"✅ Published Signal to Redis: {signal_data}")


async def process_new_trades():
    logger.info("🎧 Listening for new trade data...")
    pubsub = redis_client.pubsub()
    pubsub.subscribe("raw_trades")

    for message in pubsub.listen():
        if message["type"] == "message":
            try:
                trade_data = convert_mongo_document(json.loads(message["data"]))
                logger.info(f"📊 New Trade Received: {trade_data}") #exhaust logging file much
                await handle_trade(trade_data)

            except Exception as e:
                logger.error(f"⚠️ Error processing trade data: {e}")


async def handle_trade(trade_data):
    price = trade_data["price"]
    timestamp = trade_data["timestamp"]

    price_buffer.append(price)

    if len(price_buffer) >= LONG_WINDOW:
        short_sma, long_sma = await calculate_sma()
        await generate_trade_signal(price, short_sma, long_sma, timestamp)


async def run_sma_strategy():
    while True:
        df = await fetch_price_data()
        if df is not None and not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["interval"] = df["timestamp"].dt.floor("T" if TIME_UNIT == "minutes" else "S")
            grouped_df = df.groupby("interval")["price"].first().dropna()


            if DATA_COLLECTION_MODE == "STRICT":
                latest_time = grouped_df.index[-1]
                start_time = latest_time - timedelta(seconds=LONG_WINDOW - 1)

                while len(grouped_df[(grouped_df.index >= start_time)]) < LONG_WINDOW:
                    start_time -= timedelta(seconds=1)

                time_filtered_df = grouped_df[(grouped_df.index >= start_time)]
                logger.info(f"📌 STRICT Mode: Filtering data from {start_time} to {latest_time}. Found {len(time_filtered_df)} intervals.")

            elif DATA_COLLECTION_MODE == "FLEXIBLE":
                time_filtered_df = grouped_df.tail(max(SHORT_WINDOW, min(LONG_WINDOW, len(grouped_df))))
                logger.info(f"📌 FLEXIBLE Mode: Using last {len(time_filtered_df)} available intervals.")

            if len(time_filtered_df) >= LONG_WINDOW:
                price_buffer.clear()
                price_buffer.extend(time_filtered_df[-LONG_WINDOW:].values)
                short_sma, long_sma = await calculate_sma()
                if short_sma and long_sma:
                    await generate_trade_signal(time_filtered_df.iloc[-1], short_sma, long_sma, time_filtered_df.index[-1])
            else:
                logger.warning(f"⚠️ Not enough unique time points for SMA. Available: {len(time_filtered_df)}/{LONG_WINDOW}. Waiting for more data...")

        await asyncio.sleep(1)


if __name__ == "__main__":
    logger.info("🚀 Starting Strategy Process...")
    loop = asyncio.get_event_loop()
    if EXECUTION_MODE == "HFT":
        loop.create_task(process_new_trades())
    else:
        loop.create_task(run_sma_strategy())
    loop.run_forever()
