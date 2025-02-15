import asyncio
import pandas as pd
from pymongo import MongoClient
import logging
import redis
import json
import time
from collections import deque

# ✅ Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/strategy.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ✅ Database Connections
client = MongoClient("mongodb://mongodb:27017/")
db = client["trading_db"]
collection = db["trades"]
signals_collection = db["trade_signals"]

redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
REDIS_SIGNAL_KEY = "latest_trade_signal"
REDIS_PRICE_HISTORY = "price_history"
REDIS_LAST_ORDER_TIME = "last_order_time"  # ✅ Track last trade time

# ✅ Cooldown Period (Seconds)
COOLDOWN_PERIOD = 300  # 5 minutes

# ✅ Buffer for price history
price_buffer = deque(maxlen=1000)


async def fetch_price_data(limit=1000):
    """Fetches recent price data from MongoDB or Redis cache."""
    try:
        cursor = collection.find({}, {"_id": 0, "price": 1, "timestamp": 1}).sort("timestamp", -1).limit(limit)
        data = list(cursor)
        df = pd.DataFrame(data)

        if df.empty:
            logger.warning("⚠️ No trade data found in MongoDB.")
            return None

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        logger.info(f"📊 Fetched {len(df)} price data points from MongoDB.")

        return df
    except Exception as e:
        logger.error(f"❌ Error fetching price data: {e}")
        return None


async def calculate_sma(df, short_window=50, long_window=200):
    """Calculate Short-Term and Long-Term SMAs."""
    if df is None:
        logger.warning("⚠️ Skipping SMA calculation due to missing data.")
        return None

    df["SMA_short"] = df["price"].rolling(window=short_window, min_periods=short_window).mean()
    df["SMA_long"] = df["price"].rolling(window=long_window, min_periods=long_window).mean()
    logger.info("📉 SMA indicators calculated successfully.")
    return df


async def generate_trade_signal(df):
    """Generate trade signals when SMA crossover occurs with cooldown & duplicate checks."""
    if df is None or df.empty:
        logger.warning("⚠️ Skipping trade signal generation due to missing SMA data.")
        return None

    df["Signal"] = None
    last_signal = redis_client.get(REDIS_SIGNAL_KEY)
    last_signal = json.loads(last_signal) if last_signal else {}

    last_trade_time = redis_client.get(REDIS_LAST_ORDER_TIME)
    current_time = time.time()

    for i in range(1, len(df)):
        if pd.notna(df["SMA_short"].iloc[i]) and pd.notna(df["SMA_long"].iloc[i]):
            # ✅ Check if the signal is a crossover
            if df["SMA_short"].iloc[i] > df["SMA_long"].iloc[i] and df["SMA_short"].iloc[i - 1] <= df["SMA_long"].iloc[i - 1]:
                if last_signal and last_signal.get("signal") == "BUY":
                    logger.warning("🚫 Skipping duplicate BUY signal.")
                    continue
                if last_trade_time and (current_time - float(last_trade_time)) < COOLDOWN_PERIOD:
                    logger.warning("⚠️ Cooldown active, skipping BUY order.")
                    continue
                df.at[i, "Signal"] = "BUY"

            elif df["SMA_short"].iloc[i] < df["SMA_long"].iloc[i] and df["SMA_short"].iloc[i - 1] >= df["SMA_long"].iloc[i - 1]:
                if last_signal and last_signal.get("signal") == "SELL":
                    logger.warning("🚫 Skipping duplicate SELL signal.")
                    continue
                if last_trade_time and (current_time - float(last_trade_time)) < COOLDOWN_PERIOD:
                    logger.warning("⚠️ Cooldown active, skipping SELL order.")
                    continue
                df.at[i, "Signal"] = "SELL"

    logger.info(f"📌 Generated {df['Signal'].notnull().sum()} trade signals.")
    return df


async def store_signals(df):
    """Store new trade signals in MongoDB and Redis with cooldown tracking."""
    if df is None or df.empty:
        logger.warning("⚠️ No signals to store!")
        return

    new_signals = df[df["Signal"].notnull()]
    if not new_signals.empty:
        for _, row in new_signals.iterrows():
            existing_signal = signals_collection.find_one(
                {"signal": row["Signal"], "price": row["price"], "order_sent": False}
            )

            if existing_signal:
                logger.warning("🚫 Skipping duplicate signal, already pending execution.")
                continue  # Skip storing if the same signal is already waiting to be executed

            signal_data = {
                "timestamp": str(row["timestamp"]),
                "signal": row["Signal"],
                "price": row["price"],
                "order_sent": False
            }

            signals_collection.insert_one(signal_data)
            redis_client.set(REDIS_SIGNAL_KEY, json.dumps(signal_data))
            redis_client.set(REDIS_LAST_ORDER_TIME, str(time.time()))  # ✅ Track trade execution time
            redis_client.publish("trade_signals", json.dumps(signal_data))
            logger.info(f"✅ Published Signal to Redis: {signal_data}")


async def process_new_trades():
    """Continuously process new trades as they come in via Redis Pub/Sub."""
    logger.info("🎧 Listening for new trade data...")
    pubsub = redis_client.pubsub()
    pubsub.subscribe("raw_trades")

    for message in pubsub.listen():
        if message["type"] == "message":
            try:
                trade_data = json.loads(message["data"])
                logger.info(f"📊 New Trade Data Received: {trade_data}")

                price_buffer.append(trade_data)  # ✅ Append new trade to buffer

                redis_client.lpush(REDIS_PRICE_HISTORY, json.dumps(trade_data))
                redis_client.ltrim(REDIS_PRICE_HISTORY, 0, 999)  # Keep last 1000 trades

                df = pd.DataFrame(price_buffer)

                df = await calculate_sma(df, short_window=50, long_window=200)
                df = await generate_trade_signal(df)

                if df is not None:
                    await store_signals(df)

            except Exception as e:
                logger.error(f"⚠️ Error processing trade data: {e}")


if __name__ == "__main__":
    logger.info("🚀 Starting Strategy Process...")
    loop = asyncio.get_event_loop()
    loop.create_task(process_new_trades())
    loop.run_forever()
