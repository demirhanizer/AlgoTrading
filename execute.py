import json
import os
import time
from dotenv import load_dotenv
import ccxt
from pymongo import MongoClient
from logger import logger
import redis
from datetime import datetime, timedelta

load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
pubsub = redis_client.pubsub()
pubsub.subscribe("trade_signals")

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True
})

client = MongoClient("mongodb://mongodb:27017/")
db = client["trading_db"]
orders_collection = db.get_collection("trade_orders")
signals_collection = db.get_collection("trade_signals")

ORDER_COOLDOWN_SECONDS = 0

logger.info("📡 Trade Execution Service Started...")


def get_unsent_trade_signal():
    try:
        signal = signals_collection.find_one({"status": "pending"}, sort=[("timestamp", -1)])
        return signal
    except Exception as e:
        logger.error(f"❌ Error fetching pending trade signals: {e}")
        return None


def place_order(signal, price):
    symbol = "BTC/USDT"
    order_size = 0.0001
    sl_percent = 1  # Stop-Loss at 1%
    tp_percent = 2  # Take-Profit at 2%
    lock_key = f"trade_lock_{signal}_{price}"
    lock = redis_client.set(lock_key, "locked", ex=5, nx=True)
    if not lock:
        logger.warning(f"🚫 Skipping trade {signal} at {price}, already being processed by another instance.")
        return None
    try:
        last_trade = orders_collection.find_one({"side": signal}, sort=[("timestamp", -1)])
        if last_trade:
            last_trade_time = datetime.strptime(last_trade["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
            elapsed_time = datetime.utcnow() - last_trade_time
            if elapsed_time < timedelta(seconds=ORDER_COOLDOWN_SECONDS):
                logger.warning(f"🚫 Skipping trade: Cooldown active ({elapsed_time.seconds}s elapsed)")
                return None

        balance = exchange.fetch_balance()
        ticker = exchange.fetch_ticker(symbol)
        current_price_BTC = ticker['last']

        logger.info(f"🔍 BTC Price: {current_price_BTC}, BTC Balance: {balance['BTC']['free']}, USDT Balance: {balance['USDT']['free']}")

        if signal == "BUY":
            if balance["USDT"]['free'] > order_size * current_price_BTC:
                order = exchange.create_market_buy_order(symbol, order_size)
            else:
                logger.error(f"❌ INSUFFICIENT USDT: {balance['USDT']['free']}")
                return None
        elif signal == "SELL":
            if balance["BTC"]['free'] > order_size:
                order = exchange.create_market_sell_order(symbol, order_size)
            else:
                logger.error(f"❌ INSUFFICIENT BTC: {balance['BTC']['free']}")
                return None
        else:
            logger.warning("⚠️ Invalid trade signal detected.")
            return None

        stop_loss_price = price * (1 - sl_percent / 100) if signal == "BUY" else price * (1 + sl_percent / 100)
        take_profit_price = price * (1 + tp_percent / 100) if signal == "BUY" else price * (1 - tp_percent / 100)
        #exchange.create_order(symbol, 'LIMIT', signal, order_size, stop_loss_price) # commented since there is no sufficient balance in my current account => TODO: needs to be tested again
        #exchange.create_order(symbol, 'LIMIT', signal, order_size, take_profit_price) # commented since there is no sufficient balance in my current account => TODO: needs to be tested again

        trade_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "side": signal,
            "amount": order_size,
            "price": price,
            "stop_loss": stop_loss_price,
            "take_profit": take_profit_price,
            "status": "filled"
        }

        result = orders_collection.insert_one(trade_data)
        if result.inserted_id:
            logger.info(f"✅ Order successfully inserted with ID: {result.inserted_id}")
        else:
            logger.error("❌ Order insertion failed!")

        update_result = signals_collection.update_one(
            {"signal": signal, "price": price, "status": "pending"},
            {"$set": {"status": "filled"}}
        )

        if update_result.modified_count > 0:
            logger.info(f"✅ Trade signal updated successfully for signal: {signal} at price: {price}")
        else:
            logger.warning(f"⚠️ No updates were made (already marked as 'filled') for signal: {signal} at price: {price}.")

        trade_data["_id"] = str(result.inserted_id)
        redis_client.publish("trade_channel", json.dumps(trade_data))
        logger.info(f"✅ Trade Executed & Stored: {trade_data}")

        return order

    except Exception as e:
        logger.error(f"❌ Trade Execution Failed: {e}")
        return None
    finally:
        redis_client.delete(lock_key)


def listen_for_trade_signals():
    logger.info("🎧 Listening for trade signals...")

    while True:
        try:
            message = pubsub.get_message(ignore_subscribe_messages=True)
            if message:
                signal_data = json.loads(message["data"])
                signal_type = signal_data["signal"]
                signal_price = signal_data["price"]

                logger.info(f"📊 New Signal Received: {signal_type} at {signal_price} USDT")

                latest_signal = signals_collection.find_one({"signal": signal_type, "price": signal_price, "status": "pending"})
                if latest_signal:
                    place_order(signal_type, signal_price)
                else:
                    logger.info(f"🚫 Order already processed for signal: {signal_type} at {signal_price}")

            time.sleep(1)

        except Exception as e:
            logger.error(f"❌ Error listening for trade signals: {e}")
            time.sleep(5)


if __name__ == "__main__":
    logger.info("🚀 Trading bot started and will run continuously!")
    listen_for_trade_signals()
