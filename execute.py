import json
import os
import time
import schedule
from dotenv import load_dotenv
import ccxt
from pymongo import MongoClient
from logger import logger
import redis
from datetime import datetime, timedelta



redis_client = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
pubsub = redis_client.pubsub()
pubsub.subscribe("trade_signals")

# ✅ Ensure log directory exists
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)

# ✅ Load API Keys
load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

# ✅ Connect to Binance API
exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True
})

ORDER_COOLDOWN_SECONDS = 60

# ✅ Connect to MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client["trading_db"]
orders_collection = db.get_collection("trade_orders")
signals_collection = db.get_collection("trade_signals")

logger.info("📡 Trade Execution Service Started...")


def listen_for_trade_signals():
    logger.info("🎧 Listening for trade signals...")

    for message in pubsub.listen():
        if message["type"] == "message":
            try:
                signal_data = json.loads(message["data"])
                signal_type = signal_data["signal"]
                signal_price = signal_data["price"]

                logger.info(f"📊 New Signal Received: {signal_type} at {signal_price} USDT")
                place_order(signal_type, signal_price)
            except Exception as e:
                logger.error(f"⚠️ Error processing trade signal: {e}")


def get_latest_trade_signal():
    """Fetch the most recent trade signal from MongoDB."""
    try:
        latest_signal = redis_client.get("latest_trade_signal")
        if latest_signal:
            signal_data = json.loads(latest_signal)
            logger.info(f"🔍 Latest Signal Found: {signal_data}")
            return signal_data
        else:
            logger.warning("⚠️ No trade signals available.")
            return None
    except Exception as e:
        logger.error(f"❌ Error fetching trade signals: {e}")
        return None

def place_order(signal, price):
    """Places a trade and stores it in MongoDB."""
    symbol = "BTC/USDT"
    order_size = 0.0001
    sl_percent = 1  # Stop-Loss at 1%
    tp_percent = 2  # Take-Profit at 2%

    try:

        last_trade = orders_collection.find_one(
            {"side": signal},
            sort=[("timestamp", -1)]
        )

        if last_trade:
            last_trade_time = datetime.strptime(last_trade["timestamp"], "%Y-%m-%d %H:%M:%S")
            elapsed_time = datetime.utcnow() - last_trade_time

            if elapsed_time < timedelta(seconds=ORDER_COOLDOWN_SECONDS):
                logger.warning(f"🚫 Skipping trade: Cooldown active ({elapsed_time.seconds}s elapsed)")
                return None

        existing_signal = signals_collection.find_one(
            {"signal": signal, "price": price}
        )

        if existing_signal and existing_signal.get("order_sent"):
            logger.info(f"🚫 Order already sent: {existing_signal}")
            return None  # ✅ Skip duplicate order execution
        # ✅ Execute Market Order
        balance = exchange.fetch_balance()
        ticker = exchange.fetch_ticker('BTC/USDT')
        current_price_BTC = ticker['last']
        logger.info(str(current_price_BTC))
        logger.info(str(balance["BTC"]))
        logger.info(str(balance["USDT"]['free']))

        if signal == "BUY":

            if balance["USDT"]['free'] > order_size * current_price_BTC:
                order = exchange.create_market_buy_order(symbol, order_size)
                logger.info(str(balance["USDT"]['free']) + " - " + str(order_size * current_price_BTC))
            else:
                logger.error(f'INSUFFICIENT USDT {balance["USDT"]}')
                return None
        elif signal == "SELL":
            if balance["BTC"]['free'] > order_size:
                logger.info(str(balance["BTC"]['free']) + " - " + str(order_size))
                order = exchange.create_market_sell_order(symbol, order_size)
            else:
                logger.error(f'INSUFFICIENT BTC {balance["BTC"]}')
                return None
        else:
            logger.warning("⚠️ Invalid trade signal detected.")
            return None

        # ✅ Calculate SL & TP Prices
        stop_loss_price = price * (1 - sl_percent / 100) if signal == "BUY" else price * (1 + sl_percent / 100)
        take_profit_price = price * (1 + tp_percent / 100) if signal == "BUY" else price * (1 - tp_percent / 100)

        # ✅ Use LIMIT orders instead of STOP_MARKET (Fixes Binance error)
        exchange.create_order(symbol, 'LIMIT', signal, order_size, stop_loss_price)
        exchange.create_order(symbol, 'LIMIT', signal, order_size, take_profit_price)

        # ✅ Store trade in MongoDB
        trade_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "side": signal,
            "amount": order_size,
            "price": price,
            "stop_loss": stop_loss_price,
            "take_profit": take_profit_price,
            "status": "filled",
            "order_sent": True
        }

        orders_collection.insert_one(trade_data)
        signals_collection.update_one(
            {"signal": signal, "price": price},
            {"$set": {"order_sent": True}}
        )
        redis_client.publish("trade_channel", json.dumps(trade_data))

        logger.info(f"✅ Trade Executed & Stored: {trade_data}")

        return order

    except Exception as e:
        logger.error(f"❌ Trade Execution Failed: {e}")
        return None


def check_for_trade_signals():
    logger.info("📡 Checking for the latest trade signal...")

    latest_signal = get_latest_trade_signal()

    if latest_signal:
        signal_type = latest_signal["signal"]
        signal_price = latest_signal["price"]

        logger.info(f"📊 Latest Signal: {signal_type} at {signal_price} USDT")
        place_order(signal_type, signal_price)
    else:
        logger.warning("⚠️ No trade signals found.")

    logger.info("✅ Trade execution cycle completed.")

if __name__ == "__main__":
    logger.info("🚀 Trading bot started and will run periodically!")
    listen_for_trade_signals()

