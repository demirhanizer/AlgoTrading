import os
import time
from dotenv import load_dotenv
import ccxt
from pymongo import MongoClient

load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True
})

client = MongoClient("mongodb://localhost:27017/")
db = client["trading_db"]
signals_collection = db["trade_signals"]
orders_collection = db["trade_orders"]


def fetch_latest_signal():
    return signals_collection.find_one({}, sort=[("timestamp", -1)])


def place_order(signal):
    symbol = "BTC/USDT"
    order_size = 0.0000001

    if signal == "BUY":
        order = exchange.create_market_buy_order(symbol, order_size)
    elif signal == "SELL":
        order = exchange.create_market_sell_order(symbol, order_size)
    else:
        return None

    orders_collection.insert_one(order)
    print(f"🚀 Trade Executed: {signal} - {order}")


def trading_loop():
    last_executed_signal = None

    while True:
        latest_signal = fetch_latest_signal()

        if latest_signal and latest_signal["signal"] != last_executed_signal:
            place_order(latest_signal["signal"])
            last_executed_signal = latest_signal["signal"]

        time.sleep(10)


if __name__ == "__main__":
    print("📡 Starting Trade Execution Service...")
    trading_loop()
