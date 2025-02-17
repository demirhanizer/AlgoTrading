import time
import psutil
import redis
from flask import Flask, jsonify
from pymongo import MongoClient
from prometheus_client import Gauge, Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

redis_client = redis.Redis(host="redis", port=6379, db=0)

mongo_client = MongoClient("mongodb://mongodb:27017/")
db = mongo_client["trading_db"]
orders_collection = db.get_collection("trade_orders")

CPU_USAGE = Gauge("cpu_usage_percent", "CPU usage percentage")
MEMORY_USAGE = Gauge("memory_usage_percent", "Memory usage percentage")
TRADE_LATENCY = Histogram("trade_execution_latency", "Latency per trade execution")
TRADE_ERRORS = Counter("trade_execution_errors", "Count of failed trade executions")
TRADE_COUNT = Counter("trade_executed_total", "Total trades executed")

@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

@app.route("/health")
def health():
    return jsonify({"status": "healthy"}), 200

def monitor_metrics():
    while True:
        CPU_USAGE.set(psutil.cpu_percent(interval=1))
        MEMORY_USAGE.set(psutil.virtual_memory().percent)

        error_count = redis_client.get("error_count")
        TRADE_ERRORS.inc(int(error_count) if error_count else 0)

        time.sleep(5)

def monitor_trades():
    """Monitor trade execution count & latency externally."""
    last_checked = None

    while True:
        latest_trade = orders_collection.find_one({}, sort=[("timestamp", -1)])
        if latest_trade and latest_trade["timestamp"] != last_checked:
            TRADE_COUNT.inc()

            execution_latency = latest_trade.get("execution_time", 0.01)
            TRADE_LATENCY.observe(execution_latency)

            last_checked = latest_trade["timestamp"]
            print(f"✅ New trade detected: {latest_trade}")

        time.sleep(10)

if __name__ == "__main__":
    from threading import Thread
    Thread(target=monitor_metrics, daemon=True).start()
    Thread(target=monitor_trades, daemon=True).start()
    app.run(host="0.0.0.0", port=5001)
