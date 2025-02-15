import time
import psutil
import redis
from flask import Flask, jsonify
from pymongo import MongoClient
from prometheus_client import Gauge, Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# ✅ Connect to Redis for error tracking
redis_client = redis.Redis(host="redis", port=6379, db=0)

# ✅ Connect to MongoDB for trade monitoring
mongo_client = MongoClient("mongodb://mongodb:27017/")
db = mongo_client["trading_db"]
orders_collection = db.get_collection("trade_orders")

# ✅ Prometheus Metrics
CPU_USAGE = Gauge("cpu_usage_percent", "CPU usage percentage")
MEMORY_USAGE = Gauge("memory_usage_percent", "Memory usage percentage")
TRADE_LATENCY = Histogram("trade_execution_latency", "Latency per trade execution")
TRADE_ERRORS = Counter("trade_execution_errors", "Count of failed trade executions")
TRADE_COUNT = Counter("trade_executed_total", "Total trades executed")

@app.route("/metrics")
def metrics():
    """Expose Prometheus metrics."""
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

@app.route("/health")
def health():
    """Health check endpoint for readiness/liveness probes."""
    return jsonify({"status": "healthy"}), 200

def monitor_metrics():
    """Continuously monitor system performance metrics."""
    while True:
        CPU_USAGE.set(psutil.cpu_percent(interval=1))
        MEMORY_USAGE.set(psutil.virtual_memory().percent)

        # Fetch error count from Redis
        error_count = redis_client.get("error_count")
        TRADE_ERRORS.inc(int(error_count) if error_count else 0)

        time.sleep(5)  # Monitor every 5 seconds

def monitor_trades():
    """Monitor trade execution count & latency externally."""
    last_checked = None  # Track last checked timestamp

    while True:
        latest_trade = orders_collection.find_one({}, sort=[("timestamp", -1)])
        if latest_trade and latest_trade["timestamp"] != last_checked:
            TRADE_COUNT.inc()  # Increment trade count metric

            execution_latency = latest_trade.get("execution_time", 0.01)  # Default latency if missing
            TRADE_LATENCY.observe(execution_latency)

            last_checked = latest_trade["timestamp"]  # Update last checked trade
            print(f"✅ New trade detected: {latest_trade}")

        time.sleep(10)  # Check every 10 seconds

if __name__ == "__main__":
    from threading import Thread
    Thread(target=monitor_metrics, daemon=True).start()
    Thread(target=monitor_trades, daemon=True).start()
    app.run(host="0.0.0.0", port=5001)
