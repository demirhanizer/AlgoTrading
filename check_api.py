import os
from dotenv import load_dotenv
import ccxt

load_dotenv()
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True
})

try:
    balance = exchange.fetch_balance()
    print("✅ Binance API Key is working!")
    print("💰 USDT Balance:", balance['USDT']['free'])
except Exception as e:
    print("❌ API Key Test Failed:", e)
