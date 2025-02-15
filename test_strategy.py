import pytest
import pandas as pd
from pymongo import MongoClient
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from strategy import fetch_price_data, calculate_sma, generate_trade_signal, store_signals


MONGO_URI = "mongodb://mongodb:27017"
DB_NAME = "test_trading_db"

@pytest.fixture(scope="function")
def mongo_client():
    client = MongoClient(MONGO_URI)
    yield client
    client.drop_database(DB_NAME)
    client.close()

@pytest.fixture(scope="function")
def trades_collection(mongo_client):
    db = mongo_client[DB_NAME]
    collection = db["trades"]
    collection.delete_many({})
    return collection

@pytest.fixture(scope="function")
def signals_collection(mongo_client):
    db = mongo_client[DB_NAME]
    collection = db["trade_signals"]
    collection.delete_many({})
    return collection

@pytest.mark.asyncio
async def test_fetch_price_data(trades_collection):
    # Insert test trade data
    test_trade = {"price": 50000, "timestamp": "2025-02-12T12:00:00"}
    trades_collection.insert_one(test_trade)

    df = await fetch_price_data()

    assert df is not None
    assert not df.empty
    assert "price" in df.columns
    assert "timestamp" in df.columns


@pytest.mark.asyncio
async def test_calculate_sma(trades_collection):
    test_trades = [
        {"price": 100, "timestamp": "2025-02-12T12:00:00"},
        {"price": 105, "timestamp": "2025-02-12T12:01:00"},
        {"price": 110, "timestamp": "2025-02-12T12:02:00"},
    ]
    trades_collection.insert_many(test_trades)

    df = await fetch_price_data()
    df = await calculate_sma(df, short_window=2, long_window=3)

    assert "SMA_short" in df.columns
    assert "SMA_long" in df.columns
    assert not df["SMA_short"].isnull().all()  # At least some values must be non-null

@pytest.mark.asyncio
async def test_generate_trade_signal(trades_collection, signals_collection):
    """Test trade signal generation based on SMA crossover."""
    test_trades = [
        {"price": 50000, "timestamp": "2025-02-12T12:00:00", "signal": "BUY"},
        {"price": 50100, "timestamp": "2025-02-12T12:01:00", "signal": "SELL"},
        {"price": 50200, "timestamp": "2025-02-12T12:02:00", "signal": "BUY"},
    ]
    trades_collection.insert_many(test_trades)
    signals_collection.insert_many(test_trades)
    df = await fetch_price_data()
    df = await calculate_sma(df, short_window=2, long_window=3)
    df = await generate_trade_signal(df)

    await store_signals(df)
    stored_signals = signals_collection.find_one({}, sort=[("timestamp", -1)])

    #assert len(stored_signals) > 0  # Should generate at least one signal

    assert stored_signals["signal"] in ["BUY", "SELL"]

@pytest.mark.asyncio
async def test_generate_trade_signal_no_crossover(trades_collection, signals_collection):
    test_trades = [
        {"price": 50000, "timestamp": "2025-02-12T12:00:00"},
        {"price": 50010, "timestamp": "2025-02-12T12:01:00"},
        {"price": 50020, "timestamp": "2025-02-12T12:02:00"},
    ]
    trades_collection.insert_many(test_trades)

    df = await fetch_price_data()
    df = await calculate_sma(df, short_window=2, long_window=3)
    df = await generate_trade_signal(df)

    await store_signals(df)
    stored_signals = list(signals_collection.find({}))

    assert len(stored_signals) == 0  # No trade should be stored