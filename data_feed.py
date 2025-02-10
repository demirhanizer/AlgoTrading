import asyncio
import websockets
import json
from pymongo import MongoClient
from datetime import datetime

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

client = MongoClient("mongodb://localhost:27017/")
db = client["trading_db"]
collection = db["trades"]


async def stream_data():
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as websocket:
                print("Connected to Binance WebSocket")
                while True:
                    response = await websocket.recv()
                    trade_data = json.loads(response)


                    trade_record = {
                        "symbol": trade_data["s"],
                        "price": float(trade_data["p"]),
                        "quantity": float(trade_data["q"]),
                        "timestamp": datetime.utcnow()
                    }


                    collection.insert_one(trade_record)
                    print("Trade saved:", trade_record)

        except websockets.exceptions.ConnectionClosedError:
            print("Connection lost... Reconnecting in 5 seconds")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(stream_data())
