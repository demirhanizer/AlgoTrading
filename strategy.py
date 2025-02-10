import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["trading_db"]
collection = db["trades"]
signals_collection = db["trade_signals"]  

def fetch_price_data(limit=1000):
    cursor = collection.find({}, {"_id": 0, "price": 1, "timestamp": 1}).sort("timestamp", -1).limit(limit)
    data = list(cursor)
    df = pd.DataFrame(data)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    return df

def calculate_sma(df, short_window=50, long_window=200):
    df["SMA_short"] = df["price"].rolling(window=short_window, min_periods=short_window).mean()
    df["SMA_long"] = df["price"].rolling(window=long_window, min_periods=long_window).mean()
    return df

def generate_trade_signal(df):
    df["Signal"] = None

    for i in range(1, len(df)):
        if df["SMA_short"].iloc[i] > df["SMA_long"].iloc[i] and df["SMA_short"].iloc[i-1] <= df["SMA_long"].iloc[i-1]:
            df.at[i, "Signal"] = "BUY"
        elif df["SMA_short"].iloc[i] < df["SMA_long"].iloc[i] and df["SMA_short"].iloc[i-1] >= df["SMA_long"].iloc[i-1]:
            df.at[i, "Signal"] = "SELL"

    return df

def store_signals(df):
    for _, row in df[df["Signal"].notnull()].iterrows():
        signals_collection.insert_one({
            "timestamp": row["timestamp"],
            "signal": row["Signal"],
            "price": row["price"]
        })
        print(f"📌 Stored Signal: {row['Signal']} at {row['price']}")

if __name__ == "__main__":
    df = fetch_price_data()
    df = calculate_sma(df)
    df = generate_trade_signal(df)

    print(df[["timestamp", "price", "SMA_short", "SMA_long", "Signal"]].tail())
    store_signals(df)
