from pymongo import MongoClient

try:
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["trading_db"]
    print("✅ Connected to MongoDB!")

    db.trade_orders.insert_one({"test": "working"})
    print("✅ Insert successful!")

    records = db.trade_orders.find()
    for record in records:
        print(record)

except Exception as e:
    print(f"❌ MongoDB Connection Failed: {e}")
