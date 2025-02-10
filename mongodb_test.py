from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["trading_db"]

print("MongoDB Connected:", db.list_collection_names())
