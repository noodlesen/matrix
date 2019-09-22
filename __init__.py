from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
print(client)
db = client.history_db

BARS = db.bars
INDUSTRIES = db.industries
SECTORS = db.sectors
METRICS = db.metrics
STOCKS = db.stocks
