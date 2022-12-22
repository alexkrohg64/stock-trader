from os import environ

from pymongo import MongoClient

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
market_db = mongo_client.get_database(name='market')
market_collection = market_db.get_collection(name='MARKET_DATA')
market_item = market_collection.find_one()

print(market_item['latest_date'])
mongo_client.close()
