"""Delete data for 'latest_date' date value in DB"""
from os import environ

from pymongo import MongoClient

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
stock_db = mongo_client.get_database(name='stocks')

market_db = mongo_client.get_database(name='market')
market_collection = market_db.get_collection(name='MARKET_DATA')
market_item = market_collection.find_one()
latest_date = market_item['latest_date']

for asset_collection_name in stock_db.list_collection_names():
    asset_collection = stock_db.get_collection(name=asset_collection_name)
    thefilter = {'date': latest_date}
    print(asset_collection.delete_many(filter=thefilter).deleted_count)

mongo_client.close()
