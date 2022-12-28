"""Verify all records have the same latest date"""
from os import environ

from pymongo import MongoClient

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
market_db = mongo_client.get_database(name='market')
stock_db = mongo_client.get_database(name='stocks')
market_collection = market_db.get_collection(name='MARKET_DATA')
market_item = market_collection.find_one()

latest_date = market_item['latest_date']
print(latest_date)
error = False
for symbol in stock_db.list_collection_names():
    asset_collection = stock_db.get_collection(name=symbol)
    asset_items = asset_collection.find(filter={'date': latest_date})
    item_count = 0
    # Is there a better way to determine number of items in cursor?
    for asset_item in asset_items:
        item_count += 1
    if item_count > 1:
        print('Duplicate data detected: ' + symbol)
        error = True
    elif item_count < 1:
        print('No data for: ' + symbol)
        error = True
if not error:
    print('Data successfully validated')
mongo_client.close()
