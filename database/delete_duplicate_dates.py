from os import environ

from pymongo import MongoClient

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
stock_db = mongo_client.get_database(name='stocks')

for asset_collection_name in stock_db.list_collection_names():
    asset_collection = stock_db.get_collection(name=asset_collection_name)
    dates = set()
    delete = False
    for asset_item in asset_collection.find():
        if asset_item['date'] in dates:
            delete = True
            delete_date = asset_item['date']
        else:
            dates.add(asset_item['date'])
    if delete:
        asset_collection.delete_one(filter={'date': delete_date})

mongo_client.close()
