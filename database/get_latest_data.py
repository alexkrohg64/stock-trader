"""Display latest technical analysis data for a given symbol"""
from datetime import date, datetime, timedelta
from os import environ
import sys

from pymongo import MongoClient

if len(sys.argv) != 2:
    print('Usage: python get_latest_data.py <SYMBOL>')
    exit()
else:
    symbol = sys.argv[1].upper()

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
stock_db = mongo_client.get_database(name='stocks')
asset_collection = stock_db.get_collection(name=symbol)

target_date = datetime.combine(
    date=date.today(), time=datetime.min.time())
asset_item = asset_collection.find_one(filter={'date': target_date})

if asset_item is None:
    print('Couldn\'t find data for today. Printing most recent data...')
    while asset_item is None:
        target_date -= timedelta(days=1)
        asset_item = asset_collection.find_one(filter={'date': target_date})

print('Symbol: ' + asset_item['symbol'])
print('Date: ' + str(asset_item['date']))
print('Close: ' + str(asset_item['close']))
print('MACD: ' + str(asset_item['macd']))
print('MACD_SIGNAL: ' + str(asset_item['macd_signal']))
print('RSI: ' + str(asset_item['rsi'][len(asset_item['rsi']) - 1]))
print('BIG_EMA: ' + str(asset_item['ema_big_long']))
mongo_client.close()
