"""Display any upcoming entry signals"""
from datetime import date, datetime, timedelta
from os import environ

from pymongo import MongoClient

TARGET_RSI = 30

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
stock_db = mongo_client.get_database(name='stocks')

today = datetime.combine(
    date=date.today(), time=datetime.min.time())
long_signals = []
short_signals = []

for symbol in stock_db.list_collection_names():
    asset_collection = stock_db.get_collection(name=symbol)
    asset_item = asset_collection.find_one(filter={'date': today})
    counter = 0
    while asset_item is None and counter < 3:
        today -= timedelta(days=1)
        counter += 1
        asset_item = asset_collection.find_one(filter={'date': today})
    if asset_item is None:
        print('No data found for: ' + symbol)
        print('Abort!')
        break
    if asset_item['macd'] > asset_item['macd_signal']:
        filtered_rsi = [rsi_value for rsi_value in asset_item['rsi']
                        if rsi_value < TARGET_RSI]
        if filtered_rsi:
            filtered_trend = [
                trend_value for trend_value in asset_item['trend']
                if trend_value]
            if filtered_trend:
                print('Long signal: ' + symbol)
                long_signals.append(asset_item)
    elif asset_item['macd'] < asset_item['macd_signal']:
        filtered_rsi = [rsi_value for rsi_value in asset_item['rsi']
                        if rsi_value > (100 - TARGET_RSI)]
        if filtered_rsi:
            filtered_trend = [
                trend_value for trend_value in asset_item['trend']
                if not trend_value]
            if filtered_trend:
                print('Short signal: ' + symbol)
                short_signals.append(asset_item)
if long_signals:
    print('Long Signals:')
    print(long_signals)
if short_signals:
    print('Short Signals:')
    print(short_signals)
mongo_client.close()
