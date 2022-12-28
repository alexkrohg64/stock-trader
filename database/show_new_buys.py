"""Display any upcoming buy signals. Run after 5PM"""
from datetime import date, datetime
from os import environ

from pymongo import MongoClient

TARGET_RSI = 30

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
stock_db = mongo_client.get_database(name='stocks')

today = datetime.combine(
    date=date.today(), time=datetime.min.time())
buy_signals = []

for symbol in stock_db.list_collection_names():
    asset_collection = stock_db.get_collection(name=symbol)
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
                print('Buy signal: ' + symbol)
                buy_signals.append(asset_item)
if buy_signals:
    print(buy_signals)
mongo_client.close()
