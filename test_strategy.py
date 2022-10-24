"""Test trading strategy on historical data"""
from datetime import datetime, timedelta
from os import environ
from pymongo import MongoClient

# Configurable values
START_DATE = (2022, 8, 12)
END_DATE = (2022, 10, 21)
TARGET_RSI = 30

# dict[str, tuple(float, bool)]
potential_buys = {}

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
mongo_db = mongo_client['stocks']

current_date = datetime(
    year=START_DATE[0], month=START_DATE[1], day=START_DATE[2])
end_date = datetime(
    year=END_DATE[0], month=END_DATE[1], day=END_DATE[2])

while current_date <= end_date:
    for asset_collection_name in mongo_db.list_collection_names(
            filter={'name': {'$regex': r"^(?!MARKET_DATA)"}}):
        asset_collection = mongo_db.get_collection(asset_collection_name)
        asset = asset_collection.find_one(filter={'date': current_date})
        if asset is None:
            break
        symbol = asset['symbol']
        close = asset['close']

        already_processed = False
        if symbol in potential_buys:
            already_processed = True
            if close <= (0.9 * potential_buys[symbol][0]):
                print('LOSS registered: ' + symbol + ' - ' + repr(close))
                potential_buys.pop(symbol)
                continue
            elif close >= (1.1 * potential_buys[symbol][0]):
                potential_buys[symbol][1] = True

            if (potential_buys[symbol][1]
                    and asset['macd'] <= asset['macd_signal']):
                print('Sell signal: ' + symbol + ' - ' + repr(close))
                potential_buys.pop(symbol)

        elif asset['macd'] > asset['macd_signal']:
            filtered_rsi = [rsi_value for rsi_value in asset['rsi']
                            if rsi_value < TARGET_RSI]
            if len(filtered_rsi) > 0:
                print('Found potential: ' + symbol + ' - ' + repr(close))
                potential_buys[symbol] = [close, False]
                filtered_trend = [trend_value for trend_value in asset['trend']
                                  if trend_value]
                if len(filtered_trend) > 0:
                    print('Also passed above trend line! ' + asset['symbol'])

    current_date += timedelta(days=1)

mongo_client.close()
