"""Test trading strategy on historical data"""
from datetime import datetime, timedelta
from math import floor
from os import environ
from pymongo import MongoClient, ASCENDING

# Configurable values
TARGET_RSI = 30
funds = 10000
buy_amount = 1000
# dict[str, list(float, int, bool)]
portfolio = {}

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
mongo_db = mongo_client['stocks']
start_date = None
end_date = datetime.now() - timedelta(days=1)

print('START - $' + repr(funds))
for asset_collection_name in mongo_db.list_collection_names():
    asset_collection = mongo_db.get_collection(asset_collection_name)
    asset_cursor = asset_collection.find()
    for asset in asset_cursor.sort(key_or_list='date', direction=ASCENDING):
        if start_date is None:
            start_date = asset['date']
        symbol = asset['symbol']
        close = asset['close']

        # If already owned, check for sell
        if symbol in portfolio:
            if close <= (0.9 * portfolio[symbol][0]):
                print('Sell loss: ' + symbol
                      + ' - ' + repr(close)
                      + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                funds += (portfolio[symbol][1] * close)
                portfolio.pop(symbol)
                continue
            elif close >= (1.1 * portfolio[symbol][0]):
                portfolio[symbol][2] = True

            if (portfolio[symbol][2]
                    and asset['macd'] <= asset['macd_signal']):
                print('Sell gain: ' + symbol
                      + ' - ' + repr(close)
                      + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                funds += (portfolio[symbol][1] * close)
                portfolio.pop(symbol)

        # If not already owned, check for buy
        elif asset['macd'] > asset['macd_signal']:
            filtered_rsi = [rsi_value for rsi_value in asset['rsi']
                            if rsi_value < TARGET_RSI]
            if len(filtered_rsi) > 0:
                filtered_trend = [trend_value for trend_value in asset['trend']
                                  if trend_value]
                if len(filtered_trend) > 0:
                    print('Buy signal: ' + symbol
                          + ' - ' + repr(close)
                          + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                    if close > buy_amount:
                        print('But price is higher than buy_amount!')
                    elif funds < buy_amount:
                        print('But not enough money!')
                    else:
                        quantity = floor(buy_amount / close)
                        funds -= (quantity * close)
                        portfolio[symbol] = [close, quantity, False]


# Undo un-finished positions
for symbol in portfolio.keys():
    funds += (portfolio[symbol][0] * portfolio[symbol][1])
total_days = (end_date - start_date).days
print('FINISH - $' + repr(funds))
print('Total time passed: ' + repr(total_days) + ' days')
print('ROI: ' + repr((funds - 10000) / 10000 * 100) + '%')
mongo_client.close()
