"""Test trading strategy on historical data"""
from datetime import datetime, timedelta
from math import floor
from os import environ

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from pymongo import MongoClient, ASCENDING

# Configurable values
TARGET_RSI = 30
funds = 10000
buy_amount = 1000
# dict[symbol, list(close, quantity, goal_met, is_long)]
portfolio = {}

mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))
mongo_db = mongo_client.get_database(name='stocks')
end_date = datetime.now() - timedelta(days=1)

print('START - $' + repr(funds))
for symbol in mongo_db.list_collection_names():
    asset_collection = mongo_db.get_collection(name=symbol)
    asset_cursor = asset_collection.find()
    for asset in asset_cursor.sort(key_or_list='date', direction=ASCENDING):
        close = asset['close']

        # If already active, check for exit
        if symbol in portfolio:
            # Long
            if portfolio[symbol][3]:
                if close <= (0.9 * portfolio[symbol][0]):
                    print('Sell long loss: ' + symbol
                          + ' - ' + repr(close)
                          + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                    funds += (portfolio[symbol][1] * close)
                    portfolio.pop(symbol)
                    continue
                elif close >= (1.1 * portfolio[symbol][0]):
                    portfolio[symbol][2] = True

                if (portfolio[symbol][2]
                        and asset['macd'] <= asset['macd_signal']):
                    print('Sell long gain: ' + symbol
                          + ' - ' + repr(close)
                          + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                    funds += (portfolio[symbol][1] * close)
                    portfolio.pop(symbol)
            # Short
            else:
                if close >= (1.1 * portfolio[symbol][0]):
                    print('Buy short loss: ' + symbol
                          + ' - ' + repr(close)
                          + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                    funds -= (portfolio[symbol][1] * close)
                    portfolio.pop(symbol)
                    continue
                elif close <= (0.9 * portfolio[symbol][0]):
                    portfolio[symbol][2] = True

                if (portfolio[symbol][2]
                        and asset['macd'] >= asset['macd_signal']):
                    print('Buy short gain: ' + symbol
                          + ' - ' + repr(close)
                          + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                    funds -= (portfolio[symbol][1] * close)
                    portfolio.pop(symbol)

        # If not already owned, check for long
        elif asset['macd'] > asset['macd_signal']:
            filtered_rsi = [rsi_value for rsi_value in asset['rsi']
                            if rsi_value < TARGET_RSI]
            if filtered_rsi:
                filtered_trend = [trend_value for trend_value in asset['trend']
                                  if trend_value]
                if filtered_trend:
                    print('Long signal: ' + symbol
                          + ' - ' + repr(close)
                          + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                    if close > buy_amount:
                        print('But price is higher than buy_amount!')
                    elif funds < buy_amount:
                        print('But not enough money!')
                    else:
                        quantity = floor(buy_amount / close)
                        funds -= (quantity * close)
                        portfolio[symbol] = [close, quantity, False, True]
        # Check for short
        elif asset['macd'] < asset['macd_signal']:
            filtered_rsi = [rsi_value for rsi_value in asset['rsi']
                            if rsi_value > (100 - TARGET_RSI)]
            if filtered_rsi:
                filtered_trend = [trend_value for trend_value in asset['trend']
                                  if not trend_value]
                if filtered_trend:
                    print('Short signal: ' + symbol
                          + ' - ' + repr(close)
                          + ' - ' + asset['date'].strftime('%d-%m-%Y'))
                    if close > buy_amount:
                        print('But price is higher than buy_amount!')
                    elif funds < buy_amount:
                        print('But not enough money!')
                    else:
                        quantity = floor(buy_amount / close)
                        funds += (quantity * close)
                        portfolio[symbol] = [close, quantity, False, False]

alpaca_client = StockHistoricalDataClient(
    api_key=environ.get('APCA_API_KEY_ID'),
    secret_key=environ.get('APCA_API_SECRET_KEY'))
trades_request = StockLatestTradeRequest(
    symbol_or_symbols=list(portfolio))

trades = alpaca_client.get_stock_latest_trade(request_params=trades_request)

for symbol in portfolio:
    funds += (trades[symbol].price * portfolio[symbol][1])
print('FINISH - $' + repr(funds))
print('ROI: ' + repr((funds - 10000) / 10000 * 100) + '%')
mongo_client.close()
