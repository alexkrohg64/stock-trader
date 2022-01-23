"""Determine and record if the stock market is open today"""
import datetime
import json
from alpaca_trade_api.rest import REST

api = REST()

today = datetime.date.today()

trading_day = api.get_calendar(start=today, end=today)

if len(trading_day) != 1:
    raise Exception('Unexpected number of trading days returned! : ' + repr(len(trading_day)))

with open(file='market.json', mode='w', encoding='utf-8') as market_file:
    json.dump(obj=trading_day[0].date.date() == today, fp=market_file)
