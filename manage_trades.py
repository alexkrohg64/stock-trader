"""Analyze market data and manage open positions"""
import datetime
import json
import os
import sys
# Non-standard imports
from data import tracked_asset

TARGET_POSITIONS = 20
TARGET_RSI = 30

# Only execute when the market is open
with open(file='market.json', mode='r', encoding='utf-8') as market_file:
    market_is_open = json.load(fp=market_file)
    if not market_is_open:
        sys.exit()

# Check for earlier errors
if os.path.isfile('./error.json'):
    sys.exit()

json_assets = []
tracked_assets = []
potential_buys = []

with open(file='assets.json', mode='r', encoding='utf-8') as asset_file:
    json_assets = json.load(fp=asset_file)

for json_asset in json_assets:
    dict_date = json_asset['latest_date']
    latest_date = datetime.date(year=dict_date['year'], month=dict_date['month'],
        day=dict_date['day'])

    tracked_assets.append(tracked_asset.TrackedAsset(symbol=json_asset['symbol'],
        ema_short=json_asset['ema_short'], ema_long=json_asset['ema_long'], macd=json_asset['macd'],
        macd_signal=json_asset['macd_signal'], average_gains=json_asset['average_gains'],
        average_losses=json_asset['average_losses'], rsi=json_asset['rsi'],
        ema_big_long=json_asset['ema_big_long'], trend=json_asset['trend'],
        latest_date=latest_date, latest_close=json_asset['latest_close']))

for asset in tracked_assets:
    if asset.macd > asset.macd_signal:
        filtered_rsi = [rsi_value for rsi_value in asset.rsi if rsi_value < TARGET_RSI]
        if len(filtered_rsi) > 0:
            filtered_trend = [trend_value for trend_value in asset.trend if trend_value]
            if len(filtered_trend) > 0:
                potential_buys.append(asset)

print(potential_buys)
print(len(potential_buys))
