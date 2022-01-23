"""Update technical analysis data"""
import datetime
import json
from time import sleep
# Non-standard imports
from alpaca_trade_api.rest import REST, TimeFrame
from data import tracked_asset

json_assets = []
tracked_assets = []
api = REST()

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
        latest_date=latest_date))

for asset in tracked_assets:
    target_date = (asset.latest_date + datetime.timedelta(days=1))
    bars = api.get_bars(symbol=asset.symbol, timeframe=TimeFrame.Day, start=target_date,
        end=target_date, limit=1)

    if len(bars) != 1:
        raise Exception('Invalid amount of data returned! : ' + repr(len(bars)))

    candle = bars[0]
    date_of_candle = candle.t.date()

    if date_of_candle != target_date:
        print('Error! Expected date: ' + repr(target_date))
        raise Exception('Invalid date of data returned! : ' + repr(date_of_candle))

    asset.update_stats(closing_price=candle.c)
    # API free-rate limit: 200/min
    sleep(0.3)

with open(file='assets.json', mode='w', encoding='utf-8') as asset_file:
    json.dump(obj=tracked_assets, fp=asset_file, cls=tracked_asset.AssetEncoder)
