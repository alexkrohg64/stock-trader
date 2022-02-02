"""Update technical analysis data"""
import datetime
import json
import os
from time import sleep
import sys
# Non-standard imports
from alpaca_trade_api.rest import REST, TimeFrame
from data import tracked_asset
import telegram

# Only perform daily update when the market was open the day before
with open(file='market.json', mode='r', encoding='utf-8') as market_file:
    market_was_open_yesterday = json.load(fp=market_file)
    if not market_was_open_yesterday:
        sys.exit()

json_assets = []
tracked_assets = []
api = REST()
telegram_bot = telegram.Bot(token=os.environ['TGM_BOT_TOKEN'])

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
    symbol = asset.symbol
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    bars = api.get_bars(symbol=symbol, timeframe=TimeFrame.Day, start=yesterday,
        end=yesterday, limit=1)

    if len(bars) != 1:
        error_message = 'Error while updating: ' + symbol
        error_message += '. Invalid amount of data returned: ' + repr(len(bars))
        telegram_bot.send_message(text=error_message, chat_id=os.environ['TGM_CHAT_ID'])
        sys.exit()

    candle = bars[0]
    date_of_candle = candle.t.date()

    if date_of_candle != yesterday:
        error_message = 'Error while updating: ' + symbol
        error_message += '. Expected date: ' + repr(yesterday)
        error_message += '. Date of data returned: ' + repr(date_of_candle)
        telegram_bot.send_message(text=error_message, chat_id=os.environ['TGM_CHAT_ID'])
        sys.exit()

    if date_of_candle <= asset.latest_date:
        error_message = 'Duplicate data detected while updating: ' + symbol
        error_message += '. Asset latest date: ' + repr(asset.latest_date)
        error_message += '. Date of candle: ' + repr(date_of_candle)
        telegram_bot.send_message(text=error_message, chat_id=os.environ['TGM_CHAT_ID'])
        sys.exit()

    asset.update_stats(new_price=candle.c, new_date=yesterday)
    # API free-rate limit: 200/min
    sleep(0.3)

with open(file='assets.json', mode='w', encoding='utf-8') as asset_file:
    json.dump(obj=tracked_assets, fp=asset_file, cls=tracked_asset.AssetEncoder)
