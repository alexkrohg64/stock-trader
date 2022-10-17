"""Perform initial dataload"""
import datetime
from decimal import Decimal
from time import sleep
import os
# Non-standard imports
from alpaca_trade_api.rest import REST, TimeFrame
from pandas.tseries.offsets import BDay
from data import tracked_asset
import pymongo

DATA_POINTS = 300

tracked_assets = []

def import_asset(code, start_date, end_date):
    """Fetch and ingest data for given stock symbol"""
    bars = api.get_bars(symbol=code, timeframe=TimeFrame.Day, start=start_date, end=end_date)

    # skip assets which have not been on the market long enough
    if len(bars) < DATA_POINTS:
        return

    if len(bars) > DATA_POINTS:
        print('Warning - excessive data points detected for ' + code + '! Continuing...')

    latest_bar = bars[-1]
    latest_datetime = datetime.datetime.combine(date=latest_bar.t.date(), time=datetime.datetime.min.time())
    asset = tracked_asset.TrackedAsset(symbol=code, latest_date=latest_datetime,
        latest_close=latest_bar.c)

    if not asset.has_enough_volume(bars):
        return

    prices = [candle.c for candle in bars]

    asset.calculate_macd(prices)
    asset.calculate_rsi(prices)
    asset.calculate_ema_big_long(prices)
    tracked_assets.append(asset)

api = REST()

assets = api.list_assets(status='active', asset_class='us_equity')
symbols = [asset.symbol for asset in assets if asset.tradable]
# Filter out undesirable assets
symbols = [symbol for symbol in symbols if symbol not in ['VXX','VIXY','UVXY']]

yesterday = datetime.date.today() - datetime.timedelta(days=1)
starting_date = (yesterday - BDay(DATA_POINTS + 10)).strftime("%Y-%m-%d")

for symbol in symbols:
    import_asset(symbol, starting_date, yesterday)
    # API free-rate limit: 200/min
    sleep(0.3)

print(len(tracked_assets))

client = pymongo.MongoClient(os.environ.get('MONGO_CONNECTION_STRING'))
db = client['stocks']
for asset in tracked_assets:
    collection = db[asset.symbol]
    collection.insert_one(document=asset.__dict__)

client.close()
