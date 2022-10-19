"""Perform initial dataload"""
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass
from alpaca.trading.enums import AssetExchange
from alpaca.trading.enums import AssetStatus
from alpaca.trading.requests import GetAssetsRequest
from data import tracked_asset
from pandas.tseries.offsets import BDay
from time import sleep
import datetime
import os
import pymongo

DATA_POINTS = 300

tracked_assets = []

alpaca_historical_client = StockHistoricalDataClient(api_key=os.environ.get('APCA_API_KEY_ID'), secret_key=os.environ.get('APCA_API_SECRET_KEY'))
alpaca_trading_client = TradingClient(api_key=os.environ.get('APCA_API_KEY_ID'), secret_key=os.environ.get('APCA_API_SECRET_KEY'), paper=False)

def import_asset(code, start_date, end_date):
    """Fetch and ingest data for given stock symbol"""
    bars_request = StockBarsRequest(symbol_or_symbols=code, start=start_date,
        end=end_date, timeframe=TimeFrame.Day)
    try:
        bars_response = alpaca_historical_client.get_stock_bars(request_params=bars_request)
    except AttributeError:
        # A few problematic NASDAQ stocks exist at time of commit
        print('Swallowing empty response for: ' + code)
        return
    bars = bars_response.data[code]
    # skip assets which have not been on the market long enough
    if len(bars) < DATA_POINTS:
        return

    if len(bars) > DATA_POINTS:
        print('Warning - excessive data points detected for ' + code + '! Continuing...')

    latest_bar = bars[-1]
    latest_date = latest_bar.timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    asset = tracked_asset.TrackedAsset(symbol=code, latest_date=latest_date,
        latest_close=latest_bar.close)

    if not asset.has_enough_volume(bars):
        return

    prices = [candle.close for candle in bars]

    asset.calculate_macd(prices)
    asset.calculate_rsi(prices)
    asset.calculate_ema_big_long(prices)
    tracked_assets.append(asset)

assets_request = GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY)
assets = alpaca_trading_client.get_all_assets(filter=assets_request)
symbols = [asset.symbol for asset in assets if asset.tradable and asset.exchange != AssetExchange.OTC]
# Filter out undesirable assets
symbols = [symbol for symbol in symbols if symbol not in ['VXX','VIXY','UVXY']]

today = datetime.datetime.combine(date=datetime.date.today(), time=datetime.datetime.min.time())
starting_datetime = (today - BDay(DATA_POINTS + 10))

for symbol in symbols:
    import_asset(symbol, starting_datetime, today)
    # API free-rate limit: 200/min
    sleep(0.3)

print(len(tracked_assets))

mongo_client = pymongo.MongoClient(os.environ.get('MONGO_CONNECTION_STRING'))
db = mongo_client['stocks']
for asset in tracked_assets:
    collection = db[asset.symbol]
    collection.insert_one(document=asset.__dict__)

mongo_client.close()
