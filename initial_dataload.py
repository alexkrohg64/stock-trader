"""Perform initial dataload"""
from datetime import date, datetime
from os import environ
from time import sleep

from alpaca.data.enums import Adjustment
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus
from alpaca.trading.requests import GetAssetsRequest
from pymongo import MongoClient

from data.tracked_asset import TrackedAsset

tracked_assets = []

alpaca_historical_client = StockHistoricalDataClient(
    api_key=environ.get('APCA_API_KEY_ID'),
    secret_key=environ.get('APCA_API_SECRET_KEY'))
alpaca_trading_client = TradingClient(
    api_key=environ.get('APCA_API_KEY_ID'),
    secret_key=environ.get('APCA_API_SECRET_KEY'), paper=False)
mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))


def import_asset(symbol: str, start_date: datetime,
                 end_date: datetime) -> bool:
    """Fetch and ingest data for given stock symbol"""
    bars_request = StockBarsRequest(
        symbol_or_symbols=symbol, start=start_date, end=end_date,
        timeframe=TimeFrame.Day, adjustment=Adjustment.SPLIT)
    try:
        bars_response = alpaca_historical_client.get_stock_bars(
            request_params=bars_request)
    except AttributeError:
        # A few problematic NASDAQ stocks exist at time of commit
        print('Swallowing empty response for: ' + symbol)
        return False
    bars = bars_response.data[symbol]

    latest_bar = bars[-1]
    latest_date = latest_bar.timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0)
    asset = TrackedAsset(
        symbol=symbol, date=latest_date, close=latest_bar.close)

    if not asset.has_enough_trades(bars):
        return False

    prices = [candle.close for candle in bars]
    dates = [candle.timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0) for candle in bars]

    # This ordering of methods matters now for DB calls
    asset.calculate_macd(prices=prices, dates=dates, mongo_client=mongo_client)
    asset.calculate_rsi(prices=prices, dates=dates, mongo_client=mongo_client)
    asset.calculate_ema_big_long(
        prices=prices, dates=dates, mongo_client=mongo_client)
    tracked_assets.append(asset)
    return True


assets_request = GetAssetsRequest(
    status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY)
assets = alpaca_trading_client.get_all_assets(filter=assets_request)
sleep(0.3)
symbols = [asset.symbol for asset in assets
           if asset.tradable and asset.exchange != AssetExchange.OTC]
# Filter out undesirable assets
symbols = [symbol for symbol in symbols if symbol not in
           ['VXX', 'VIXY', 'UVXY']]

today = datetime.combine(
    date=date.today(), time=datetime.min.time())
starting_datetime = datetime(year=2015, month=12, day=1)

for symbol in symbols:
    if not import_asset(
            symbol=symbol, start_date=starting_datetime, end_date=today):
        # API free-rate limit: 200/min
        sleep(0.3)
    else:
        print(symbol)

print(len(tracked_assets))

# Update MARKET_DATA collection
market_object = {
    'my_id': environ.get('MARKET_COLLECTION_ID'),
    'market_is_open': True,
    'day_of_month': today.day,
    'latest_date': tracked_assets[0].date
}
mongo_db = mongo_client.get_database(name='market')
mongo_collection = mongo_db.get_collection(name='MARKET_DATA')
mongo_collection.insert_one(document=market_object)
mongo_client.close()
