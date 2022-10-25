"""Perform initial dataload"""
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass
from alpaca.trading.enums import AssetExchange
from alpaca.trading.enums import AssetStatus
from alpaca.trading.enums import CorporateActionType
from alpaca.trading.models import CorporateActionAnnouncement
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.requests import GetCorporateAnnouncementsRequest
from data import tracked_asset
from datetime import date, datetime, timedelta, timezone
from os import environ
from pandas.tseries.offsets import BDay
from pymongo import MongoClient
from time import sleep

DATA_POINTS = 300

tracked_assets = []

alpaca_historical_client = StockHistoricalDataClient(
    api_key=environ.get('APCA_API_KEY_ID'),
    secret_key=environ.get('APCA_API_SECRET_KEY'))
alpaca_trading_client = TradingClient(
    api_key=environ.get('APCA_API_KEY_ID'),
    secret_key=environ.get('APCA_API_SECRET_KEY'), paper=False)
mongo_client = MongoClient(environ.get('MONGO_CONNECTION_STRING'))


def get_stock_splits(
        start_date: datetime,
        end_date: datetime) -> dict[str, CorporateActionAnnouncement]:
    result = {}
    ca_types = [CorporateActionType.SPINOFF, CorporateActionType.SPLIT]
    # Loop over API-limited interval of 90 day search period
    # to collect all relevant announcements
    current_date = start_date
    while current_date < end_date:
        news_request = GetCorporateAnnouncementsRequest(
            ca_types=ca_types, since=current_date,
            until=(current_date + timedelta(days=90)))
        announcements = alpaca_trading_client.get_corporate_annoucements(
            filter=news_request)
        for announcement in announcements:
            affected_symbol = announcement.target_symbol
            if affected_symbol is not None and affected_symbol not in result:
                result[affected_symbol] = announcement
        current_date += timedelta(days=90)
        sleep(0.3)
    return result


def import_asset(code: str, start_date: datetime,
                 end_date: datetime) -> bool:
    """Fetch and ingest data for given stock symbol"""
    bars_request = StockBarsRequest(
        symbol_or_symbols=code, start=start_date, end=end_date,
        timeframe=TimeFrame.Day)
    try:
        bars_response = alpaca_historical_client.get_stock_bars(
            request_params=bars_request)
    except AttributeError:
        # A few problematic NASDAQ stocks exist at time of commit
        print('Swallowing empty response for: ' + code)
        return False
    bars = bars_response.data[code]
    # skip assets which have not been on the market long enough
    if len(bars) < DATA_POINTS:
        return False

    if len(bars) > DATA_POINTS:
        print('Warning - excessive data points detected for '
              + code + '! Continuing...')

    latest_bar = bars[-1]
    latest_date = latest_bar.timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0)
    asset = tracked_asset.TrackedAsset(
        symbol=code, date=latest_date, close=latest_bar.close)

    if not asset.has_enough_volume(bars):
        return False

    prices = [candle.close for candle in bars]
    dates = [candle.timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0) for candle in bars]

    if code in stock_split_symbols:
        stock_split = stock_splits[code]
        if stock_split.ca_type == CorporateActionType.SPINOFF:
            print('Unable to process SPINOFF events at this time')
            print('Symbol: ' + code)
            return False
        ex_date = stock_split.ex_date
        if ex_date is None:
            print('Unexpected empty EX_DATE for split announcement!')
            print('Symbol: ' + code)
            return False
        ex_datetime = datetime.combine(
            date=ex_date, time=datetime.min.time(), tzinfo=timezone.utc)
        index = 0
        while dates[index] < ex_datetime:
            prices[index] = (
                prices[index]
                * stock_split.old_rate / stock_split.new_rate)
            index += 1
    # This ordering of methods matters now for DB calls
    asset.calculate_macd(prices=prices, dates=dates, db_client=mongo_client)
    asset.calculate_rsi(prices=prices, dates=dates, db_client=mongo_client)
    asset.calculate_ema_big_long(
        prices=prices, dates=dates, db_client=mongo_client)
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
starting_datetime = (today - BDay(DATA_POINTS + 10))

stock_splits = get_stock_splits(start_date=starting_datetime, end_date=today)
stock_split_symbols = stock_splits.keys()

for symbol in symbols:
    if not import_asset(
            code=symbol, start_date=starting_datetime, end_date=today):
        # API free-rate limit: 200/min
        sleep(0.3)
    else:
        print(symbol)

print(len(tracked_assets))

# Update MARKET_DATA collection
market_object = {
    'my_id': environ.get('MARKET_COLLECTION_ID'),
    'market_is_open': False,
    'day_of_month': today.day,
    'latest_date': tracked_assets[0].date
}
mongo_client['market']['MARKET_DATA'].insert_one(document=market_object)
mongo_client.close()
