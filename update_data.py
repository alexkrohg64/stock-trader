"""Update technical analysis data"""
from base64 import b64decode
from datetime import date, datetime, timedelta, timezone
from os import environ
from time import sleep
from urllib import parse

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetStatus
from boto3 import client as boto_client
from pymongo import MongoClient
from telegram import Bot

from data.tracked_asset import TrackedAsset

LAMBDA_FUNCTION_NAME = environ.get('AWS_LAMBDA_FUNCTION_NAME')
kms_client = boto_client('kms')


def decrypt_kms(enc_string: str) -> str:
    return kms_client.decrypt(
        CiphertextBlob=b64decode(enc_string),
        EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
    )['Plaintext'].decode('utf-8')


ID_DECRYPTED = decrypt_kms(enc_string=environ.get('APCA_API_KEY_ID'))
KEY_DECRYPTED = decrypt_kms(enc_string=environ.get('APCA_API_SECRET_KEY'))
BOT_DECRYPTED = decrypt_kms(enc_string=environ.get('TGM_BOT_TOKEN'))
CHAT_DECRYPTED = decrypt_kms(enc_string=environ.get('TGM_CHAT_ID'))
MONGO_DECRYPTED = decrypt_kms(
    enc_string=environ.get('MONGO_CONNECTION_STRING'))

alpaca_trading_client = TradingClient(
    api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED, paper=False)
alpaca_historical_client = StockHistoricalDataClient(
    api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED)
telegram_bot = Bot(token=BOT_DECRYPTED)

session_encoded = parse.quote_plus(environ.get('AWS_SESSION_TOKEN'))
mongo_connection_string = MONGO_DECRYPTED + session_encoded
mongo_client = MongoClient(mongo_connection_string)

market_db = mongo_client.get_database(name='market')
market_collection = market_db.get_collection(name='MARKET_DATA')

yesterday_date = date.today() - timedelta(days=1)
# Convert date to datetime
yesterday = datetime.combine(
    date=yesterday_date, time=datetime.min.time(),
    tzinfo=timezone.utc)


def fetch_prices_and_update(asset: TrackedAsset) -> bool:
    asset_symbol = asset.symbol
    bars_request = StockBarsRequest(
        symbol_or_symbols=asset_symbol, start=yesterday, limit=1,
        timeframe=TimeFrame.Day)

    try:
        bars_response = alpaca_historical_client.get_stock_bars(
            request_params=bars_request)
    except AttributeError as aerr:
        sleep(0.3)
        # Check if asset has become inactive
        try:
            asset_response = alpaca_trading_client.get_asset(
                symbol_or_asset_id=asset_symbol
            )
            sleep(0.3)
            if asset_response.status == AssetStatus.INACTIVE:
                message = 'WARNING asset has become inactive: ' + asset_symbol
                message += '. Removing from tracked data...'
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
                return False
            else:
                message = 'Error fetching data from API for: ' + asset_symbol
                message += '. Abort. Error: ' + repr(aerr)
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
                raise UpdateDataError
        except AttributeError as inner_aerr:
            message = 'Inner exception checking for asset: ' + asset_symbol
            message += '. Abort. Error: ' + repr(inner_aerr)
            telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
            raise UpdateDataError
    sleep(0.3)
    bars = bars_response.data[asset_symbol]

    if len(bars) != 1:
        message = 'Error while updating: ' + asset_symbol
        message += '. Invalid amount of data returned: ' + repr(len(bars))
        telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
        raise UpdateDataError

    candle = bars[0]
    date_of_candle = candle.timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0)

    if date_of_candle != yesterday:
        message = 'Error while updating: ' + asset_symbol
        message += '. Expected date: ' + repr(yesterday)
        message += '. Date of data returned: ' + repr(date_of_candle)
        telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
        raise UpdateDataError

    if date_of_candle <= asset.date.replace(tzinfo=timezone.utc):
        message = 'Duplicate data detected while updating: ' + asset_symbol
        message += '. Asset latest date: ' + repr(asset.date)
        message += '. Date of candle: ' + repr(date_of_candle)
        telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
        raise UpdateDataError

    asset.update_stats(new_price=candle.close, new_date=yesterday)
    return True


def get_market_date() -> datetime:
    """Return latest tracked date, or None if market was not open yesterday"""
    # Ensure data is in sync
    market_item = market_collection.find_one()
    if market_item['day_of_month'] != yesterday.day:
        error_message = 'Dates do not match up! '
        error_message += 'DB day: ' + repr(market_item['day_of_month'])
        error_message += '. Yesterday day: ' + repr(yesterday.day)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        raise UpdateDataError

    # Only perform daily update when the market was open the day before
    if not market_item['market_is_open']:
        return None

    # asset_date tracks most recently stored date of all assets
    return market_item['latest_date']


def lambda_handler(event, context):
    try:
        asset_date = get_market_date()
        if asset_date is not None:
            process_stocks(asset_date)
            # Update overall latest_date tracker
            market_collection.update_one(
                filter={'my_id': environ.get('MARKET_COLLECTION_ID')},
                update={'$set': {'latest_date': yesterday}})
    except UpdateDataError:
        return
    except Exception as err:
        error_message = 'Unexpected exception: ' + repr(err)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
    finally:
        mongo_client.close()


def process_stocks(asset_date: datetime) -> None:
    # Gather most recent records for each symbol
    stock_db = mongo_client.get_database(name='stocks')
    inactive_stocks = []
    for asset_collection_name in stock_db.list_collection_names():
        asset_collection = stock_db.get_collection(asset_collection_name)
        asset_item = asset_collection.find_one(filter={'date': asset_date})

        asset = TrackedAsset(
            symbol=asset_item['symbol'], date=asset_date,
            close=asset_item['close'], ema_short=asset_item['ema_short'],
            ema_long=asset_item['ema_long'], macd=asset_item['macd'],
            macd_signal=asset_item['macd_signal'],
            average_gains=asset_item['average_gains'],
            average_losses=asset_item['average_losses'], rsi=asset_item['rsi'],
            ema_big_long=asset_item['ema_big_long'], trend=asset_item['trend'])

        if not fetch_prices_and_update(asset):
            inactive_stocks.append(asset_collection_name)
            continue

        # Incrementally update DB
        new_document = {
            'symbol': asset.symbol,
            'date': asset.date,
            'close': asset.close,
            'ema_short': asset.ema_short,
            'ema_long': asset.ema_long,
            'macd': asset.macd,
            'macd_signal': asset.macd_signal,
            'average_gains': asset.average_gains,
            'average_losses': asset.average_losses,
            'rsi': asset.rsi,
            'ema_big_long': asset.ema_big_long,
            'trend': asset.trend
        }
        asset_collection.insert_one(document=new_document)

    for inactive_stock in inactive_stocks:
        stock_db.drop_collection(name_or_collection=inactive_stock)


class UpdateDataError(Exception):
    pass
