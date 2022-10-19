"""Update technical analysis data"""
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from base64 import b64decode
from time import sleep
import boto3
import datetime
import os
import pymongo
import telegram
import tracked_asset
import urllib

LAMBDA_FUNCTION_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']
ID_ENCRYPTED = os.environ['APCA_API_KEY_ID']
# Decrypt code should run once and variables stored outside of the function
# handler so that these are decrypted once per container
ID_DECRYPTED = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ID_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

KEY_ENCRYPTED = os.environ['APCA_API_SECRET_KEY']
KEY_DECRYPTED = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(KEY_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

BOT_ENCRYPTED = os.environ['TGM_BOT_TOKEN']
BOT_DECRYPTED = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(BOT_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

CHAT_ENCRYPTED = os.environ['TGM_CHAT_ID']
CHAT_DECRYPTED = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(CHAT_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

MONGO_ENCRYPTED = os.environ['MONGO_CONNECTION_STRING']
MONGO_DECRYPTED = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(MONGO_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

def lambda_handler(event, context):
    session_encoded = urllib.parse.quote_plus(os.environ.get('AWS_SESSION_TOKEN'))
    mongo_connection_string = MONGO_DECRYPTED + session_encoded
    mongo_client = pymongo.MongoClient(mongo_connection_string)
    mongo_db = mongo_client['stocks']

    telegram_bot = telegram.Bot(token=BOT_DECRYPTED)

    # Ensure data is in sync
    market_open_collection = mongo_db.get_collection(name='MARKET_DATA')
    market_item = market_open_collection.find_one()
    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
    # Convert date to datetime
    yesterday = datetime.datetime.combine(date=yesterday_date, time=datetime.datetime.min.time(), tzinfo=datetime.timezone.utc)
    if market_item['day_of_month'] != yesterday.day:
        error_message = 'Dates do not match up! '
        error_message += 'DB day: ' + repr(market_item['day_of_month'])
        error_message += '. Yesterday day: ' + repr(yesterday.day)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        return

    # Only perform daily update when the market was open the day before
    if not market_item['market_is_open']:
        return

    # asset_date tracks most recently stored date of all assets
    asset_date = market_item['latest_date']

    alpaca_client = StockHistoricalDataClient(api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED)

    # Gather most recent records for each symbol
    for asset_collection_name in mongo_db.list_collection_names(filter={'name': {'$regex': r"^(?!MARKET_DATA)"}}):
        asset_collection = mongo_db.get_collection(asset_collection_name)
        asset_item = asset_collection.find_one(filter={'latest_date': asset_date})

        asset = tracked_asset.TrackedAsset(symbol=asset_item['symbol'],
            ema_short=asset_item['ema_short'], ema_long=asset_item['ema_long'],
            macd=asset_item['macd'], macd_signal=asset_item['macd_signal'],
            average_gains=asset_item['average_gains'], average_losses=asset_item['average_losses'],
            rsi=asset_item['rsi'], ema_big_long=asset_item['ema_big_long'],
            trend=asset_item['trend'], latest_date=asset_date, latest_close=asset_item['latest_close'])

        asset_symbol = asset.symbol
        bars_request = StockBarsRequest(symbol_or_symbols=asset_symbol, start=yesterday,
            limit=1, timeframe=TimeFrame.Day)

        try:
            bars_response = alpaca_client.get_stock_bars(request_params=bars_request)
        except AttributeError:
            error_message = 'Error fetching data from API for: ' + asset_symbol
            error_message += '. Abort further processing. Request: ' + repr(bars_request)
            telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
            return

        bars = bars_response.data[asset_symbol]

        if len(bars) != 1:
            error_message = 'Error while updating: ' + asset_symbol
            error_message += '. Invalid amount of data returned: ' + repr(len(bars))
            telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
            return

        candle = bars[0]
        date_of_candle = candle.timestamp.replace(hour=0, minute=0, second=0, microsecond=0)

        if date_of_candle != yesterday:
            error_message = 'Error while updating: ' + asset_symbol
            error_message += '. Expected date: ' + repr(yesterday)
            error_message += '. Date of data returned: ' + repr(date_of_candle)
            telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
            return

        if date_of_candle <= asset.latest_date.replace(tzinfo=datetime.timezone.utc):
            error_message = 'Duplicate data detected while updating: ' + asset_symbol
            error_message += '. Asset latest date: ' + repr(asset.latest_date)
            error_message += '. Date of candle: ' + repr(date_of_candle)
            telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
            return

        asset.update_stats(new_price=candle.close, new_date=yesterday)
        # Incrementally update DB. This reduces total DB traffic
        # by only iterating over the data once. Comes with the cost
        # of potential partial updates to the data. Keep this in
        # mind when troubleshooting future errors.
        asset_collection.insert_one(document=asset.__dict__)
        # API free-rate limit: 200/min
        sleep(0.3)

    # Update overall asset_date tracker
    market_open_collection.update_one(filter={'my_id': os.environ.get('MARKET_COLLECTION_ID')},
        update={'$set': {'latest_date': yesterday}})
    mongo_client.close()
