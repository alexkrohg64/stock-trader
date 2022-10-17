"""Update technical analysis data"""
from alpaca.trading.client import TradingClient
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
os.environ['APCA_API_KEY_ID'] = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ID_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

KEY_ENCRYPTED = os.environ['APCA_API_SECRET_KEY']
os.environ['APCA_API_SECRET_KEY'] = boto3.client('kms').decrypt(
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

def lambda_handler(event, context):
    # asset_list table tracks known symbols
    list_table = boto3.resource('dynamodb').Table('asset_list')
    list_table.load()

    tracked_assets = []
    api = REST()
    telegram_bot = telegram.Bot(token=BOT_DECRYPTED)

    # Ensure data is in sync
    market_item = list_table.get_item(Key={'symbol': 'MARKET_IS_OPEN'})['Item']
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    if market_item['day_of_month'] != yesterday.day:
        error_message = 'Dates do not match up! '
        error_message += 'DB day: ' + repr(market_item['day_of_month'])
        error_message += '. Yesterday day: ' + repr(yesterday.day)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        return

    # Only perform daily update when the market was open the day before
    if not market_item['market_is_open']:
        return

    # asset_date tracks most recently stored date of all assets, format %Y%m%d
    asset_date = list_table.get_item(Key={'symbol': 'LATEST_DATE'})['Item']['date']

    # Load tracked_assets based off known symbols in asset_list table
    list_items = list_table.scan()['Items']
    for list_item in list_items:
        if list_item['symbol'] == 'MARKET_IS_OPEN':
            continue

        asset_symbol = list_item['symbol']
        asset_table = boto3.resource('dynamodb').Table(asset_symbol + '_TABLE')
        asset_table.load()

        # Get most recent record
        asset_item = asset_table.get_item(Key={'date': asset_date})['Item']
        db_date = datetime.datetime.strptime(asset_item['date'], '%Y%m%d').date()

        tracked_assets.append(tracked_asset.TrackedAsset(symbol=asset_item['symbol'],
            ema_short=float(asset_item['ema_short']), ema_long=float(asset_item['ema_long']),
            macd=float(asset_item['macd']), macd_signal=float(asset_item['macd_signal']),
            average_gains=float(asset_item['average_gains']), average_losses=float(asset_item['average_losses']),
            rsi=asset_item['rsi'], ema_big_long=float(asset_item['ema_big_long']),
            trend=asset_item['trend'], latest_date=db_date, latest_close=float(asset_item['latest_close'])))

    for asset in tracked_assets:
        asset.rsi = [float(rsi_value) for rsi_value in asset.rsi]
        symbol = asset.symbol
        bars = api.get_bars(symbol=symbol, timeframe=TimeFrame.Day, start=yesterday,
            end=yesterday, limit=1)

        if len(bars) != 1:
            error_message = 'Error while updating: ' + symbol
            error_message += '. Invalid amount of data returned: ' + repr(len(bars))
            telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
            return

        candle = bars[0]
        date_of_candle = candle.t.date()

        if date_of_candle != yesterday:
            error_message = 'Error while updating: ' + symbol
            error_message += '. Expected date: ' + repr(yesterday)
            error_message += '. Date of data returned: ' + repr(date_of_candle)
            telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
            return

        if date_of_candle <= asset.latest_date:
            error_message = 'Duplicate data detected while updating: ' + symbol
            error_message += '. Asset latest date: ' + repr(asset.latest_date)
            error_message += '. Date of candle: ' + repr(date_of_candle)
            telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
            return

        asset.update_stats(new_price=candle.c, new_date=yesterday)
        # API free-rate limit: 200/min
        sleep(0.3)

    # Update DB after all broker API requests are successful and all stats updated
    for asset in tracked_assets:
        raw_dict = asset.__dict__
        numbers_dict = {k:Decimal(str(v)) for k, v in raw_dict.items() if isinstance(v, float)}
        others_dict = {k:v for k, v in raw_dict.items() if not isinstance(v, float)}
        for index, rsi_value in enumerate(others_dict['rsi']):
            others_dict['rsi'][index] = Decimal(str(rsi_value))
        others_dict['date'] = asset.latest_date.strftime('%Y%m%d')
        del others_dict['latest_date']
        updated_dict = {**numbers_dict, **others_dict}
        updated_asset_table = boto3.resource('dynamodb').Table(asset.symbol + '_TABLE')
        updated_asset_table.load()
        updated_asset_table.put_item(Item=updated_dict)

    # Update overall asset_date tracker
    asset_date = tracked_assets[0].latest_date.strftime('%Y%m%d')
    list_table.put_item(Item={
        'symbol': 'LATEST_DATE',
        'date': asset_date
    })
