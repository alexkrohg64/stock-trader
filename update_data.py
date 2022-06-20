"""Update technical analysis data"""
import boto3
import datetime
from decimal import Decimal
import json
import os
from time import sleep
import sys
# Non-standard imports
from alpaca_trade_api.rest import REST, TimeFrame
from base64 import b64decode
import telegram
import tracked_asset

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
    table = boto3.resource('dynamodb').Table('assets')
    table.load()

    tracked_assets = []
    api = REST()
    telegram_bot = telegram.Bot(token=BOT_DECRYPTED)

    # Ensure data is in sync
    market_item = table.get_item(Key={'symbol': 'MARKET_IS_OPEN'})['Item']
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).day
    if market_item['day_of_month'] != yesterday:
        error_message = 'Dates do not match up! '
        error_message += 'DB day: ' + repr(market_item['day_of_month'])
        error_message += '. Yesterday day: ' + repr(yesterday)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        return

    # Only perform daily update when the market was open the day before
    if not market_item['market_is_open']:
        return

    db_items = table.scan()['Items']

    for db_item in db_items:
        if db_item['symbol'] == 'MARKET_IS_OPEN':
            continue
        db_date = db_item['latest_date']
        latest_date = datetime.date(year=db_date['year'], month=db_date['month'],
            day=db_date['day'])

        tracked_assets.append(tracked_asset.TrackedAsset(symbol=db_item['symbol'],
            ema_short=float(db_item['ema_short']), ema_long=float(db_item['ema_long']),
            macd=float(db_item['macd']), macd_signal=float(db_item['macd_signal']),
            average_gains=float(db_item['average_gains']), average_losses=float(db_item['average_losses']),
            rsi=db_item['rsi'], ema_big_long=float(db_item['ema_big_long']),
            trend=db_item['trend'], latest_date=latest_date, latest_close=float(db_item['latest_close'])))

    for asset in tracked_assets:
        asset.rsi = [float(rsi_value) for rsi_value in asset.rsi]
        symbol = asset.symbol
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
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

    for asset in tracked_assets:
        raw_dict = asset.__dict__
        numbers_dict = {k:Decimal(str(v)) for k, v in raw_dict.items() if isinstance(v, float)}
        others_dict = {k:v for k, v in raw_dict.items() if not isinstance(v, float)}
        for index, rsi_value in enumerate(others_dict['rsi']):
            others_dict['rsi'][index] = Decimal(str(rsi_value))
        others_dict['latest_date'] = {'year': others_dict['latest_date'].year,
                                    'month': others_dict['latest_date'].month,
                                    'day': others_dict['latest_date'].day}
        updated_dict = {**numbers_dict, **others_dict}
        table.put_item(Item=updated_dict)
