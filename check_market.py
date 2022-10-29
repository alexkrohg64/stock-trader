"""Determine and record if the stock market is open today"""
from base64 import b64decode
from datetime import date
from os import environ
from urllib import parse

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetCalendarRequest
from boto3 import client as boto_client
from pymongo import MongoClient
from telegram import Bot

LAMBDA_FUNCTION_NAME = environ['AWS_LAMBDA_FUNCTION_NAME']
kms_client = boto_client('kms')


def decrypt_kms(enc_string: str) -> str:
    return kms_client.decrypt(
        CiphertextBlob=b64decode(enc_string),
        EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
    )['Plaintext'].decode('utf-8')


ID_DECRYPTED = decrypt_kms(enc_string=environ['APCA_API_KEY_ID'])
KEY_DECRYPTED = decrypt_kms(enc_string=environ['APCA_API_SECRET_KEY'])
BOT_DECRYPTED = decrypt_kms(enc_string=environ['TGM_BOT_TOKEN'])
CHAT_DECRYPTED = decrypt_kms(enc_string=environ['TGM_CHAT_ID'])
MONGO_DECRYPTED = decrypt_kms(enc_string=environ['MONGO_CONNECTION_STRING'])

alpaca_client = TradingClient(
    api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED, paper=False)
telegram_bot = Bot(token=BOT_DECRYPTED)

sess_encoded = parse.quote_plus(environ.get('AWS_SESSION_TOKEN'))
mongo_connection_string = MONGO_DECRYPTED + sess_encoded
mongo_client = MongoClient(mongo_connection_string)

today = date.today()


def is_market_open() -> bool:
    today_filter = GetCalendarRequest(start=today, end=today)

    try:
        trading_calendar = alpaca_client.get_calendar(filters=today_filter)
    except AttributeError:
        error_message = 'Error fetching trading calendar! Filter: '
        error_message += repr(today_filter)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        raise CheckMarketError

    if len(trading_calendar) != 1:
        error_message = 'Unexpected number of trading days returned! : '
        error_message += repr(len(trading_calendar))
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        raise CheckMarketError

    return trading_calendar[0].date == today


def lambda_handler(event, context):
    try:
        market_is_open = is_market_open()
        update_db(market_is_open)
    except CheckMarketError:
        return
    except Exception as err:
        error_message = 'Unexpected exception: ' + repr(err)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
    finally:
        mongo_client.close()


def update_db(market_is_open: bool) -> None:
    update_dict = {
        'market_is_open': market_is_open,
        'day_of_month': today.day
    }

    mongo_db = mongo_client['market']
    mongo_collection = mongo_db['MARKET_DATA']
    mongo_collection.update_one(
        filter={'my_id': environ.get('MARKET_COLLECTION_ID')},
        update={'$set': update_dict})


class CheckMarketError(Exception):
    pass
