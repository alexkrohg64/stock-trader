"""Determine and record if the stock market is open today"""
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetCalendarRequest
from base64 import b64decode
from boto3 import client as boto_client
from datetime import date
from os import environ
from pymongo import MongoClient
from telegram import Bot
from urllib import parse

LAMBDA_FUNCTION_NAME = environ['AWS_LAMBDA_FUNCTION_NAME']
ID_ENCRYPTED = environ['APCA_API_KEY_ID']
# Decrypt code should run once and variables stored outside of the function
# handler so that these are decrypted once per container
ID_DECRYPTED = boto_client('kms').decrypt(
    CiphertextBlob=b64decode(ID_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

KEY_ENCRYPTED = environ['APCA_API_SECRET_KEY']
KEY_DECRYPTED = boto_client('kms').decrypt(
    CiphertextBlob=b64decode(KEY_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

BOT_ENCRYPTED = environ['TGM_BOT_TOKEN']
BOT_DECRYPTED = boto_client('kms').decrypt(
    CiphertextBlob=b64decode(BOT_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

CHAT_ENCRYPTED = environ['TGM_CHAT_ID']
CHAT_DECRYPTED = boto_client('kms').decrypt(
    CiphertextBlob=b64decode(CHAT_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')

MONGO_ENCRYPTED = environ['MONGO_CONNECTION_STRING']
MONGO_DECRYPTED = boto_client('kms').decrypt(
    CiphertextBlob=b64decode(MONGO_ENCRYPTED),
    EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
)['Plaintext'].decode('utf-8')


def lambda_handler(event, context):
    alpaca_client = TradingClient(
        api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED, paper=False)
    telegram_bot = Bot(token=BOT_DECRYPTED)

    today = date.today()
    today_filter = GetCalendarRequest(start=today, end=today)

    try:
        trading_calendar = alpaca_client.get_calendar(filters=today_filter)
    except AttributeError:
        error_message = 'Error fetching trading calendar! Filter: '
        error_message += repr(today_filter)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        return

    if len(trading_calendar) != 1:
        error_message = 'Unexpected number of trading days returned! : '
        error_message += repr(len(trading_calendar))
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        return

    market_is_open = trading_calendar[0].date == today
    update_dict = {
        'market_is_open': market_is_open,
        'day_of_month': today.day
    }

    sess_encoded = parse.quote_plus(environ.get('AWS_SESSION_TOKEN'))
    mongo_connection_string = MONGO_DECRYPTED + sess_encoded

    mongo_client = MongoClient(mongo_connection_string)
    mongo_db = mongo_client['stocks']
    mongo_collection = mongo_db['MARKET_DATA']
    mongo_collection.update_one(
        filter={'my_id': environ.get('MARKET_COLLECTION_ID')},
        update={'$set': update_dict})
    mongo_client.close()
