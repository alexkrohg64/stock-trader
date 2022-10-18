"""Determine and record if the stock market is open today"""
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetCalendarRequest
from base64 import b64decode
import boto3
import datetime
import os
import pymongo
import telegram
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
    alpaca_client = TradingClient(api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED, paper=False)
    telegram_bot = telegram.Bot(token=BOT_DECRYPTED)

    today = datetime.date.today()
    today_filter = GetCalendarRequest(start=today, end=today)

    trading_calendar = alpaca_client.get_calendar(filters=today_filter)

    if len(trading_calendar) != 1:
        error_message = 'Unexpected number of trading days returned! : ' + repr(len(trading_calendar))
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        return

    market_is_open = trading_calendar[0].date == today
    update_dict = {
        'market_is_open': market_is_open,
        'day_of_month': today.day
    }

    session_encoded = urllib.parse.quote_plus(os.environ.get('AWS_SESSION_TOKEN'))
    mongo_connection_string = MONGO_DECRYPTED + session_encoded

    mongo_client = pymongo.MongoClient(mongo_connection_string)
    mongo_db = mongo_client['stocks']
    mongo_collection = mongo_db['MARKET_DATA']
    mongo_collection.update_one(filter={'my_id': os.environ.get('MARKET_COLLECTION_ID')}, update={'$set': update_dict})
    mongo_client.close()
