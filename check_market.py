"""Determine and record if the stock market is open today"""
import boto3
import datetime
import json
import os
from alpaca_trade_api.rest import REST
from base64 import b64decode
import telegram

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
    api = REST()
    telegram_bot = telegram.Bot(token=BOT_DECRYPTED)

    today = datetime.date.today()

    trading_day = api.get_calendar(start=today, end=today)

    if len(trading_day) != 1:
        error_message = 'Unexpected number of trading days returned! : ' + repr(len(trading_day))
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        return

    market_is_open = trading_day[0].date.date() == today

    table = boto3.resource('dynamodb').Table('assets')
    table.load()
    table.put_item(Item={
        'symbol': 'MARKET_IS_OPEN',
        'market_is_open': market_is_open
    })
