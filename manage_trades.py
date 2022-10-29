"""Analyze data and manage trades"""
from base64 import b64decode
from os import environ
from time import sleep
from urllib import parse

from alpaca.trading.client import TradingClient
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


ID_DECRYPTED = decrypt_kms(enc_string=environ['PAPER_APCA_API_KEY_ID'])
KEY_DECRYPTED = decrypt_kms(enc_string=environ['PAPER_APCA_API_SECRET_KEY'])
BOT_DECRYPTED = decrypt_kms(enc_string=environ['TGM_BOT_TOKEN'])
CHAT_DECRYPTED = decrypt_kms(enc_string=environ['TGM_CHAT_ID'])
MONGO_DECRYPTED = decrypt_kms(enc_string=environ['MONGO_CONNECTION_STRING'])

alpaca_client = TradingClient(
    api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED, paper=True)
telegram_bot = Bot(token=BOT_DECRYPTED)

session_encoded = parse.quote_plus(environ.get('AWS_SESSION_TOKEN'))
mongo_connection_string = MONGO_DECRYPTED + session_encoded
mongo_client = MongoClient(mongo_connection_string)


def lambda_handler(event, context):
    try:
        manage_trades()
    except ManageTradesError:
        return
    except Exception as err:
        error_message = 'Unexpected exception: ' + repr(err)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
    finally:
        mongo_client.close()


def manage_trades() -> None:
    # Fetch available funds, open positions, and limit orders
    try:
        account = alpaca_client.get_account()
        sleep(0.3)
        positions = alpaca_client.get_all_positions()
        sleep(0.3)
        orders = alpaca_client.get_orders()

    except AttributeError:
        error_message = 'Error fetching trading account!'
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        raise ManageTradesError
    print(account)
    print(positions)
    print(orders)


class ManageTradesError(Exception):
    pass
