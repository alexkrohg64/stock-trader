"""Analyze data and manage trades"""
from base64 import b64decode
from os import environ
from time import sleep
from urllib import parse

from alpaca.trading.client import TradingClient
from boto3 import client as boto_client
from pymongo import MongoClient
from telegram import Bot

TARGET_PERCENT = 10
STOP_LOSS_PERCENT = 10

LAMBDA_FUNCTION_NAME = environ.get('AWS_LAMBDA_FUNCTION_NAME')
kms_client = boto_client('kms')


def decrypt_kms(enc_string: str) -> str:
    return kms_client.decrypt(
        CiphertextBlob=b64decode(enc_string),
        EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
    )['Plaintext'].decode('utf-8')


ID_DECRYPTED = decrypt_kms(enc_string=environ.get('PAPER_APCA_API_KEY_ID'))
KEY_DECRYPTED = decrypt_kms(
    enc_string=environ.get('PAPER_APCA_API_SECRET_KEY'))
BOT_DECRYPTED = decrypt_kms(enc_string=environ.get('TGM_BOT_TOKEN'))
CHAT_DECRYPTED = decrypt_kms(enc_string=environ.get('TGM_CHAT_ID'))
MONGO_DECRYPTED = decrypt_kms(
    enc_string=environ.get('MONGO_CONNECTION_STRING'))

alpaca_client = TradingClient(
    api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED, paper=True)
telegram_bot = Bot(token=BOT_DECRYPTED)

session_encoded = parse.quote_plus(environ.get('AWS_SESSION_TOKEN'))
mongo_connection_string = MONGO_DECRYPTED + session_encoded
mongo_client = MongoClient(mongo_connection_string)
stock_db = mongo_client.get_database(name='stocks')
market_db = mongo_client.get_database(name='market')
# Do not buy stocks which just took a loss
black_list = []


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

    # First check for changes outside code execution.
    # Compare current positions against saved values.
    # If any are missing, the stop loss limit order must have sold.
    assets_collection = market_db.get_collection(name='HELD_ASSETS')
    db_positions = assets_collection.find_one()

    market_collection = market_db.get_collection(name='MARKET_DATA')
    latest_date = market_collection.find_one()['latest_date']

    position_symbols = [position.symbol for position in positions]
    order_symbols = [order.symbol for order in orders]
    for symbol in list(db_positions):
        if symbol not in position_symbols:
            message = 'LOSS limit sold for: ' + symbol
            telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
            del db_positions[symbol]
            black_list.append(symbol)
        # Verify above check by double checking open limit orders
        elif symbol not in order_symbols:
            message = 'ERROR missing expected limit order for: ' + symbol
            telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
            # May not be a loss, however further analysis is needed to
            # account for this in future before buying again.
            black_list.append(symbol)

    # Second check open positions for target and/or sell signal
    for symbol in list(db_positions):
        stock_collection = stock_db.get_collection(symbol)
        stock_item = stock_collection.find_one(filter={'date': latest_date})
        latest_close = stock_item['close']
        if latest_close >= (1.1 * db_positions[symbol][0]):
            db_positions[symbol][2] = True

        if (db_positions[symbol][2]
                and stock_item['macd'] <= stock_item['macd_signal']):
            pass  # Sell signal

    # Finally calculate buy_amount and place any buy/limit orders


class ManageTradesError(Exception):
    pass
