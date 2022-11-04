"""For all new positions entered today, submit stop loss orders"""
from base64 import b64decode
from bson.binary import Binary
from datetime import date
from os import environ
from time import sleep
from urllib import parse

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from alpaca.trading.requests import StopOrderRequest
from boto3 import client as boto_client
from pymongo import MongoClient
from telegram import Bot

STOP_LOSS_PERCENT = 10

LAMBDA_FUNCTION_NAME = environ.get('AWS_LAMBDA_FUNCTION_NAME')
kms_client = boto_client('kms')


def decrypt_kms(enc_string: str) -> str:
    return kms_client.decrypt(
        CiphertextBlob=b64decode(enc_string),
        EncryptionContext={'LambdaFunctionName': LAMBDA_FUNCTION_NAME}
    )['Plaintext'].decode('utf-8')


ID_DECRYPTED = decrypt_kms(enc_string=environ.get('APCA_API_KEY_ID'))
KEY_DECRYPTED = decrypt_kms(
    enc_string=environ.get('APCA_API_SECRET_KEY'))
BOT_DECRYPTED = decrypt_kms(enc_string=environ.get('TGM_BOT_TOKEN'))
CHAT_DECRYPTED = decrypt_kms(enc_string=environ.get('TGM_CHAT_ID'))
MONGO_DECRYPTED = decrypt_kms(
    enc_string=environ.get('MONGO_CONNECTION_STRING'))

alpaca_client = TradingClient(
    api_key=ID_DECRYPTED, secret_key=KEY_DECRYPTED, paper=False)
telegram_bot = Bot(token=BOT_DECRYPTED)

session_encoded = parse.quote_plus(environ.get('AWS_SESSION_TOKEN'))
mongo_connection_string = MONGO_DECRYPTED + session_encoded
mongo_client = MongoClient(mongo_connection_string)
market_db = mongo_client.get_database(name='market')
market_collection = market_db.get_collection(name='MARKET_DATA')
market_item = market_collection.find_one()
today = date.today()


def is_market_open() -> bool:
    """Check if market is open today"""
    if market_item['day_of_month'] != today.day:
        error_message = 'Dates do not match up! '
        error_message += 'DB day: ' + repr(market_item['day_of_month'])
        error_message += '. Yesterday day: ' + repr(today.day)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        raise ProcessNewOrdersError

    return market_item['market_is_open']


def lambda_handler(event, context):
    try:
        if is_market_open():
            process_new_orders()
    except ProcessNewOrdersError:
        return
    except Exception as err:
        error_message = 'Unexpected exception: ' + repr(err)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
    finally:
        mongo_client.close()


def process_new_orders() -> None:
    held_asset_collection = market_db.get_collection(name='HELD_ASSETS')
    held_assets = held_asset_collection.find_one()
    # Remove ID-related keys
    del held_assets['_id']
    del held_assets['my_id']

    for symbol in held_assets:
        if 'order_id' in held_assets[symbol]:
            try:
                buy_order = alpaca_client.get_order_by_id(
                    order_id=Binary.as_uuid(held_assets[symbol]['order_id']))
            except AttributeError as aerr:
                message = 'Error getting buy order for: ' + symbol
                message += '. Exception: ' + repr(aerr)
                telegram_bot.send_message(
                    text=message, chat_id=CHAT_DECRYPTED)
                raise ProcessNewOrdersError
            # Sleep for API
            sleep(0.3)
            if buy_order.status != OrderStatus.FILLED:
                message = 'Unexpected order status for: ' + symbol
                message += '. Status: ' + repr(buy_order.status)
                telegram_bot.send_message(
                    text=message, chat_id=CHAT_DECRYPTED)
                raise ProcessNewOrdersError

            bought_quantity = int(buy_order.filled_qty)
            bought_price = float(buy_order.filled_avg_price)
            stop_loss_price = round(
                number=(bought_price * ((100 - STOP_LOSS_PERCENT) / 100)),
                ndigits=2)

            order_request = StopOrderRequest(
                symbol=symbol, qty=bought_quantity,
                side=OrderSide.SELL, type=OrderType.MARKET,
                time_in_force=TimeInForce.GTC,
                stop_price=stop_loss_price)
            try:
                alpaca_client.submit_order(
                    order_data=order_request)
            except AttributeError as aerr:
                err = 'Error submitting stop loss order for: ' + symbol
                err += '. Exception: ' + repr(aerr)
                telegram_bot.send_message(
                    text=err, chat_id=CHAT_DECRYPTED)
                raise ProcessNewOrdersError
            message = 'Found new order, placed stop loss: ' + symbol
            telegram_bot.send_message(
                text=message, chat_id=CHAT_DECRYPTED)

            held_asset_collection.update_one(
                filter={'my_id': environ.get('HELD_ASSETS_ID')},
                update={'$set': {symbol: {'target_met': False}}})


class ProcessNewOrdersError(Exception):
    pass
