"""Analyze data and manage trades"""
from base64 import b64decode
from datetime import date
from os import environ
from time import sleep
from urllib import parse

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest
from boto3 import client as boto_client
from pymongo import MongoClient
from telegram import Bot

OPEN_POSITIONS = 10
STOP_LOSS_PERCENT = 10
TARGET_PERCENT = 10
TARGET_RSI = 30

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
market_collection = market_db.get_collection(name='MARKET_DATA')
market_item = market_collection.find_one()
today = date.today()
# Do not buy stocks which just took a loss
black_list = []


def is_market_open() -> bool:
    """Check if market is open today"""
    if market_item['day_of_month'] != today.day:
        error_message = 'Dates do not match up! '
        error_message += 'DB day: ' + repr(market_item['day_of_month'])
        error_message += '. Yesterday day: ' + repr(today.day)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        raise ManageTradesError

    return market_item['market_is_open']


def lambda_handler(event, context):
    try:
        if is_market_open():
            manage_trades()
    except ManageTradesError:
        return
    except Exception as err:
        error_message = 'Unexpected exception: ' + repr(err)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
    finally:
        mongo_client.close()


def manage_trades() -> None:
    # Fetch available funds, open positions, and open orders
    try:
        account = alpaca_client.get_account()
        sleep(0.3)
        positions = alpaca_client.get_all_positions()
        sleep(0.3)
        orders = alpaca_client.get_orders()
        sleep(0.3)

    except AttributeError:
        error_message = 'Error fetching trading account!'
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
        raise ManageTradesError

    latest_date = market_item['latest_date']
    held_asset_collection = market_db.get_collection(name='HELD_ASSETS')
    held_assets = held_asset_collection.find_one()
    position_symbols = [position.symbol for position in positions]

    # First sync saved data with active positions
    sold_symbols = [symbol for symbol in held_assets
                    if symbol not in position_symbols
                    and symbol not in ['_id', 'my_id']]
    if sold_symbols:
        message = 'WARNING Stop loss detected for: ' + repr(sold_symbols)
        message += '. Removing from tracked data.'
        telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
        for sold_symbol in sold_symbols:
            del held_assets[sold_symbol]
            held_asset_collection.update_one(
                filter={'my_id': environ.get('HELD_ASSETS_ID')},
                update={'$unset': sold_symbol})
            black_list.append(sold_symbol)

    # Second check open positions for target and/or sell signal
    for position in positions:
        symbol = position.symbol
        stock_collection = stock_db.get_collection(symbol)
        stock_item = stock_collection.find_one(filter={'date': latest_date})
        # If target not already hit, check for target hit
        if not held_assets[symbol]:
            latest_close = stock_item['close']
            entry_price_str = position.avg_entry_price
            if entry_price_str is None or entry_price_str == '':
                message = 'Empty value for avg_entry_price! '
                message += 'Unable to determine target for: ' + symbol
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
                raise ManageTradesError
            entry_price = float(entry_price_str)
            if latest_close >= (entry_price * ((100 + TARGET_PERCENT) / 100)):
                message = 'Target reached for: ' + symbol
                message += '. entry_price: ' + repr(entry_price)
                message += '. latest_close: ' + repr(latest_close)
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
                held_asset_collection.update_one(
                    filter={'my_id': environ.get('HELD_ASSETS_ID')},
                    update={'$set': {symbol: True}})
                held_assets[symbol] = True
        # If target has been hit, check for macd crossover
        if (held_assets[symbol]
                and stock_item['macd'] <= stock_item['macd_signal']):
            message = 'Sell signal for : ' + symbol
            message += '. Canceling stop loss order then exiting position.'
            telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
            current_orders = [order for order in orders
                              if order.symbol == symbol]
            if len(current_orders) != 1:
                sleep(0.1)
                message = 'Unexpected amount of open orders for: ' + symbol
                message += '. INVESTIGATE IMMEDIATELY.'
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
                raise ManageTradesError
            stop_loss_order = current_orders[0]
            try:
                alpaca_client.cancel_order_by_id(
                    order_id=stop_loss_order.id)
            except AttributeError as aerr:
                err = 'Error canceling sell order for: ' + symbol
                err += '. Exception: ' + repr(aerr)
                telegram_bot.send_message(
                    text=err, chat_id=CHAT_DECRYPTED)
                raise ManageTradesError
            # Wait for above order to fully cancel
            sleep(2)
            sell_amount_str = position.market_value
            if sell_amount_str is None or sell_amount_str == '':
                message = 'Empty value for market_value! '
                message += 'Unable to create sell request for: ' + symbol
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
                raise ManageTradesError
            sell_amount = round(number=float(sell_amount_str), ndigits=2)
            order_request = MarketOrderRequest(
                symbol=symbol, notional=sell_amount, side=OrderSide.SELL,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY)
            try:
                alpaca_client.submit_order(
                    order_data=order_request)
            except AttributeError as aerr:
                err = 'Error submitting sell order for: ' + symbol
                err += '. Exception: ' + repr(aerr)
                telegram_bot.send_message(
                    text=err, chat_id=CHAT_DECRYPTED)
                raise ManageTradesError
            # It is given at this point that macd <= macd_signal
            black_list.append(symbol)

    # Finally calculate buy_amount and place any buy/limit orders
    total_funds = 0
    if len(positions) < OPEN_POSITIONS:
        total_funds = account.cash / (
            (OPEN_POSITIONS - len(positions)) / OPEN_POSITIONS)
    buy_amount = round(number=(total_funds / OPEN_POSITIONS), ndigits=2)
    for symbol in stock_db.list_collection_names():
        if symbol in black_list or symbol in position_symbols:
            continue
        asset_collection = stock_db.get_collection(symbol)
        asset = asset_collection.find_one(filter={'date': latest_date})
        if asset['macd'] > asset['macd_signal']:
            filtered_rsi = [rsi_value for rsi_value in asset['rsi']
                            if rsi_value < TARGET_RSI]
            if filtered_rsi:
                filtered_trend = [trend_value for trend_value in asset['trend']
                                  if trend_value]
                if filtered_trend:
                    message = 'Buy signal: ' + symbol
                    closing_price = asset['close']
                    if account.cash < buy_amount:
                        message += '. But not enough money!'
                        telegram_bot.send_message(
                            text=message, chat_id=CHAT_DECRYPTED)
                    else:
                        stop_loss_price = (
                            closing_price * ((100 - STOP_LOSS_PERCENT) / 100))
                        stop_loss_request = StopLossRequest(
                            time_in_force=TimeInForce.GTC,
                            stop_price=stop_loss_price
                        )
                        order_request = MarketOrderRequest(
                            symbol=symbol, notional=buy_amount,
                            side=OrderSide.BUY, type=OrderType.MARKET,
                            time_in_force=TimeInForce.DAY,
                            order_class=OrderClass.OTO,
                            stop_loss=stop_loss_request)
                        try:
                            alpaca_client.submit_order(
                                order_data=order_request)
                        except AttributeError as aerr:
                            err = 'Error submitting buy order for: ' + symbol
                            err += '. Exception: ' + repr(aerr)
                            telegram_bot.send_message(
                                text=err, chat_id=CHAT_DECRYPTED)
                            raise ManageTradesError
                        message += '. Order successfully placed.'
                        telegram_bot.send_message(
                            text=message, chat_id=CHAT_DECRYPTED)
                        held_asset_collection.update_one(
                            filter={'my_id': environ.get('HELD_ASSETS_ID')},
                            update={'$set': {symbol: False}})


class ManageTradesError(Exception):
    pass
