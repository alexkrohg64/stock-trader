"""Determine if the market is open and react to splits and mergers"""
from base64 import b64decode
from datetime import date, timedelta
from os import environ
from time import sleep
from urllib import parse

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import CorporateActionType
from alpaca.trading.requests import GetCalendarRequest
from alpaca.trading.requests import GetCorporateAnnouncementsRequest
from boto3 import client as boto_client
from pymongo import MongoClient, ASCENDING
from telegram import Bot

from data.tracked_asset import TrackedAsset

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
stock_db = mongo_client['stocks']

today = date.today()


def check_announcements() -> None:
    ca_types = [CorporateActionType.SPINOFF, CorporateActionType.SPLIT,
                CorporateActionType.MERGER]
    news_request = GetCorporateAnnouncementsRequest(
            ca_types=ca_types, since=today,
            until=(today + timedelta(days=10)))
    announcements = alpaca_client.get_corporate_annoucements(
            filter=news_request)
    tracked_symbols = stock_db.list_collection_names()
    for announcement in announcements:
        affected_symbol = announcement.target_symbol
        if affected_symbol in tracked_symbols:
            if announcement.ca_type == CorporateActionType.MERGER:
                message = 'Merger detected! Deleting asset: ' + affected_symbol
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
                stock_db.drop_collection(name_or_collection=affected_symbol)
            elif announcement.ca_type == CorporateActionType.SPLIT:
                if announcement.ex_date is None:
                    message = 'Split announcement contains None EX date! '
                    message += 'Symbol: ' + affected_symbol
                    telegram_bot.send_message(
                        text=message, chat_id=CHAT_DECRYPTED)
                elif announcement.ex_date == today:
                    message = 'Split detected! Updating: ' + affected_symbol
                    telegram_bot.send_message(
                        text=message, chat_id=CHAT_DECRYPTED)
                    perform_split(symbol=affected_symbol,
                                  old_rate=announcement.old_rate,
                                  new_rate=announcement.new_rate)
            else:  # SPINOFF
                message = 'Spinoff detected! PANIC: ' + affected_symbol
                telegram_bot.send_message(text=message, chat_id=CHAT_DECRYPTED)
            # Sleep to not overload telegram
            sleep(0.5)


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
        update_market_collection(market_is_open)
        check_announcements()
    except CheckMarketError:
        return
    except Exception as err:
        error_message = 'Unexpected exception: ' + repr(err)
        telegram_bot.send_message(text=error_message, chat_id=CHAT_DECRYPTED)
    finally:
        mongo_client.close()


def perform_split(symbol: str, old_rate: float, new_rate: float) -> None:
    prices = []
    dates = []
    asset_collection = stock_db.get_collection(symbol)
    asset_cursor = asset_collection.find()
    # Update all values, assume EX date is today
    for asset_item in asset_cursor.sort(
            key_or_list='date', direction=ASCENDING):
        prices.append(asset_item['close'] * old_rate / new_rate)
        dates.append(asset_item['date'])

    asset = TrackedAsset(symbol=symbol, date=dates[-1], close=prices[-1])
    stock_db.drop_collection(name_or_collection=symbol)
    sleep(0.1)
    asset.calculate_macd(prices=prices, dates=dates, db_client=mongo_client)
    asset.calculate_rsi(prices=prices, dates=dates, db_client=mongo_client)
    asset.calculate_ema_big_long(
        prices=prices, dates=dates, db_client=mongo_client)


def update_market_collection(market_is_open: bool) -> None:
    update_dict = {
        'market_is_open': market_is_open,
        'day_of_month': today.day
    }

    market_db = mongo_client['market']
    market_collection = market_db['MARKET_DATA']
    market_collection.update_one(
        filter={'my_id': environ.get('MARKET_COLLECTION_ID')},
        update={'$set': update_dict})


class CheckMarketError(Exception):
    pass
