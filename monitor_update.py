"""Check asset file has been updated on appropriate days"""
import datetime
import json
import os
import sys
import telegram

# Only perform check when the market was open the day before
with open(file='market.json', mode='r', encoding='utf-8') as market_file:
    market_was_open_yesterday = json.load(fp=market_file)
    if not market_was_open_yesterday:
        sys.exit()

mod_time_since_epoch = os.path.getmtime('assets.json')

mod_time = datetime.date.fromtimestamp(mod_time_since_epoch)

today = datetime.date.today()

if mod_time < today:
    telegram_bot = telegram.Bot(token=os.environ['TGM_BOT_TOKEN'])
    ERROR_MESSAGE = 'Asset file should have been updated but was not!'
    telegram_bot.send_message(text=ERROR_MESSAGE, chat_id=os.environ['TGM_CHAT_ID'])
