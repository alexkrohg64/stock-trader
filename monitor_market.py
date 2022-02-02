"""Check market file has been updated every day"""
import datetime
import os
import telegram

mod_time_since_epoch = os.path.getmtime('market.json')

mod_time = datetime.date.fromtimestamp(mod_time_since_epoch)

today = datetime.date.today()

if mod_time < today:
    telegram_bot = telegram.Bot(token=os.environ['TGM_BOT_TOKEN'])
    ERROR_MESSAGE = 'Market file should have been updated but was not!'
    telegram_bot.send_message(text=ERROR_MESSAGE, chat_id=os.environ['TGM_CHAT_ID'])
