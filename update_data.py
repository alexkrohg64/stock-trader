"""Update technical analysis data"""
import json

tracked_assets = []

with open(file='assets.json', mode='r', encoding='utf-8') as file:
    tracked_assets = json.load(file)

for asset in tracked_assets:
    print(asset)
    print(asset['macd'])
    print(asset['rsi'].pop(4))
    print(asset['ema_big_long'])
