"""Update technical analysis data"""
import json

tracked_assets = []

with open(file='assets.json', mode='r', encoding='utf-8') as asset_file:
    tracked_assets = json.load(asset_file)

for asset in tracked_assets:
    print(asset)
    print(asset['macd'])
    print(asset['rsi'].pop(4))
    print(asset['ema_big_long'])
