"""Migrate technical analysis data"""
import boto3
import datetime
from decimal import Decimal
# Non-standard imports
from data import tracked_asset

table = boto3.resource('dynamodb').Table('assets')
table.load()
index_table = boto3.resource('dynamodb').Table('asset_list')
index_table.load()
client = boto3.client('dynamodb')

tracked_assets = []

db_items = table.scan()['Items']

for db_item in db_items:
    if db_item['symbol'] == 'MARKET_IS_OPEN':
        continue
    db_date = db_item['latest_date']
    latest_date = datetime.date(year=db_date['year'], month=db_date['month'],
        day=db_date['day'])

    tracked_assets.append(tracked_asset.TrackedAsset(symbol=db_item['symbol'],
        ema_short=float(db_item['ema_short']), ema_long=float(db_item['ema_long']),
        macd=float(db_item['macd']), macd_signal=float(db_item['macd_signal']),
        average_gains=float(db_item['average_gains']), average_losses=float(db_item['average_losses']),
        rsi=db_item['rsi'], ema_big_long=float(db_item['ema_big_long']),
        trend=db_item['trend'], latest_date=latest_date, latest_close=float(db_item['latest_close'])))

for asset in tracked_assets:
    table_name = asset.symbol + '_TABLE'
    asset.rsi = [float(rsi_value) for rsi_value in asset.rsi]

    raw_dict = asset.__dict__
    numbers_dict = {k:Decimal(str(v)) for k, v in raw_dict.items() if isinstance(v, float)}
    others_dict = {k:v for k, v in raw_dict.items() if not isinstance(v, float)}
    for index, rsi_value in enumerate(others_dict['rsi']):
        others_dict['rsi'][index] = Decimal(str(rsi_value))
    others_dict['date'] = asset.latest_date.strftime('%Y%m%d')
    del others_dict['latest_date']
    updated_dict = {**numbers_dict, **others_dict}

    client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'date',
                'AttributeType': 'S'
            }
        ],
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'date',
                'KeyType': 'HASH'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    asset_table = boto3.resource('dynamodb').Table(table_name)
    asset_table.wait_until_exists()
    asset_table.put_item(Item=updated_dict)
    index_table.put_item(
        Item={
            'symbol': asset.symbol
        }
    )
