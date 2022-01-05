from alpaca_trade_api.rest import REST, TimeFrame

api = REST()

assets = api.list_assets(status='active', asset_class='us_equity')
symbols = [asset.symbol for asset in assets if asset.tradable]
print(len(symbols))