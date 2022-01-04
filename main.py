from alpaca_trade_api.rest import REST, TimeFrame

api = REST()
bars = api.get_bars(symbol="AAPL", timeframe=TimeFrame.Day, start="2021-06-01", end="2021-06-11")

print(len(bars))