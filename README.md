# stock-trader

Generate passive income by automatically analyzing and trading stocks. Perform analysis and trades daily.

These python functions are designed to run in AWS Lambda on a pre-determined schedule, and store data in a MongoDB database.

# Dependencies

The following python modules are required to be installed as Lambda layers:
 - alpaca-py
 - python-telegram-bot
 - pymongo[srv]
 - pymongo[aws]
 - data (zip and add layer from this repository's data directory)
