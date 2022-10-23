"""Custom asset module for tracking technical analysis data"""
from datetime import datetime
from pymongo import MongoClient

EMA_BIG_LONG_PERIOD = 200
DATA_FRONT_LOAD = 50
MACD_LONG_PERIOD = 26
MACD_SHORT_PERIOD = 12
MACD_SIGNAL_PERIOD = 9
RSI_PERIOD = 14
RSI_QUEUE = 5
TREND_QUEUE = 20
VOLUME_THRESHOLD = 5000000

SMOOTH_9 = 0.2
SMOOTH_12 = 0.153846153846
SMOOTH_26 = 0.074074074074
SMOOTH_200 = 0.0099502487562189


class TrackedAsset:
    """Custom asset class for tracking technical analysis data"""
    def __init__(self, symbol: str, ema_short: float = 0.0,
                 ema_long: float = 0.0, macd: float = 0.0,
                 macd_signal: float = 0.0, average_gains: float = 0.0,
                 average_losses: float = 0.0, rsi: list[float] = None,
                 ema_big_long: float = 0.0, trend: list[bool] = None,
                 date: datetime = None, close: float = 0.0):
        self.symbol = symbol
        self.ema_short = ema_short
        self.ema_long = ema_long
        self.macd = macd
        self.macd_signal = macd_signal
        self.average_gains = average_gains
        self.average_losses = average_losses
        if rsi is None:
            self.rsi = []
        else:
            self.rsi = rsi
        self.ema_big_long = ema_big_long
        if trend is None:
            self.trend = []
        else:
            self.trend = trend
        self.date = date
        self.close = close

    def __repr__(self):
        return self.symbol

    def __str__(self):
        return self.symbol

    def calculate_ema_big_long(
            self, prices: list, dates: list, db_client: MongoClient) -> None:
        """Calculate long-period trend line"""
        self.ema_big_long = sum(
            prices[0:EMA_BIG_LONG_PERIOD]) / EMA_BIG_LONG_PERIOD

        for i in range(EMA_BIG_LONG_PERIOD, len(prices)):
            self.ema_big_long = ((prices[i] - self.ema_big_long) * SMOOTH_200
                                 + self.ema_big_long)
            if i > len(prices) - DATA_FRONT_LOAD - TREND_QUEUE - 1:
                self.trend.append(prices[i] > self.ema_big_long)
                if i > len(prices) - DATA_FRONT_LOAD - 1:
                    self.trend.pop(0)
                    update = {
                        '$set': {
                            'ema_big_long': self.ema_big_long,
                            'trend': self.trend
                        }
                    }
                    self.update_db(
                        filter_date=dates[i],
                        update=update,
                        db_client=db_client)

        print('EMA-200: ' + repr(self.ema_big_long))
        print(self.trend)
        print(self.date)
        print(self.close)

    def calculate_macd(
            self, prices: list, dates: list, db_client: MongoClient) -> None:
        """Calculate MACD-related values"""
        self.ema_short = sum(prices[0:MACD_SHORT_PERIOD]) / MACD_SHORT_PERIOD
        self.ema_long = sum(prices[0:MACD_LONG_PERIOD]) / MACD_LONG_PERIOD
        macd_signal_sum = 0.0

        for i in range(MACD_SHORT_PERIOD, MACD_LONG_PERIOD):
            self.ema_short = ((prices[i] - self.ema_short) * SMOOTH_12
                              + self.ema_short)

        for i in range(MACD_LONG_PERIOD, len(prices)):
            self.ema_short = ((prices[i] - self.ema_short) * SMOOTH_12
                              + self.ema_short)
            self.ema_long = ((prices[i] - self.ema_long) * SMOOTH_26
                             + self.ema_long)

            if i < MACD_LONG_PERIOD + MACD_SIGNAL_PERIOD:
                macd_signal_sum += self.ema_short - self.ema_long
                if i == MACD_LONG_PERIOD + MACD_SIGNAL_PERIOD - 1:
                    self.macd_signal = macd_signal_sum / MACD_SIGNAL_PERIOD
            else:
                self.macd_signal = (SMOOTH_9
                                    * (self.ema_short
                                       - self.ema_long
                                       - self.macd_signal)
                                    + self.macd_signal)

            if i >= len(prices) - DATA_FRONT_LOAD:
                new_doc = {
                    'symbol': self.symbol,
                    'date': dates[i],
                    'close': prices[i],
                    'ema_short': self.ema_short,
                    'ema_long': self.ema_long,
                    'macd': self.ema_short - self.ema_long,
                    'macd_signal': self.macd_signal
                }
                db_client['stocks'][self.symbol].insert_one(document=new_doc)

        self.macd = self.ema_short - self.ema_long
        print(self.symbol)
        print('MACD: ' + repr(self.macd))
        print('Signal: ' + repr(self.macd_signal))

    def calculate_rsi(
            self, prices: list, dates: list, db_client: MongoClient) -> None:
        """Calculate RSI-related values"""
        sum_gains = 0.0
        sum_losses = 0.0

        for i in range(RSI_PERIOD):
            change = prices[i+1] - prices[i]

            if change >= 0:
                sum_gains += change
            else:
                sum_losses -= change

        self.average_gains = sum_gains / RSI_PERIOD
        self.average_losses = sum_losses / RSI_PERIOD

        for i in range(RSI_PERIOD, len(prices) - 1):
            change = prices[i+1] - prices[i]
            self.update_gains_and_losses(change)

            if i > (len(prices) - DATA_FRONT_LOAD - RSI_QUEUE - 2):
                rsi_value = 100 - 100 / (
                    1 + self.average_gains / self.average_losses)
                self.rsi.append(rsi_value)
                if i > (len(prices) - DATA_FRONT_LOAD - 2):
                    self.rsi.pop(0)
                    update = {
                        '$set': {
                            'average_gains': self.average_gains,
                            'average_losses': self.average_losses,
                            'rsi': self.rsi
                        }
                    }
                    self.update_db(
                        filter_date=dates[i+1],
                        update=update,
                        db_client=db_client)

        print('RSI: ' + repr(self.rsi))

    @staticmethod
    def has_enough_volume(bars) -> bool:
        """Check if average daily volume meets configured threshold"""
        volumes = [candle.volume for candle in bars]
        average_volume = sum(volumes) / len(volumes)
        return average_volume >= VOLUME_THRESHOLD

    def update_gains_and_losses(self, change) -> None:
        """Common logic to update RSI-related values"""
        if change >= 0:
            self.average_gains = (
                (RSI_PERIOD - 1) * self.average_gains + change) / RSI_PERIOD
            self.average_losses = (
                (RSI_PERIOD - 1) * self.average_losses) / RSI_PERIOD
        else:
            self.average_gains = (
                (RSI_PERIOD - 1) * self.average_gains) / RSI_PERIOD
            self.average_losses = (
                (RSI_PERIOD - 1) * self.average_losses - change) / RSI_PERIOD

    def update_db(
            self, filter_date: datetime, update: dict,
            db_client: MongoClient) -> None:
        collection = db_client['stocks'][self.symbol]
        collection.update_one(filter={'date': filter_date}, update=update)

    def update_stats(self, new_price, new_date) -> None:
        """Update technical analysis data"""
        self.ema_short = ((new_price - self.ema_short) * SMOOTH_12
                          + self.ema_short)
        self.ema_long = ((new_price - self.ema_long) * SMOOTH_26
                         + self.ema_long)
        self.macd = self.ema_short - self.ema_long
        self.macd_signal = ((self.macd - self.macd_signal) * SMOOTH_9
                            + self.macd_signal)

        change = new_price - self.close
        self.update_gains_and_losses(change=change)
        self.rsi.pop(0)
        rsi_value = 100 - 100 / (1 + self.average_gains / self.average_losses)
        self.rsi.append(rsi_value)

        self.ema_big_long = ((new_price - self.ema_big_long) * SMOOTH_200
                             + self.ema_big_long)
        self.trend.pop(0)
        self.trend.append(new_price > self.ema_big_long)

        self.date = new_date
        self.close = new_price
