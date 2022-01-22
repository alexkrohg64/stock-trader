"""Custom asset module for tracking technical analysis data"""
from json import JSONEncoder

EMA_BIG_LONG_PERIOD = 200
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
    def __init__(self, code):
        self.symbol = code
        self.ema_short = 0.0
        self.ema_long = 0.0
        self.macd = 0.0
        self.macd_signal = 0.0
        self.average_gains = 0.0
        self.average_losses = 0.0
        self.rsi = []
        self.ema_big_long = 0.0
        self.trend = []

    def __repr__(self):
        return self.symbol

    def __str__(self):
        return self.symbol

    def calculate_ema_big_long(self, prices):
        """Calculate long-period trend line"""
        self.ema_big_long = sum(prices[0:EMA_BIG_LONG_PERIOD]) / EMA_BIG_LONG_PERIOD

        for i in range(EMA_BIG_LONG_PERIOD, len(prices) - TREND_QUEUE):
            self.ema_big_long = (prices[i] - self.ema_big_long) * SMOOTH_200 + self.ema_big_long

        for i in range(len(prices) - TREND_QUEUE, len(prices)):
            self.ema_big_long = (prices[i] - self.ema_big_long) * SMOOTH_200 + self.ema_big_long
            self.trend.append(prices[i] > self.ema_big_long)

        print('EMA-200: ' + repr(self.ema_big_long))
        print(self.trend)

    def calculate_macd(self, prices):
        """Calculate MACD-related values"""
        self.ema_short = sum(prices[0:MACD_SHORT_PERIOD]) / MACD_SHORT_PERIOD
        self.ema_long = sum(prices[0:MACD_LONG_PERIOD]) / MACD_LONG_PERIOD
        macd_signal_sum = 0.0

        for i in range(MACD_SHORT_PERIOD, MACD_LONG_PERIOD):
            self.ema_short = (prices[i] - self.ema_short) * SMOOTH_12 + self.ema_short

        for i in range(MACD_LONG_PERIOD, len(prices)):
            self.ema_short = (prices[i] - self.ema_short) * SMOOTH_12 + self.ema_short
            self.ema_long = (prices[i] - self.ema_long) * SMOOTH_26 + self.ema_long

            if i < MACD_LONG_PERIOD + MACD_SIGNAL_PERIOD:
                macd_signal_sum += self.ema_short - self.ema_long
                if i == MACD_LONG_PERIOD + MACD_SIGNAL_PERIOD - 1:
                    self.macd_signal = macd_signal_sum / MACD_SIGNAL_PERIOD
            else:
                self.macd_signal = ((self.ema_short - self.ema_long - self.macd_signal) * SMOOTH_9
                                    + self.macd_signal)

        self.macd = self.ema_short - self.ema_long
        print(self.symbol)
        print('MACD: ' + repr(self.macd))
        print('Signal: ' + repr(self.macd_signal))

    def calculate_rsi(self, prices):
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

            if change >= 0:
                self.average_gains = ((RSI_PERIOD - 1) * self.average_gains + change) / RSI_PERIOD
                self.average_losses = (RSI_PERIOD - 1) * self.average_losses / RSI_PERIOD
            else:
                self.average_gains = (RSI_PERIOD - 1) * self.average_gains / RSI_PERIOD
                self.average_losses = ((RSI_PERIOD - 1) * self.average_losses - change) / RSI_PERIOD

            if i > (len(prices) - RSI_QUEUE - 2):
                rsi_value = 100 - 100 / (1 + self.average_gains / self.average_losses)
                self.rsi.append(rsi_value)

        print('RSI: ' + repr(self.rsi))

    @staticmethod
    def has_enough_volume(bars):
        """Check if average daily volume meets configured threshold"""
        volumes = [candle.v for candle in bars]
        average_volume = sum(volumes) / len(volumes)
        return average_volume >= VOLUME_THRESHOLD

class AssetEncoder(JSONEncoder):
    """Serialize custom object"""
    def default(self, o):
        return o.__dict__
