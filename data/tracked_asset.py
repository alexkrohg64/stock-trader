"""Custom asset module for tracking technical analysis data"""
EMA_BIG_LONG_PERIOD = 200
MACD_LONG_PERIOD = 26
MACD_SHORT_PERIOD = 12
MACD_SIGNAL_PERIOD = 9
RSI_PERIOD = 14
VOLUME_THRESHOLD = 5000000

SMOOTH_9 = 0.2
SMOOTH_12 = 0.153846153846
SMOOTH_26 = 0.074074074074
SMOOTH_200 = 0.0099502487562189

class TrackedAsset:
    """Custom asset class for tracking technical analysis data"""
    def __init__(self, code):
        self.symbol = code
        self.macd = 0.0
        self.macd_signal = 0.0
        self.rsi = 0.0

    def __repr__(self):
        return self.symbol

    def __str__(self):
        return self.symbol

    def calculate_macd(self, bars):
        """Calculate MACD-related values"""
        prices = [candle.c for candle in bars]

        ema_short = sum(prices[0:MACD_SHORT_PERIOD]) / MACD_SHORT_PERIOD
        ema_long = sum(prices[0:MACD_LONG_PERIOD]) / MACD_LONG_PERIOD
        macd_signal_sum = 0.0

        for i in range(MACD_SHORT_PERIOD, MACD_LONG_PERIOD):
            ema_short = (bars[i].c - ema_short) * SMOOTH_12 + ema_short

        for i in range(MACD_LONG_PERIOD, len(bars)):
            ema_short = (bars[i].c - ema_short) * SMOOTH_12 + ema_short
            ema_long = (bars[i].c - ema_long) * SMOOTH_26 + ema_long

            if i < MACD_LONG_PERIOD + MACD_SIGNAL_PERIOD:
                macd_signal_sum += ema_short - ema_long
                if i == MACD_LONG_PERIOD + MACD_SIGNAL_PERIOD - 1:
                    self.macd_signal = macd_signal_sum / MACD_SIGNAL_PERIOD
            else:
                self.macd_signal = ((ema_short - ema_long - self.macd_signal) * SMOOTH_9
                                    + self.macd_signal)
                if i == len(bars) - 1:
                    print('MACD: ' + repr(ema_short - ema_long))
                    print('MACD Signal: ' + repr(self.macd_signal))
        print(self.symbol)
        self.macd = ema_short - ema_long

    def calculate_rsi(self, bars):
        """Calculate RSI-related values"""
        self.rsi = bars[0].c

    @staticmethod
    def has_enough_volume(bars):
        """Check if average daily volume meets configured threshold"""
        volumes = [candle.v for candle in bars]
        average_volume = sum(volumes) / len(volumes)
        return average_volume >= VOLUME_THRESHOLD
    