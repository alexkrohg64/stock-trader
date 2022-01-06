"""Custom asset module for tracking technical analysis data"""
EMA_BIG_LONG_PERIOD = 200
MACD_LONG_PERIOD = 26
MACD_SHORT_PERIOD = 12
MACD_SIGNAL_PERIOD = 9
RSI_PERIOD = 14
VOLUME_THRESHOLD = 20000000

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

    def __repr__(self):
        return self.symbol

    def __str__(self):
        return self.symbol

    def calculate_macd(self, bars):
        """Calculate MACD-related values"""
        self.macd = bars[0].c
        self.macd_signal = bars[0].c

    @staticmethod
    def has_enough_volume(bars):
        """Check if average daily volume meets configured threshold"""
        volumes = [candle.v for candle in bars]
        average_volume = sum(volumes) / len(volumes)
        return average_volume >= VOLUME_THRESHOLD
    