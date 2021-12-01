import backtrader as bt


class MyStrategy(bt.Strategy):
    def __init__(self):
        # self.datavolume = self.datas[0].volume
        # self.dataclose = self.datas[0].close
        print(self.data)
        self.sma = bt.indicators.Average(self.data.volume, period=20)

    def next(self):

        if self.sma > self.data.close:
            self.buy()

        elif self.sma < self.data.close:
            self.sell()
