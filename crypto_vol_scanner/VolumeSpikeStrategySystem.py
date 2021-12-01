import backtrader as bt


class VolumeStrategy(bt.Strategy):
    params = (
        ("MaxTriggerNumber", int(1)),
        ("VolAvgLength", int(50)),
        ("TriggerPriceMultiplier", 10.0),
        ("TriggerVolumeMultiplier", 10.0),
        ("MAX_ORDER", 100),
    )

    def log(self, Trade="", Price="", Size="", Cost="", Commission="", dt=None):
        """Logging function for this strategy"""
        dt = dt or self.datas[0].datetime.date(0)
        with open(self.RESULTS_FOLDER + "\\" + "temp.csv", "a") as f:
            f.write(f"{dt},{Trade},{Price},{Size},{Cost},{Commission}\n")
        # print("%s, %s" % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the lines in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.datavolume = self.datas[0].volume

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # Add trigger variables
        self.initial_trigger = False
        self.trigger_number = 0
        self.vol_avg = bt.indicators.Average(
            self.datavolume,
            period=self.params.VolAvgLength,
            plot=True,
            plotname=str(self.params.VolAvgLength) + "d Vol-Avg",
        )
        self.vol_1d_lag = self.datavolume * 10
        self.initial_price = 0
        self.target_price = 0

        self.RESULTS_FOLDER = (
            r"C:\Users\justi\crypto-vol-scanner\crypto_vol_scanner\results"
        )
        with open(self.RESULTS_FOLDER + "\\" + "temp.csv", "w") as f:
            f.write("Date,Trade,Price,Size,Cost,Commission\n")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    Trade="BUY",
                    Price=order.executed.price,
                    Size=order.executed.size,
                    Cost=order.executed.value,
                    Commission=order.executed.comm,
                )
                self.buyprice = order.executed.price
                self.target_price = self.buyprice * self.params.TriggerPriceMultiplier
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log(
                    Trade="SELL",
                    Price=order.executed.price,
                    Size=order.executed.size,
                    Cost=order.executed.value,
                    Commission=order.executed.comm,
                )

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            pass
            # self.log("Order Canceled/Margin/Rejected")

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        # self.log("OPERATION PROFIT, GROSS %.2f, NET %.2f" % (trade.pnl, trade.pnlcomm))

    def next(self):
        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            if (
                self.datavolume > self.vol_1d_lag * self.params.TriggerVolumeMultiplier
                and self.initial_trigger == False
            ):
                self.initial_trigger = True
                # worst possible price
                self.initial_price = self.datahigh

                # BUY, BUY, BUY!!!
                # Keep track of the created order to avoid a 2nd order
                self.buy(
                    size=self.params.MAX_ORDER,
                    price=self.datahigh,
                    exectype=bt.Order.Limit,
                )

        else:
            # add a sell order once the buy order is filled
            if (
                self.initial_trigger == True
                and self.trigger_number < self.params.MaxTriggerNumber
            ):
                # Get limit order created to sell too
                # self.log("SELL CREATE, %.2f" % self.target_price)
                # Keep track of the created order to avoid a 2nd order
                self.sell(
                    size=self.params.MAX_ORDER,
                    price=self.target_price,
                    exectype=bt.Order.Limit,
                )
                self.trigger_number += 1

        self.vol_1d_lag = self.vol_avg[0]
