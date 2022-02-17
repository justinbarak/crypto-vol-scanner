import os
import backtrader as bt
from numpy import datetime64
import pandas as pd
from pathlib import Path
from datetime import datetime
import math


class VolumeStrategy(bt.Strategy):
    """
    This strategy will buy and sell small volumes at many different pricing\n
    multiples to try and establish what the optimal strategy would be across\n
     many different options.
    Iterate through the strategy with several `VolAvgLength: int` and \n
    `TriggerVolumeMultiplier: float` values. Always update the `CurrencyID`\n
     value to allow proper logging and future data retrieval.
    """

    params = (
        ("VolAvgLength", int(50)),
        ("TriggerPriceMultipliers", list(range(4, 22, 2))),
        ("TriggerVolumeMultiplier", 10.0),
        ("OnlyTransparent", False),
        ("CurrencyID", "NONE"),
        ("save_path", Path("./_temp_df_.pkl").resolve()),
    )

    def log(
        self,
        Trade: str,
        Price: float,
        PriceMultiplier: int,
        # dt: datetime = None,
    ):
        """Logging function for this strategy
        columns=["buy_date", "sell_date", "buy_price", "sale_price", "currency_id","run_name","status",
                "sell_price_multiple","buy_volume_multiple","volume_avg_duration","transparent", "buy_avg_volume"]
        """
        dt = self.datas[0].datetime.date(0)
        name = f"{self.RUN_NAME}".replace(",,", f",{PriceMultiplier},")
        if Trade == "BUY":
            self.log_df = self.log_df.append(
                {
                    "buy_date": datetime64(dt),
                    "sell_date": datetime(
                        1900, 1, 1
                    ),  # placeholder until the trade is closed
                    "buy_price": Price,
                    "sale_price": -1,  # placeholder until the trade is closed
                    "currency_id": self.params.CurrencyID,
                    "run_name": name,
                    "status": "opened",
                    "sell_price_multiple": PriceMultiplier,
                    "buy_volume_multiple": self.params.TriggerVolumeMultiplier,
                    "volume_avg_duration": self.params.VolAvgLength,
                    "transparent": self.params.OnlyTransparent,
                    "buy_avg_volume": self.vol_avg[0],
                },
                ignore_index=True,
                sort=False,
            )
        elif Trade == "SELL":
            # verify the trade was opened
            if len(self.log_df[self.log_df["run_name"] == name]) == 1:
                row_index = self.log_df.index[self.log_df["run_name"] == name].tolist()[0]
                self.log_df.at[row_index, "sell_date"] = datetime64(dt)

                # cap multiple at lower of price and theoretical max exit price
                Price = min(
                    Price,
                    self.log_df.at[row_index, "buy_price"]
                    * self.log_df.at[row_index, "sell_price_multiple"],
                )

                self.log_df.at[row_index, "sale_price"] = Price
                self.log_df.at[row_index, "status"] = "closed"
            else:
                raise (ValueError("Trade was not opened but is being closed"))
        else:
            raise (ValueError("Trade must be either BUY or SELL"))
        # re-pickle the dataframe at the end of each log update
        self.log_df.to_pickle(self.df_save_path)

    def __init__(self):
        # Keep a reference to the lines in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.datavolume = self.datas[0].volume

        # To keep track of pending orders and buy price/commission
        self.order = []
        self.buyprice = None
        self.buycomm = None

        # Add trigger variables
        self.initial_trigger = False
        self.vol_avg = SMA(
            self.datavolume,
            period=self.params.VolAvgLength,
            # plot=True,
            # plotname=str(self.params.VolAvgLength) + "d Vol-Avg",
        )

        # trigger price multiplier will replace in the log function
        self.RUN_NAME = f"{self.params.CurrencyID}({self.params.VolAvgLength:d},,{self.params.TriggerVolumeMultiplier:.1f},{self.params.OnlyTransparent})"

        self.log_df = pd.DataFrame(
            columns=[
                "buy_date",
                "sell_date",
                "buy_price",
                "sale_price",
                "currency_id",
                "run_name",
                "status",
                "sell_price_multiple",
                "buy_volume_multiple",
                "volume_avg_duration",
                "transparent",
                "buy_avg_volume",
            ]
        )
        # clean up previous run
        self.df_save_path = self.params.save_path
        # if os.path.exists(self.df_save_path):
        #     os.remove(self.df_save_path)
        self.log_df.to_pickle(self.df_save_path)
        # print("finished strategy.__init__")

    def notify_order(self, order: bt.Order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    Trade="BUY",
                    Price=order.executed.price,
                    PriceMultiplier=order.info.PriceMultiplier,
                    # dt=order.executed.dt,
                )
                buyprice = order.executed.price
                sell_order = self.sell(
                    size=1,
                    exectype=bt.Order.Limit,
                    tradeid=20 + order.ref,
                    price=buyprice * order.info.PriceMultiplier,
                )
                sell_order.addinfo(PriceMultiplier=order.info.PriceMultiplier)
            else:  # Sell
                self.log(
                    Trade="SELL",
                    PriceMultiplier=order.info.PriceMultiplier,
                    Price=order.executed.price,
                )

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            pass

    def next(self):
        # Check if an order is pending ... if yes, we cannot send a 2nd one
        # if self.order:
        #     return

        # Check if we are in the market
        # if not self.position:

        # Not yet ... we MIGHT BUY if ...
        if (
            self.datavolume[0] > (self.vol_avg[0] * self.params.TriggerVolumeMultiplier)
            and self.initial_trigger == False
        ):
            # print("buy triggered")
            self.initial_trigger = True

            # BUY, BUY, BUY!!!
            for i, multiple in enumerate(self.params.TriggerPriceMultipliers):
                order = self.buy(
                    size=1,
                    exectype=bt.Order.Market,
                    tradeid=i,
                )
                order.addinfo(PriceMultiplier=multiple)
                # print(f"buy order {multiple}x issued")

        # else:
        #     pass

        # self.vol_1d_lag = self.vol_avg[0]
        # print(f"{self.datas[0].datetime.date(0)} {self.datavolume[0]} {self.vol_avg[0]}")


class SMA(bt.Indicator):
    lines = ("sma",)
    params = (("period", 45),)

    def __init__(self):
        self.addminperiod(self.params.period)

    def next(self):
        self.lines.sma[0] = (
            math.fsum(self.data.get(size=self.params.period)) / self.params.period
        )
