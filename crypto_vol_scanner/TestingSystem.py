import os.path
import os

from backtrader import trade
from VolumeSpikeStrategySystem import VolumeStrategy
import backtrader as bt


from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import datetime as dt

from borb.pdf.document import Document
from borb.pdf.page.page import Page
from borb.pdf.pdf import PDF
from borb.pdf.canvas.layout.text.paragraph import Paragraph
from borb.pdf.canvas.layout.page_layout.multi_column_layout import SingleColumnLayout
from borb.io.read.types import Decimal
from borb.pdf.canvas.layout.table.fixed_column_width_table import (
    FixedColumnWidthTable as Table,
)
from borb.pdf.canvas.layout.table.table import TableCell
from borb.pdf.canvas.color.color import X11Color
from borb.pdf.canvas.layout.layout_element import Alignment
from borb.pdf.canvas.layout.image.image import Image
from borb.pdf.page.page_size import PageSize


# class extensions for additional data
class PandasData(bt.feed.DataBase):
    """
    The ``dataname`` parameter inherited from ``bt.feed.DataBase`` is the pandas
    DataFrame
    """

    params = (
        # Possible values for datetime (must always be present)
        #  None : datetime is the "index" in the Pandas Dataframe
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ("datetime", None),
        # Possible values below:
        #  None : column not present
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ("open", -1),
        ("high", -1),
        ("low", -1),
        ("close", -1),
        ("volume", -1),
        ("openinterest", None),
    )


def saveplots(
    cerebro,
    numfigs=1,
    iplot=True,
    start=None,
    end=None,
    width=32,
    height=24,
    dpi=300,
    tight=True,
    use=None,
    file_path="",
    **kwargs,
):
    from backtrader import plot

    if cerebro.p.oldsync:
        plotter = plot.Plot_OldSync(
            **kwargs,
            width=32,
            height=24,
        )
    else:
        plotter = plot.Plot(
            **kwargs,
            width=32,
            height=24,
        )

    figs = []
    for stratlist in cerebro.runstrats:
        for si, strat in enumerate(stratlist):
            rfig = plotter.plot(
                strat,
                figid=si * 100,
                numfigs=numfigs,
                iplot=False,  # iplot,
                start=start,
                end=end,
                use=use,
                fmt_x_ticks="%Y-%b-%d",
                fmt_x_data="%Y-%b-%d",
            )
            figs.append(rfig)

    for fig in figs:
        for f in fig:
            f.savefig(file_path, bbox_inches="tight")
    return figs


# Data Feed
directory = "C:\\Users\\justi\\crypto-vol-scanner\\downloaded_data\\dad"
files = Path(directory).glob("*.csv")
RESULTS_FOLDER = r"C:\Users\justi\crypto-vol-scanner\crypto_vol_scanner\results"

# clear out old data
files_for_deletion = Path(RESULTS_FOLDER).glob("*.png")
for file in files_for_deletion:
    os.remove(file)

# initiate counts and list for data after running
symbolCount = 0
symbols = []
winnerCount = 0
loserPCT = []
winnerPCT = []
loserCount = 0
logs = []
tradeLogs = pd.DataFrame({})

# run through the entire folder of data files
for file in files:
    symbolCount += 1
    # put data into backtrader cerebro
    df = pd.read_csv(file, index_col=0, parse_dates=True)
    data_feed = bt.feeds.PandasData(
        dataname=df, timeframe=bt.TimeFrame.Days, compression=1
    )
    cerebro = bt.Cerebro()
    cerebro.broker.set_cash(10000)
    cerebro.adddata(data_feed)
    strategy_params = (
        ("MaxTriggerNumber", int(1)),
        ("VolAvgLength", int(50)),
        ("TriggerPriceMultiplier", 10.0),
        ("TriggerVolumeMultiplier", 10.0),
        ("MAX_ORDER", 100),
    )

    cerebro.addstrategy(VolumeStrategy)
    Symbol = os.path.basename(os.path.normpath(file))
    Symbol = Symbol.replace(".csv", "")
    Symbol = Symbol.split(" ")[-1]
    print("Crypto Symbol: " + Symbol)
    logs.append("Crypto Symbol: " + Symbol)
    symbols.append(Symbol)
    Start = "%.2f" % round(cerebro.broker.getvalue())
    print("Starting Portfolio Value: " + Start)
    logs.append("Starting Portfolio Value: " + Start)
    cerebro.run()
    End = "%.2f" % round(cerebro.broker.getvalue(), 2)
    print("Final Portfolio Value: " + End)
    logs.append("Final Portfolio Value: " + End)

    # getting the percent change
    # pctChangeFloat = float(End) / float(Start) - 1
    # if pctChangeFloat > 0:
    #     winnerCount += 1
    #     winnerPCT.append(pctChangeFloat)
    # else:
    #     loserCount += 1
    #     loserPCT.append(pctChangeFloat)
    # pctChange = "{:.2%}".format(pctChangeFloat)

    # print("Percent change: " + pctChange)
    # logs.append("Percent change: " + pctChange)
    # print()
    # logs.append("")

    # saving plots
    saveplots(cerebro, file_path=RESULTS_FOLDER + "\\" + Symbol + ".png")
    # recover data from temp.csv file
    tempLogs = pd.read_csv(RESULTS_FOLDER + "\\temp.csv", index_col=0)
    tempLogs["Symbol"] = Symbol
    tradeLogs = tradeLogs.append(tempLogs)


# # printing winners, losers, and averages
# winnerPCTCount = winnerCount / symbolCount
# print("This strategy had " + "{:.2%}".format(winnerPCTCount) + " winners")
# logs.append("This strategy had " + "{:.2%}".format(winnerPCTCount) + " winners")
# if winnerCount > 0:
#     avgWinnerPCT = sum(winnerPCT) / len(winnerPCT)
# else:
#     avgWinnerPCT = 0
# print("The average winner: +" + "{:.2%}".format(avgWinnerPCT))
# logs.append("The average winner: +" + "{:.2%}".format(avgWinnerPCT))
# loserPCTCount = loserCount / symbolCount
# print("This strategy had " + "{:.2%}".format(loserPCTCount) + " losers")
# logs.append("This strategy had " + "{:.2%}".format(loserPCTCount) + " losers")
# avgLoserPCT = sum(loserPCT) / len(loserPCT)
# print("The average loser: " + "{:.2%}".format(avgLoserPCT))
# logs.append("The average loser: " + "{:.2%}".format(avgLoserPCT))
# print()

# # printing total portfolio change
# portfolioChange = sum(loserPCT + winnerPCT) / len(loserPCT + winnerPCT)
# pfChange = "{:.2%}".format(portfolioChange)
# if portfolioChange > 0:
#     pfChange = "+" + pfChange
# print("Total portfolio change: " + pfChange)
# logs.append("Total portfolio change: " + pfChange)

# logging results to file
log_file = RESULTS_FOLDER + "\\" + "logs.txt"
with open(log_file, "w") as f:
    for log in logs:
        f.write(log + "\n")


tradeLogs.to_csv(RESULTS_FOLDER + "\\" + "tradeLogs.csv")

# turning files into .pdf
symbols.sort(key=str.lower)  # alphabetize
no_trades = 0
only_buys = 0
successful_trades = 0
document = Document()
title_page = Page(PageSize.A4_LANDSCAPE.value[0], PageSize.A4_LANDSCAPE.value[1])

# Layout
layout = SingleColumnLayout(title_page)

# Create and add heading
layout.add(
    Paragraph(
        "Trading Armstrong Effect Analysis", font="Helvetica", font_size=Decimal(20)
    )
)

layout.add(
    Paragraph(f"Initialization Parameters:", font="Helvetica", font_size=Decimal(12))
)
for parameter in strategy_params:
    layout.add(
        Paragraph(
            f"{strategy_params[0]} = {strategy_params[1]}",
            font="Helvetica",
            font_size=Decimal(12),
        )
    )

# Append page
document.append_page(title_page)

for symbol in symbols:
    chart = RESULTS_FOLDER + "\\" + symbol + ".png"

    # only show the matching symbol
    is_symbol = tradeLogs["Symbol"] == symbol
    trades = tradeLogs[is_symbol]
    del trades["Cost"]
    print(f"{symbol} trade count: {trades.shape[0]}")
    page = Page(PageSize.A4_LANDSCAPE.value[0], PageSize.A4_LANDSCAPE.value[1])
    layout = SingleColumnLayout(page)
    layout.add(Paragraph(f"Symbol: {symbol}", font="Helvetica", font_size=Decimal(16)))

    if trades.shape[0] == 0:
        # no trades made, pattern not identified
        no_trades += 1
        summary = f"No trades were executed for {symbol} as the specified volume surge was not identified."
    elif trades.shape[0] == 1:
        # only entering trade, specified exit not achieved
        only_buys += 1
        summary = f"An entry (buy) was executed for {symbol}, but the position was not exited by the end of the data."
    else:
        # buy and sell trades
        successful_trades += 1
        # get the buy and sell dates
        trade_dates = trades.index.tolist()
        first_date = dt.datetime.strptime(trade_dates[0], "%Y-%m-%d").date()
        last_date = dt.datetime.strptime(trade_dates[1], "%Y-%m-%d").date()
        duration = (last_date - first_date).days
        is_buy = trades["Trade"] == "BUY"
        is_sell = trades["Trade"] == "SELL"
        buy_price = trades[is_buy]["Price"].to_numpy()[0]
        sell_price = trades[is_sell]["Price"].to_numpy()[0]

        ROI = sell_price / buy_price - 1
        ARR = (ROI + 1) ** (1 / (duration / 365)) - 1

        summary = (
            f"A successful entry and exit for {symbol} was completed. "
            + f"The ROI was {ROI:.2f}x and was closed in {duration:d} days for an Annual Rate of Return of {ARR:.4f}"
        )
    # add summary
    layout.add(Paragraph(f"{summary}", font="Helvetica", font_size=Decimal(14)))

    # add table of data
    if trades.shape[0] > 0:
        table = Table(
            number_of_rows=(trades.shape[0] + 1),
            number_of_columns=(trades.shape[1] + 1),
        )
        table_header = trades.columns.values.tolist()
        table_data = trades.values.tolist()
        table.add(
            TableCell(
                Paragraph("Date", font_color=X11Color("White")),
                background_color=X11Color("SlateGray"),
            )
        )
        for item in table_header:
            table.add(
                TableCell(
                    Paragraph(item, font_color=X11Color("White")),
                    background_color=X11Color("SlateGray"),
                )
            )
        dates = trades.index.tolist()
        for (row, date) in zip(table_data, dates):
            table.add(Paragraph(str(date)))
            table.add(Paragraph(row[0]))
            table.add(Paragraph(f"{row[1]:.5f}"))
            table.add(Paragraph(f"{row[2]:d}"))
            table.add(Paragraph(f"{row[3]:.5f}"))
            table.add(Paragraph(str(row[4])))
        # Set padding
        table.set_padding_on_all_cells(Decimal(2), Decimal(2), Decimal(2), Decimal(2))
        layout.add(table)

    # add chart of trade data
    layout.add(
        Image(
            Path(chart),
            width=Decimal(160 * 3),
            height=Decimal(90 * 3),
            horizontal_alignment=Alignment.CENTERED,
        )
    )
    # Append page
    document.append_page(page)

assert len(symbols) == (no_trades + only_buys + successful_trades)

pdf_name = RESULTS_FOLDER + "\\results.pdf"
# Persist PDF to file
with open(pdf_name, "wb") as pdf_file_handle:
    PDF.dumps(pdf_file_handle, document)
