from dotenv import load_dotenv
import os
from pprint import pprint as pp
import json
import pandas as pd
from pandas.api.types import CategoricalDtype
from collections import OrderedDict
from copy import deepcopy
from tqdm import tqdm as progress_bar
from pathlib import Path
import backtrader as bt
from nomics_bt_strategy import VolumeStrategy
from nomics_data_prep_for_backtrader import load_currency_data, all_currencies
from backtrader.feeds import PandasData
import dill as pickle
import numpy as np
import warnings


class PandasData_normal(PandasData):
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
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
    )


class PandasData_transparent(PandasData):
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
        ("open", "transparent_open"),
        ("high", "transparent_high"),
        ("low", "transparent_low"),
        ("close", "transparent_close"),
        ("volume", "transparent_volume"),
    )


def type_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # pandas has error with datetime64[ns] conversion, useless warning ignored
    warnings.filterwarnings("ignore", category=FutureWarning)

    cat_status = CategoricalDtype(
        categories=["closed", "opened"],
        ordered=True,
    )
    return_df = df.astype(
        {
            "buy_date": "datetime64",
            "sell_date": "datetime64",
            "buy_price": "float64",
            "sale_price": "float64",
            "currency_id": "str",
            "run_name": str,
            "status": cat_status,
            "sell_price_multiple": "int16",
            "buy_volume_multiple": "int16",
            "volume_avg_duration": "int16",
            "transparent": bool,
        },
        copy=True,
    )
    # return_df["buy_date"] = pd.to_datetime(
    #     return_df["buy_date"], infer_datetime_format=True
    # )
    # return_df["sell_date"] = pd.to_datetime(
    #     return_df["sell_date"], infer_datetime_format=True
    # )
    return return_df


results_folder = Path("../bt_data_runs/").resolve()
temp_save_path = results_folder.joinpath(f"_temp_df.pkl")
archive_save_path = results_folder.joinpath(f"archive_df.pkl")

# Option to save existing dataframe and skip already completed currencies
total_redo = False
if total_redo:
    # clear existing archive
    if os.path.exists(archive_save_path):
        os.remove(archive_save_path)
    archive_df = pd.DataFrame()
    completed_currencies = []
else:
    # load existing archive
    if os.path.exists(archive_save_path):
        archive_df = pd.read_pickle(archive_save_path)
        with open(results_folder.joinpath(f"completed_currencies.pkl"), "rb") as f:
            completed_currencies = pickle.load(f)
    else:
        archive_df = pd.DataFrame()
        completed_currencies = []


currencies = all_currencies()
# remove already completed currencies
currencies = [c for c in currencies if c not in completed_currencies]

for currency in progress_bar(currencies, desc="Currencies", position=0):
    # load data
    data_df = load_currency_data(currency)
    data_df.set_index("timestamp", inplace=True)

    data_normal = PandasData_normal(dataname=data_df)
    data_transparent = PandasData_transparent(dataname=data_df)
    df_list = []

    candle_length = len(data_df)

    for VolAvgParam in progress_bar(
        range(5, 95, 5), desc=f"VolAvgParameters-{currency}", leave=False, position=1
    ):
        if VolAvgParam + 2 >= candle_length:
            # avoid errors with averaging
            continue
        for TriggerVolParam in range(6, 20, 2):
            for transparent in [True, False]:
                cerebro = bt.Cerebro()
                # Add the Data Feed to Cerebro
                if transparent:
                    cerebro.adddata(data_transparent)
                else:
                    cerebro.adddata(data_normal)
                # params = (
                #         ("VolAvgLength", int(50)),
                #         ("TriggerPriceMultipliers", list(range(4, 22, 2))),
                #         ("TriggerVolumeMultiplier", 10.0),
                #         ("OnlyTransparent", False),
                #         ("CurrencyID", "NONE"),
                #     )
                strategy_params = {
                    "VolAvgLength": int(VolAvgParam),
                    "TriggerVolumeMultiplier": TriggerVolParam,
                    "CurrencyID": currency,
                    "OnlyTransparent": transparent,
                }
                cerebro.addstrategy(VolumeStrategy, **strategy_params)
                # no commission
                # cerebro.broker.setcommission(commission=0.005) # 0.5% commission
                cerebro.broker.setcash(1000000)
                cerebro.run()
                temp_df = pd.read_pickle(temp_save_path)
                # print(temp_df.head())
                # add to list of df
                df_list.append(deepcopy(temp_df))

    # save temp_df by appending to more permanent df
    if len(df_list) > 0:
        concat_df = type_raw_dataframe(pd.concat(df_list))
        archive_df = archive_df.append(deepcopy(concat_df), ignore_index=True)

    completed_currencies.append(currency)

    # avoid excessive writing to disc
    if len(completed_currencies) % 20 == 0:
        # save archive_df
        archive_df.to_pickle(archive_save_path)
        with open(results_folder.joinpath(f"completed_currencies.pkl"), "wb") as f:
            pickle.dump(completed_currencies, f)

    if len(completed_currencies) % 3000 == 0:
        break

    # archive_df.to_excel(Path("../bt_data_runs/output.xlsx"), index=False)
