import json
import os
from copy import deepcopy
from pathlib import Path
from pprint import pprint as pp
from time import sleep

# Import the backtrader platform
import backtrader as bt
import numpy as np
import pandas as pd
from tqdm.notebook import tqdm_notebook as progress_bar


def all_currencies() -> list:
    """
    Get list of all already downloaded and pickled currencies
    """
    currencies = [
        x.stem for x in Path("..\\downloaded_data\\nomics\\candles").glob("*.pkl")
    ]
    return currencies


def load_currency_data(currency: str) -> pd.DataFrame:
    """
    Load currency candle data from pickled file. Return dataframe.
    """
    currency_data = pd.read_pickle(
        f"..\\downloaded_data\\nomics\\candles\\{currency}.pkl"
    )
    return currency_data


def _clean_currency_data() -> None:
    """
    Use this to get rid of currencies that would have issues with the backtrader.
    It will delete the .pkl files from currencies with issues. This is permanent.
    """
    currencies = all_currencies()
    validation_list = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "transparent_open",
        "transparent_high",
        "transparent_low",
        "transparent_close",
        "transparent_volume",
    ]
    error_list = []
    for currency in currencies:
        df = load_currency_data(currency)
        column_headers = df.columns.tolist()
        try:
            for i, val in enumerate(validation_list):
                if val != column_headers[i]:
                    print(val, column_headers[i], currency)
                    print("Error in column headers")
                    break
        except:
            error_list.append(currency)
            if len(column_headers) == 0:
                os.remove(f"..\\downloaded_data\\nomics\\candles\\{currency}.pkl")
            else:
                raise Exception("Error in column headers")
    print(len(error_list))
    print(error_list)
    return None


class PandasData(bt.feed.DataBase):
    """
    The ``dataname`` parameter inherited from ``bt.feed.DataBase`` is the pandas
    DataFrame
    """

    # ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'transparent_open', 'transparent_high', 'transparent_low', 'transparent_close', 'transparent_volume']
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
        ("transparent_open", "transparent_open"),
        ("transparent_high", "transparent_high"),
        ("transparent_low", "transparent_low"),
        ("transparent_close", "transparent_close"),
        ("transparent_volume", "transparent_volume"),
    )
