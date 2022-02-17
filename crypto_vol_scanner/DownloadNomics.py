# %%
from dotenv import load_dotenv
import os
import nomics
from pprint import pprint as pp
import json
from time import sleep
import pandas as pd
import requests
import io
from collections import OrderedDict
from copy import deepcopy
from tqdm.notebook import tqdm_notebook as progress_bar
from alive_progress import alive_bar
from pathlib import Path
from pathlib import PurePath


def get_all_currencies(API_KEY: str) -> pd.DataFrame:
    """
    Get a list of all currencies from nomics API. Save some currency data
    (id, status, etc.) as a pickled dataframe.
    """
    nom = nomics.Nomics(API_KEY)
    page_requested = 1
    desired_data = [
        "id",
        "name",
        "status",
        "num_exchanges",
        "num_pairs",
        "first_candle",
        "high",
        "high_timestamp",
        "platform_currency",
    ]
    formats = {
        "num_exchanges": "int16",
        "num_pairs": "int16",
        "first_candle": "datetime64[ns]",
        "high": "float64",
        "high_timestamp": "datetime64[ns]",
    }
    df = pd.DataFrame(columns=desired_data)
    df = df.astype(formats)
    estimated_pages = 325  # actual as of 12/31/2021
    with alive_bar(
        total=estimated_pages, title="Pages Downloaded...", theme="ascii"
    ) as bar:
        while True:
            print(f"Page {page_requested}")
            response = nom.Currencies.get_currencies(
                ids="", per_page=100, page=page_requested
            )
            if len(response) == 0:
                break
            page_requested += 1
            for item in response:
                new_row = {}
                for key in desired_data:
                    new_row[key] = item.get(key, None)
                df = df.append(deepcopy(new_row), ignore_index=True)
            bar()
    # df.to_excel("..\\downloaded_data\\nomics\\crypto_data.xlsx")
    df.to_pickle("..\\downloaded_data\\nomics\\crypto_data.pkl")
    print("Done")
    return None


def download_daily_candles(API_KEY: str, update_all: bool = False):
    nom = nomics.Nomics(API_KEY)
    # get the list of currencies
    currency_data_df = pd.read_pickle("..\\downloaded_data\\nomics\\crypto_data.pkl")

    currency_list = currency_data_df["id"].to_list()

    response = {}
    problem_currencies = []

    # get list of already downloaded and pickled currencies
    already_downloaded_currencies = [
        x.stem for x in Path("..\\downloaded_data\\nomics\\candles").glob("*.pkl")
    ]
    if update_all:
        for pkl in already_downloaded_currencies:
            os.remove(f"..\\downloaded_data\\nomics\\candles\\{pkl}.pkl")
        already_downloaded_currencies = []

    # get daily candles for each currency
    for currency in progress_bar(currency_list, desc="Downloading candles"):
        try:
            if (
                not PurePath(currency).is_reserved()
                and currency not in already_downloaded_currencies
            ):
                response = nom.Candles.get_candles(interval="1d", currency=currency)
                df = pd.json_normalize(response)
                df.fillna(0, inplace=True)
                # add types
                headers = df.columns.to_list()
                for column in headers:
                    if column == "timestamp":
                        df[column] = pd.to_datetime(
                            df[column], infer_datetime_format=True
                        )
                    else:
                        df[column] = df[column].astype("float64")
                # pickle the data
                save_path = Path(
                    "..\\downloaded_data\\nomics\\candles\\" + currency + ".pkl"
                ).resolve()
                df.to_pickle(save_path)
            else:
                if currency not in already_downloaded_currencies:
                    problem_currencies.append(currency)
        except Exception as e:
            problem_currencies.append(currency)
            print(f"{currency} failed: {e}")
    print(f"Problem currencies:\n")
    pp(problem_currencies)

    # problem currencies as of 12/31/2021
    # ['AUX', 'CON', 'COM2', 'O/S', 'COM3', 'COM4', 'COM5', 'PRN']


def main():
    # initialize the environment
    load_dotenv()
    API_KEY = os.getenv("API_KEY")

    # uncomment to pull all currencies
    # get_all_currencies(API_KEY)

    # uncomment to pull daily candle data
    download_daily_candles(API_KEY, update_all=False)


if __name__ == "__main__":
    main()

# %%


# %%
# %%
