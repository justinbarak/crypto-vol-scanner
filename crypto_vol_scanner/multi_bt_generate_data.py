from dotenv import load_dotenv
import os
from pprint import pprint as pp
import json
import pandas as pd
from pandas.api.types import CategoricalDtype
from copy import deepcopy
from tqdm import tqdm as progress_bar
from pathlib import Path
import backtrader as bt
from multi_nomics_bt_strategy import VolumeStrategy
from nomics_data_prep_for_backtrader import (
    load_currency_data,
    all_currencies,
    _clean_currency_data,
)
from backtrader.feeds import PandasData
import dill as pickle
import numpy as np
import warnings
import logging
from time import sleep
from alive_progress import alive_bar
import pprint as pp

# multiprocessing required imports
from multiprocessing import Process, Queue, Event
import multiprocessing
import queue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(process)d-%(levelname)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
            "buy_avg_volume": "float32",
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


# function run by worker processes
def worker(
    input: Queue,
    output: Queue,
    proc_id: int,
    stop_signal: Event,
    results_folder: Path,
    progress_update_queue: Queue,
):
    """
    This function will run down the input queue and save results to the output queue."""
    try:
        for kwargs in iter(input.get, "STOP"):
            result = simulate_backtrader(
                process_number=proc_id, results_folder=results_folder, **kwargs
            )
            progress_update_queue.put(1)
            if result is None:
                continue
            output.put(result)
            if stop_signal.is_set():
                break
    except KeyboardInterrupt:
        return


# function run by the status update process
def status_update(
    task_queue: Queue,
    completed_queue: Queue,
    progress_update_queue: Queue,
    stop_signal: Event,
    processing_finished: Event,
    n_workers: int,
) -> None:
    """
    This function will update the screen with the status of the worker processes."""
    try:
        total_task_count = task_queue.qsize() - n_workers
        # prepare to show progress bar
        with alive_bar(total=total_task_count, title="Task Status", theme="ascii") as bar:
            # exit if all tasks are completed
            while not processing_finished.is_set() or completed_queue.qsize() > 0:
                try:
                    progress_update_queue.get(timeout=1)
                    bar()
                except queue.Empty:
                    pass
                # exit if stop signal is set
                if stop_signal.is_set():
                    print(f"Currently at {completed_queue.qsize()} remaining tasks")
                    break

    except KeyboardInterrupt:
        print(f"Finishing up {completed_queue.qsize()} tasks")
        return None


def manage_queue(
    completed_queue: Queue,
    stop_signal: Event,
    completed_currencies: list,
    results_folder: Path,
    combined_archive_save_path: Path,
    processing_finished: Event,
) -> None:
    """
    This function will manage the queue and save the completed dataframes to the archive."""
    try:
        df_list = []
        new_currencies = set()
        # exit if all tasks are completed
        while (not processing_finished.is_set()) or (completed_queue.qsize() > 0):
            try:
                temp_dataframe, currency = completed_queue.get(timeout=1)
                temp_dataframe = type_raw_dataframe(temp_dataframe)
                df_list.append(deepcopy(temp_dataframe))
                new_currencies.add(currency)

                # partial archiving just slows program down, didn't save full amount of data
                # if len(df_list) > 1000:
                #     df = pd.concat(df_list, ignore_index=True)
                #     df.to_pickle(path=combined_archive_save_path)
                #     df_list = []

            except queue.Empty:
                pass
                # exit if stop signal is set
            if stop_signal.is_set():
                logger.info(
                    f"Stop_signal triggered with {completed_queue.qsize()} tasks"
                    " remaining"
                )
                break
    except KeyboardInterrupt:
        pass
    finally:
        # short pause to verify that all tasks are completed
        sleep(1)
        logger.info(
            f"Stopping the queue manager with {completed_queue.qsize()} tasks remaining"
        )
        # save and pickle the result dataframes
        df = pd.concat(df_list, ignore_index=True)
        df.to_pickle(path=combined_archive_save_path)
        # save and pickle the completed currencies
        completed_currencies.extend(new_currencies)
        with open(results_folder.joinpath(f"completed_currencies.pkl"), "wb") as f:
            pickle.dump(completed_currencies, f)
        logger.info("All data successfully pickled.")


def total_redo(
    status: bool, results_folder: Path = "", combined_archive_save_path: Path = ""
) -> tuple:
    """
    This function will return empty dataframes if total_redo is True.\n
    If False it will return the dataframes/currencies from the archive."""
    if status:
        logger.info("Total redo")
        if os.path.exists(combined_archive_save_path):
            os.remove(combined_archive_save_path)
        return pd.DataFrame(), []
    else:
        logger.info("Reusing completed runs")
        if os.path.exists(combined_archive_save_path):
            archive_df = pd.read_pickle(combined_archive_save_path)
            with open(results_folder.joinpath(f"completed_currencies.pkl"), "rb") as f:
                completed_currencies = pickle.load(f)
                return archive_df, completed_currencies
        else:
            return total_redo(True)


def main() -> None:

    # comment out below if running repeat analysis
    _clean_currency_data()

    n_workers = max(multiprocessing.cpu_count() * 6 // 7 - 1, 1)
    results_folder = Path("../bt_data_runs/").resolve()
    combined_archive_save_path = results_folder.joinpath(f"aggregated_archive_df.pkl")
    logger.info(f"Using {n_workers} workers for the main process")
    # Option to save existing dataframe and skip already completed currencies
    total_redo_choice = True
    archive_df, completed_currencies = total_redo(
        total_redo_choice, results_folder, combined_archive_save_path
    )

    # create queues
    task_queue = Queue()
    completed_queue = Queue()
    progress_update_queue = Queue()

    # create stop event flag
    stop_signal = Event()
    processing_finished = Event()

    currencies = all_currencies()
    # remove already completed currencies
    currencies = [c for c in currencies if c not in completed_currencies]

    try:
        # submit tasks to queue
        for currency in progress_bar(
            currencies, desc="Creating task queue", position=0, leave=False
        ):
            for VolAvgParam in range(5, 95, 5):
                for TriggerVolParam in range(6, 20, 2):
                    for transparent in [True, False]:
                        task_queue.put(
                            (
                                {
                                    "currency": currency,
                                    "VolAvgParam": VolAvgParam,
                                    "TriggerVolParam": TriggerVolParam,
                                    "transparent": transparent,
                                }
                            )
                        )
        for _ in range(n_workers):
            task_queue.put("STOP")

        # launch process to report progress
        progress_queue_process = Process(
            target=status_update,
            args=(
                task_queue,
                completed_queue,
                progress_update_queue,
                stop_signal,
                processing_finished,
                n_workers,
            ),
        )
        progress_queue_process.start()

        # launch process to consume completed results from queue
        completed_queue_process = Process(
            target=manage_queue,
            args=(
                completed_queue,
                stop_signal,
                completed_currencies,
                results_folder,
                combined_archive_save_path,
                processing_finished,
            ),
        )
        completed_queue_process.start()

        # launch worker processes
        workers = []
        for processor_id in range(n_workers):
            workers.append(
                Process(
                    target=worker,
                    args=(
                        task_queue,
                        completed_queue,
                        processor_id,
                        stop_signal,
                        results_folder,
                        progress_update_queue,
                    ),
                )
            )
            workers[processor_id].start()

        for process in workers:
            process.join()

        # stop workers
        processing_finished.set()
        progress_queue_process.join()
        completed_queue_process.join()
        stop_signal.set()
        logger.info("Completed all tasks")
    except KeyboardInterrupt:
        stop_signal.set()
        logger.info("KeyboardInterrupt")


def simulate_backtrader(
    currency: str,
    VolAvgParam: int,
    process_number: int,
    TriggerVolParam: int,
    transparent: bool,
    results_folder: Path,
) -> pd.DataFrame:
    temp_save_path = results_folder.joinpath(f"_temp_df_{process_number}.pkl")

    data_df = load_currency_data(currency)
    data_df.set_index("timestamp", inplace=True)
    data_normal = PandasData_normal(dataname=data_df)
    data_transparent = PandasData_transparent(dataname=data_df)

    candle_length = len(data_df)

    # avoid errors with averaging
    if VolAvgParam + 2 >= candle_length:
        return None

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
        "save_path": temp_save_path,
    }
    cerebro.addstrategy(VolumeStrategy, **strategy_params)
    # no commission
    cerebro.broker.setcash(1000000)
    cerebro.run()
    temp_df = pd.read_pickle(temp_save_path)

    if len(temp_df) == 0:
        return None
    return (deepcopy(type_raw_dataframe(temp_df)), currency)


if __name__ == "__main__":
    main()
