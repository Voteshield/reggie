import gc
import sys

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import format_column_name
from reggie.configs.configs import Config
import logging
import pandas as pd
import numpy as np
import datetime
from io import StringIO
from datetime import datetime
from dateutil import parser
from collections import defaultdict
import json
import time
from collections import defaultdict
import os

"""
The california File Comes in 3 files

Big todo:
Create hist stuff first, then del from memory to free room to
Join district info in


Use ensure int string where necessary otherwise you have fun float string problems
"""
import psutil
from psutil._common import bytes2human


class PreprocessCalifornia(Preprocessor):
    def __init__(self, raw_s3_file, config_file, force_date=None, **kwargs):

        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        super().__init__(
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            **kwargs
        )
        self.raw_s3_file = raw_s3_file
        self.processed_file = None
        self.config_file = config_file

    def execute(self):
        def memprof(n):
            x = psutil.Process().memory_full_info()
            logging.info(
                "{} Resident Set Size: {}, Unique Set Size: {}, Virtual: {}".format(
                    n,
                    bytes2human(x.rss),
                    bytes2human(x.uss),
                    bytes2human(x.vms)
                )
            )
        prof_num = 0
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        config = Config(file_name=self.config_file)
        new_files = self.unpack_files(file_obj=self.main_file)
        logging.info('after read in')
        memprof(prof_num)
        del self.main_file, self.temp_files
        gc.collect()

        prof_num += 1
        logging.info('after gc in')
        memprof(prof_num)
        # Have to use longer whole string not just suffix because hist will match to voter file
        voter_file = [f for f in new_files if "pvrdr-vrd" in f["name"]][0]
        district_file = [f for f in new_files if "pvrdr-pd" in f["name"]][0]
        history_file = [f for f in new_files if "pvrdr-vph" in f["name"]][0]

        # chunksize
        chunk_size = 3000000
        # chunk_size = 36 * 1024 * 1024  # for dask in mb
        # Todo: Remove, diagnostic only accurate for only one file
        rows = 213637309
        num_chunks = rows // chunk_size + 2

        # todo: remove
        # Diagnostic
        voter_size = voter_file["obj"].__sizeof__()
        history_size = history_file["obj"].__sizeof__()
        district_size = district_file["obj"].__sizeof__()

        logging.info(
            "Reading In files: voter_size {}\n history_size {} \n district_size{} \n total: {}".format(
                voter_size // 1023 ** 3,
                history_size // 1023 ** 3,
                district_size // 1023 ** 3,
                (voter_size + history_size + district_size) // 1023 ** 3,
            )
        )

        prof_num += 1
        logging.info('before temp id')
        memprof(prof_num)

        temp_voter_id_df = pd.read_csv(
            voter_file["obj"],
            sep="\t",
            encoding="latin-1",
            usecols=["RegistrantID"],
            dtype=str,
        )
        # rewind
        voter_file["obj"].seek(0)

        voter_ids = temp_voter_id_df["RegistrantID"].unique().tolist()
        prof_num += 1
        logging.info('temp_ voter id')
        memprof(prof_num)
        del temp_voter_id_df
        gc.collect()

        hist_dict = {i: np.nan for i in voter_ids}
        votetype_dict = {i: np.nan for i in voter_ids}
        del voter_ids
        gc.collect()

        elect_dict = defaultdict(int)

        prof_num += 1
        logging.info('all the id dicts')
        memprof(prof_num)

        def dict_cols(chunk, history_dict=None, votetype_dict=None, election_dict=None):
            chunk["combined_col"] = (
                chunk["ElectionType"].replace(" ", "")
                + "_"
                + chunk["ElectionDate"]
                # + "_"
                # + chunk["Method"]
            )
            chunk["election"] = (
                chunk["ElectionType"].replace(" ", "")
                + "_"
                + chunk["ElectionDate"]
            )
            chunk.drop(
                columns=[
                    "ElectionType",
                    "ElectionName",
                    "ElectionDate",
                    "CountyCode",
                    # "Method",
                ],
                inplace=True,
            )
            for row in chunk.itertuples():
                try:
                    current_li = hist_dict[row.RegistrantID]
                    votetype_hist = votetype_dict[row.RegistrantID]
                    # throws key error for entries not in voter file
                    election_dict[row.election] += 1
                    combined_row = row.combined_col
                    if isinstance(current_li, list):
                        current_li.append(combined_row)
                        votetype_hist.append(row.Method)
                        history_dict[row.RegistrantID] = current_li
                        votetype_dict[row.RegistrantID] = votetype_hist
                    else:
                        # test_dict[row['RegistrantID']][0]
                        history_dict[row.RegistrantID] = [
                            combined_row
                        ]  # Create list of elections even if len 1
                        votetype_dict[row.RegistrantID] = [row.Method]
                except KeyError:
                    continue

            # return history_dict, election_dict

        history_chunks = pd.read_csv(
            history_file["obj"],
            sep="\t",
            usecols=[
                "RegistrantID",
                "CountyCode",
                "ElectionDate",
                "ElectionName",
                "ElectionType",
                "Method",
            ],
            dtype=str,
            chunksize=chunk_size,
        )
        progress_tracker = 0
        for chunk in history_chunks:
            progress_tracker += 1

            prof_num += 1
            memprof(prof_num)

            logging.info("Chunk {}/{}".format(progress_tracker, num_chunks))
            start_t = time.time()
            dict_cols(chunk, hist_dict, votetype_dict, elect_dict)
            end_time = time.time()
            logging.info(
                "time_elapsed: {}".format(round((end_time - start_t), 2))
            )
            time_remaining = round(
                ((end_time - start_t) * (num_chunks - progress_tracker) / 60),
                2,
            )
            logging.info(
                "time more or less remaining {}".format(time_remaining)
            )

        logging.info("completed loop, before deletion")
        prof_num += 1
        memprof(prof_num)

        # Todo: check this
        history_size["obj"].close()
        history_size = history_file["obj"].__sizeof__()
        logging.info("history size now: {} ".format(history_size))

        logging.info('after hist close?')
        prof_num += 1
        memprof(prof_num)

        del history_file

        logging.info('after hist del')
        prof_num += 1
        memprof(prof_num)

        gc.collect()

        logging.info('after collect')
        prof_num += 1
        memprof(prof_num)

        logging.info(
            "the size of the hist dictionary is {} megabytes".format(
                sys.getsizeof(hist_dict) // 1024 ** 2
            )
        )
        # index will be voterids
        logging.info("df creation")
        # There is a bug in from_dict when the values are a list
        # see: https://github.com/pandas-dev/pandas/issues/29213
        # hist_df = pd.DataFrame.from_dict(hist_dict, orient="index")
        hist_series = pd.Series(hist_dict, name='all_history')
        del hist_dict

        prof_num += 1
        memprof(prof_num)
        gc.collect()

        logging.info('both series')
        prof_num += 1
        memprof(prof_num)

        votetype_series = pd.Series(votetype_dict, name='votetype_history')
        logging.info(
            "series memory usage: {}".format(
                votetype_series.memory_usage(deep=True) / 1024 ** 3
            )
        )

        logging.info('series read in')
        prof_num += 1
        memprof(prof_num)

        del votetype_dict
        gc.collect()

        # Getting all memory using os.popen()
        # be careful of int indexes?
        # csv_hist = hist_series.to_csv(encoding="utf-8", index=True)
        logging.info("reading in voter df")
        category_list = ['CountyCode', 'Suffix', 'StreetDirPrefix', 'AddressNumberSuffix', 'StreetType',
                         'StreetDirSuffix', 'UnitType', 'City', 'State', 'Zip', 'Language', 'Gender', 'PartyCode',
                         'Status', 'VoterStatusReasonCodeDesc', 'AssistanceRequestFlag', 'VbmVoterType', 'PrecinctId']
        col_ifornia = pd.read_csv(voter_file["obj"],
                                  sep='\t', nrows=0, encoding="latin-1"
                                  ).columns.tolist()
        voter_file["obj"].seek(0)
        dtype_dict = {col: ("string[pyarrow]" if col not in category_list else "category") for col in col_ifornia}
        voter_df = pd.read_csv(
            voter_file["obj"],
            sep="\t",
            dtype=dtype_dict,
            encoding="latin-1",
            on_bad_lines="warn",
        )

        logging.info("read in voter df")
        prof_num += 1
        memprof(prof_num)

        logging.info(
            "dataframe memory usage: {}".format(
                round((voter_df.memory_usage(deep=True).sum() / 1024 ** 2), 2)
            )
        )
        del voter_file
        gc.collect()

        logging.info('voter file deleted')
        prof_num += 1
        memprof(prof_num)

        voter_df.set_index('RegistrantID', inplace=True)
        voter_df = voter_df.merge(hist_series, left_index=True, right_index=True)

        logging.info('merged on id')
        prof_num += 1
        memprof(prof_num)

        del hist_series
        gc.collect()

        logging.info('deleted hist series')
        prof_num += 1
        memprof(prof_num)

        voter_csv = voter_df.to_csv(encoding="utf-8", index=True)
        logging.info('wrote csv')

        prof_num += 1
        memprof(prof_num)

        del voter_df
        del votetype_series
        gc.collect()

        logging.info('cleaned dataframes')
        prof_num += 1
        memprof(prof_num)
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(voter_csv),
            s3_bucket=self.s3_bucket,
        )
