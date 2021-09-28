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
import json
import time
from collections import defaultdict
import os

"""
The california File Comes in 3 files

Big todo:
Join district info in
Ask DAs what should be included
Follow up with password protected files, unzip rezip


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

    @staticmethod
    def coerce_cali_strings(
        df,
        config,
        cat_columns,
        exclude=[""],
        extra_cols=None,
        col_list="columns",
    ):
        """
        takes all columns with text or varchar labels in the config,
        strips out whitespace and converts text to all lowercase, california
        specific method keeps recasts to pyarrow or categorical dtype after complete
        to save memory. Utilizing the .str accessors temporarily increases memory footprint for each column/series
         but df returned should be the same size as initially read in
         Assumes all dtypes for strings are pyarrow strings
        NOTE: does not convert voter_status or party_identifier,
              since those are typically defined as capitalized
        :param df: dataframe to modify
        :param config: config object
        :param cat_columns: categorical columns to return
        :param extra_cols: extra columns to add
        :param exclude: columns to exclude
        :param col_list: name of field in yaml to pull column types from
        :return: modified dataframe
        """
        text_fields = [
            c
            for c, v in config[col_list].items()
            if v == "text" or "char" in v
        ]
        if extra_cols is not None:
            text_fields = text_fields + extra_cols
        for field in text_fields:
            if (
                (field in df)
                and (field != config["voter_status"])
                and (field != config["party_identifier"])
                and (field not in exclude)
            ):
                logging.info("internal pyarrow for {}".format(field))
                string_copy = df[field]
                string_copy = string_copy.str.strip()
                string_copy = string_copy.str.split().str.join(" ")
                string_copy = string_copy.str.lower()
                string_copy = string_copy.str.encode("utf-8", errors="ignore")
                df[field] = string_copy.str.decode("utf-8")
                if field not in cat_columns:
                    df[field] = df[field].astype("string[pyarrow]")
                else:
                    logging.info("categorical")
                    df[field] = df[field].astype("category")
        return df

    def execute(self):
        # Todo: remove
        def memprof(n):
            x = psutil.Process().memory_full_info()
            logging.info(
                "{} Resident Set Size: {}, Unique Set Size: {}, Virtual: {}".format(
                    n,
                    bytes2human(x.rss),
                    bytes2human(x.uss),
                    bytes2human(x.vms),
                )
            )

        def district_fun(df_dist, df_voter, dist_dict):
            for dist_code in dist_dict.keys():
                temp_df = df_dist[df_dist["DistrictTypeCode"] == dist_code]
                temp_df = temp_df.rename(
                    columns={"DistrictName": dist_dict[dist_code]}
                )
                df_voter = pd.merge(
                    df_voter,
                    temp_df[["PrecinctId", dist_dict[dist_code]]],
                    how="left",
                    on="PrecinctId",
                )
            df_voter.drop(columns=["PrecinctId"], inplace=True)
            return df_voter

        prof_num = 0
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        config = Config(file_name=self.config_file)
        new_files = self.unpack_files(file_obj=self.main_file)
        logging.info("after all files unpacked")
        memprof(prof_num)
        del self.main_file, self.temp_files
        gc.collect()

        prof_num += 1
        logging.info("after gc unpacked files")
        memprof(prof_num)
        # Have to use longer whole string not just suffix because hist will match to voter file
        voter_file = [f for f in new_files if "pvrdr-vrd" in f["name"]][0]
        district_file = [f for f in new_files if "pvrdr-pd" in f["name"]][0]
        history_file = [f for f in new_files if "pvrdr-vph" in f["name"]][0]

        # chunksize
        chunk_size = 3000000
        # chunk_size = 36 * 1024 * 1024  # for dask in mb
        # Todo: Remove, diagnostic only accurate for only one file, closeish on others though
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
        logging.info("before temp id dataframe")
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

        logging.info("after id dataframe read, before deletion")
        memprof(prof_num)
        del temp_voter_id_df
        gc.collect()

        hist_dict = {i: np.nan for i in voter_ids}
        votetype_dict = {i: np.nan for i in voter_ids}
        del voter_ids
        gc.collect()

        # key election, values date and count, then sort.
        # gonna have to iterate over all_hist and map to sparse
        elect_dict = defaultdict(int)

        prof_num += 1
        logging.info("created all the id dicts")
        memprof(prof_num)

        def dict_cols(
            chunk, history_dict=None, votetype_dict=None, election_dict=None
        ):
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
                ],
                inplace=True,
            )
            # todo: turn election dict into what will be sorted codes
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

        logging.info("completed chunk loop, before hist deletion")
        prof_num += 1
        memprof(prof_num)

        # Todo: check this
        history_file["obj"].close()
        history_size = history_file["obj"].__sizeof__()
        logging.info(
            "history size after buffer flush: {} ".format(
                round(history_size), 2
            )
        )

        logging.info("after hist in memory close")
        prof_num += 1
        memprof(prof_num)

        del history_file

        logging.info("after calling del on hist")
        prof_num += 1
        memprof(prof_num)

        gc.collect()

        logging.info("after gc collect")
        prof_num += 1
        memprof(prof_num)

        logging.info(
            "the size of the hist dictionary is {} megabytes".format(
                round((sys.getsizeof(hist_dict) // 1024 ** 2), 2)
            )
        )
        # index will be voterids
        # There is a bug in from_dict when the values are a list
        # see: https://github.com/pandas-dev/pandas/issues/29213
        # hist_df = pd.DataFrame.from_dict(hist_dict, orient="index")
        hist_series = pd.Series(hist_dict, name="all_history")
        del hist_dict

        logging.info("deleted hist dict")
        prof_num += 1
        memprof(prof_num)
        gc.collect()

        votetype_series = pd.Series(votetype_dict, name="votetype_history")
        logging.info(
            "votetype series memory usage: {}".format(
                round((votetype_series.memory_usage(deep=True) / 1024 ** 3), 2)
            )
        )

        logging.info("both series created")
        prof_num += 1
        memprof(prof_num)

        del votetype_dict
        gc.collect()

        logging.info("reading in voter df")

        # todo: move to yaml?
        category_list = [
            "CountyCode",
            "Suffix",
            "StreetDirPrefix",
            "AddressNumberSuffix",
            "StreetType",
            "StreetDirSuffix",
            "UnitType",
            "City",
            "State",
            "Zip",
            "Language",
            "Gender",
            "PartyCode",
            "Status",
            "VoterStatusReasonCodeDesc",
            "AssistanceRequestFlag",
            "VbmVoterType",
            "USCongressionalDistrict",
            "StateSenate",
            "Municipality",
        ]
        # read in columns to set dtype as pyarrow
        col_ifornia = pd.read_csv(
            voter_file["obj"], sep="\t", nrows=0, encoding="latin-1"
        ).columns.tolist()
        voter_file["obj"].seek(0)
        dtype_dict = {
            col: (
                "string[pyarrow]" if col not in category_list else "category"
            )
            for col in col_ifornia
        }
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

        voter_file["obj"].close()
        del voter_file
        gc.collect()

        logging.info("cleared voter file object")
        prof_num += 1
        memprof(prof_num)

        # Do district stuff here
        # This will add these columns, so change the yamls to reflect that
        # NP nans for munisipality seem to be unincorperated areas?

        # Todo: Maybe move to yaml?
        district_dict = {
            "CG": "USCongressionalDistrict",
            "SS": "StateSenate",
            "SA": "StateAssembly",
            "CI": "Municipality",
            "SU": "CountySupervisoral",
        }
        district_df = pd.read_csv(district_file["obj"], sep="\t", dtype="string[pyarrow]")

        logging.info("Read in District DF")
        prof_num += 1
        memprof(prof_num)

        district_file["obj"].close()
        del district_file

        logging.info("cleared district file object")
        prof_num += 1
        memprof(prof_num)

        merged_districts = district_fun(
            district_df,
            voter_df[["RegistrantID", "PrecinctId"]],
            district_dict,
        )

        logging.info("created merged district df")
        prof_num += 1
        memprof(prof_num)

        voter_df = voter_df.merge(
            merged_districts, left_on="RegistrantID", right_on="RegistrantID"
        )

        logging.info("merged into voter_df")
        prof_num += 1
        memprof(prof_num)

        del merged_districts
        gc.collect()

        logging.info("deleted merged district df")
        prof_num += 1
        memprof(prof_num)

        voter_df.set_index("RegistrantID", inplace=True)

        voter_df = voter_df.merge(
            hist_series, left_index=True, right_index=True
        )

        logging.info("merged hist on id")
        prof_num += 1
        memprof(prof_num)

        del hist_series
        gc.collect()

        logging.info("deleted hist series")
        prof_num += 1
        memprof(prof_num)

        voter_df = voter_df.merge(
            votetype_series, left_index=True, right_index=True
        )

        del votetype_series
        gc.collect()

        logging.info("deleted votetype_series")
        prof_num += 1
        memprof(prof_num)

        # create sparse history
        sorted_keys = sorted(
            elect_dict.items(), key=lambda x: x[0].split("_")[1]
        )
        sorted_codes_dict = {
            value[0]: {"index": i, "count": value[1]}
            for i, value in enumerate(sorted_keys)
        }
        sorted_codes = [x[0] for x in sorted_keys]
        voter_df["sparse_history"] = voter_df.all_history.apply(
            lambda x: [sorted_codes_dict[y]["index"] for y in x]
            if x == x
            else np.nan
        )
        # Begin Coerce

        # Todo: create custom coerce function because it removes pyarrow and also
        # categories to turn them in to strings
        logging.info("coesrcing strings")
        voter_df = self.coerce_cali_strings(voter_df, config, category_list)
        logging.info("coerced string")
        prof_num += 1
        memprof(prof_num)

        voter_df = self.config.coerce_dates(voter_df)
        logging.info("coerced dates")
        prof_num += 1
        memprof(prof_num)

        voter_df = self.config.coerce_numeric(voter_df)
        logging.info("coerced numeric")
        prof_num += 1
        memprof(prof_num)

        voter_df = voter_df.reset_index().rename(
            columns={"index": "RegistrantID"}
        )
        voter_csv = voter_df.to_csv(encoding="utf-8", index=False)

        logging.info("wrote csv")
        prof_num += 1
        memprof(prof_num)

        del voter_df
        gc.collect()

        logging.info("cleaned dataframes")
        prof_num += 1
        memprof(prof_num)
        self.meta = {
            "message": "california_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(voter_csv),
            s3_bucket=self.s3_bucket,
        )
