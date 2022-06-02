import datetime
import gc
import json
import logging

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import (
    ensure_int_string,
)


class PreprocessNewJersey2(Preprocessor):
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

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        def format_birthdays_differently_per_county(df):
            field = self.config["birthday_identifier"]
            df[field] = df[field].apply(str)
            for format_str in self.config["date_format"]:
                formatted = pd.to_datetime(
                    df[field], format=format_str, errors="coerce"
                )
                if len(formatted[~formatted.isna()]) > (0.5 * len(formatted)):
                    df[field] = formatted
                    break
            return df

        def combine_dfs(filelist):
            df = pd.DataFrame()
            for f in filelist:
                logging.info("Reading file: {}".format(f["name"]))
                new_df = self.read_csv_count_error_lines(
                    f["obj"], error_bad_lines=False
                )
                if "vlist" in f["name"]:
                    new_df = format_birthdays_differently_per_county(new_df)
                df = pd.concat([df, new_df], axis=0)
            return df

        def simplify_status(status):
            basic_status = ["Active", "Inactive", "Pending"]
            if type(status) is str:
                for s in basic_status:
                    if s in status:
                        return s
            return np.nan

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]["index"] for k in arr]
            else:
                return np.nan

        def handle_date(d):
            possible_date = date_from_str(d)
            if possible_date is None:
                return ""
            return pd.to_datetime(possible_date).strftime("%m/%d/%Y")

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="infer"
        )
        voter_files = [n for n in new_files if "vlist" in n["name"].lower()]
        hist_files = [n for n in new_files if "ehist" in n["name"].lower()]
        del self.main_file, self.temp_files
        gc.collect()

        if not self.ignore_checks:
            self.file_check(len(voter_files), len(hist_files))
        voter_df = combine_dfs(voter_files)
        hist_df = combine_dfs(hist_files)
        del voter_files, hist_files, new_files
        gc.collect()

        voter_df = self.config.coerce_strings(voter_df)

        if "displayId" in voter_df.columns:
            voter_df.rename(
                columns={"displayId": self.config["voter_id"]}, inplace=True
            )
        voter_df[self.config["voter_id"]] = voter_df[
            self.config["voter_id"]
        ].str.upper()
        voter_df[self.config["party_identifier"]] = voter_df[
            self.config["party_identifier"]
        ].str.replace(".", "")
        voter_df = self.config.coerce_numeric(
            voter_df,
            extra_cols=[
                "apt_unit",
                "ward",
                "district",
                "congressional",
                "legislative",
                "freeholder",
                "school",
                "fire",
            ],
        )

        # ensure district fields are e.g. "1" not "1.0"
        voter_df["congressional"] = (
            voter_df["congressional"].map(ensure_int_string)
        )
        voter_df["legislative"] = (
            voter_df["legislative"].map(ensure_int_string)
        )
        voter_df["district"] = (
            voter_df["district"].map(ensure_int_string)
        )

        # multiple active / inactive statuses are incompatible with our data
        # model; simplify them while also keeping the original data
        voter_df["unabridged_status"] = voter_df[self.config["voter_status"]]
        voter_df[self.config["voter_status"]] = voter_df[
            self.config["voter_status"]
        ].map(simplify_status)

        # handle history:
        hist_df["election_name"] = (
            hist_df["election_date"] + "_" + hist_df["election_name"]
        )

        hist_df.dropna(subset=["election_name"], inplace=True)
        sorted_codes = sorted(hist_df["election_name"].unique().tolist())
        counts = hist_df["election_name"].value_counts()
        sorted_codes_dict = {
            k: {
                "index": int(i),
                "count": int(counts[k]),
                "date": handle_date(k),
            }
            for i, k in enumerate(sorted_codes)
        }

        hist_df.sort_values("election_name", inplace=True)
        hist_df.rename(
            columns={"voter_id": self.config["voter_id"]}, inplace=True
        )

        voter_df.set_index(self.config["voter_id"], drop=False, inplace=True)
        voter_groups = hist_df.groupby(self.config["voter_id"])

        # get extra data from history file that is missing from voter file
        voter_df["gender"] = voter_groups["voter_sex"].apply(
            lambda x: list(x)[-1]
        )

        # at some point in in late 2020-early 2021 NJ started adding a reg_date
        # column and deprecating the registration_date information in the
        # voter_history file
        if "reg_date" in voter_df.columns:
            voter_df.rename(
                columns={"reg_date": "registration_date"}, inplace=True
            )
            # remove the UTC, does not fail if not utc
            voter_df["registration_date"] = pd.to_datetime(
                voter_df.registration_date, errors="coerce"
            ).dt.tz_localize(None)
        else:
            voter_df["registration_date"] = voter_groups[
                "voter_registrationDate"
            ].apply(lambda x: list(x)[-1])

        self.column_check(list(voter_df.columns))

        voter_df = self.config.coerce_dates(voter_df)

        voter_df["all_history"] = voter_groups["election_name"].apply(list)
        voter_df["sparse_history"] = voter_df["all_history"].map(
            insert_code_bin
        )
        voter_df["party_history"] = voter_groups["voter_party"].apply(list)
        voter_df["votetype_history"] = voter_groups["ballot_type"].apply(list)
        del hist_df, voter_groups
        gc.collect()

        expected_cols = (
            self.config["ordered_columns"]
            + self.config["ordered_generated_columns"]
        )
        voter_df = self.reconcile_columns(voter_df, expected_cols)
        voter_df = voter_df[expected_cols]

        # Check the file for all the proper locales
        self.locale_check(
            set(voter_df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "new_jersey2_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        csv_obj = voter_df.to_csv(encoding="utf-8", index=False)
        del voter_df
        gc.collect()

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_obj),
            s3_bucket=self.s3_bucket,
        )
        del csv_obj
        gc.collect()
