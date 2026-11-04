import datetime
import gc
import json

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)


def drop_unnamed_columns(df):
    """Drop the empty trailing header column, which pandas reads as "Unnamed: N"."""
    return df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed:")]


class PreprocessMississippi(Preprocessor):
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

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )

        if not self.ignore_checks:
            self.file_check(len(new_files))

        # Names vary, e.g. "...Weekly Distribution_<user>_<date>_<n>VotingHistory.csv"
        # and "<date> Statewide Voting History Weekly Distribution.csv", so
        # spaces are removed before matching
        hist_file = [
            n
            for n in new_files
            if "votinghistory" in n["name"].lower().replace(" ", "")
        ][0]
        voter_file = [
            n
            for n in new_files
            if "weeklydistribution" in n["name"].lower().replace(" ", "")
            and n is not hist_file
        ][0]
        del new_files

        voter_df = self.read_csv_count_error_lines(
            voter_file["obj"], on_bad_lines="warn", dtype=str
        )
        voter_df = drop_unnamed_columns(voter_df)
        hist_df = self.read_csv_count_error_lines(
            hist_file["obj"], on_bad_lines="warn", dtype=str
        )
        hist_df = drop_unnamed_columns(hist_df)
        del voter_file, hist_file
        gc.collect()


        # No birth date or party in the voter file, add dummy columns
        voter_df[self.config["birthday_identifier"]] = np.nan
        voter_df[self.config["party_identifier"]] = np.nan

        self.column_check(list(voter_df.columns))

        hist_df = hist_df.dropna(subset=["ELECTION NAME", "ELECTION DATE"])
        hist_df["ELECTION DATE"] = pd.to_datetime(
            hist_df["ELECTION DATE"],
            format=self.config["date_format"],
            errors="coerce",
        )
        hist_df = hist_df.dropna(subset=["ELECTION DATE"])
        hist_df = hist_df.sort_values("ELECTION DATE")

        hist_df["combined_name"] = (
            hist_df["ELECTION NAME"]
            .str.strip()
            .str.split()
            .str.join("_")
            .str.lower()
            + "_"
            + hist_df["ELECTION DATE"].dt.strftime("%m/%d/%Y")
        )
        hist_df["POLITICAL PARTY"] = hist_df["POLITICAL PARTY"].str.strip()

        sorted_codes = hist_df["combined_name"].unique().tolist()
        sorted_codes.sort(
            key=lambda x: datetime.strptime(x.split("_")[-1], "%m/%d/%Y")
        )
        counts = hist_df["combined_name"].value_counts()

        sorted_codes_dict = {
            k: {
                "index": i,
                "count": int(counts.loc[k]),
                "date": k.split("_")[-1],
            }
            for i, k in enumerate(sorted_codes)
        }

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]["index"] for k in arr]
            else:
                return np.nan

        voter_df = voter_df.set_index(self.config["voter_id"], drop=False)
        voter_groups = hist_df.groupby(self.config["voter_id"])
        voter_df["all_history"] = voter_groups["combined_name"].apply(list)
        voter_df["sparse_history"] = voter_df["all_history"].map(
            insert_code_bin
        )
        voter_df["party_history"] = voter_groups["POLITICAL PARTY"].apply(list)
        del hist_df, voter_groups
        gc.collect()

        expected_cols = (
            self.config["ordered_columns"]
            + self.config["ordered_generated_columns"]
        )
        voter_df = self.reconcile_columns(voter_df, expected_cols)
        voter_df = voter_df[expected_cols]

        voter_df = self.config.coerce_strings(voter_df)
        voter_df = self.config.coerce_dates(voter_df)

        # Check the file for all the proper locales
        self.locale_check(
            set(voter_df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "mississippi_{}".format(datetime.now().isoformat()),
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
