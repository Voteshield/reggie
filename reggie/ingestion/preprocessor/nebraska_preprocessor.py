import logging

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    FileItem,
    Preprocessor,
    date_from_str,
)
import json


class PreprocessNebraska(Preprocessor):
    def __init__(self, raw_s3_file, config_file, force_date=None, **kwargs):

        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        super().__init__(
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            **kwargs,
        )
        self.raw_s3_file = raw_s3_file
        self.processed_file = None

    def ne_hist_date(self, s, history_code_df) -> str:
        """
        Takes the history code dataframe and retrieves the date each election
        code corresponds to. Sometimes, there are election codes in the history
        columns in the voter file that does not correspond to any code given
        in the code file.
        Args:
            s: The string value in the voter file column to look up in the code file
            history_code_df: The history code file
        Returns:
            election_date: string containing the date in MM/DD/YYYY format
        """
        election_date = ""
        try:
            # Make sure date is in expected format
            election_date = history_code_df[
                history_code_df["text_election_code"] == s
            ]["date_election"]
            election_date = election_date.iloc[0]
            if (
                election_date.count("/") == 2
                and election_date.item().count("-") == 0
            ):
                election_date = datetime.strptime(
                    election_date, "%m/%d/%Y %H:%M:%S"
                ).date()

                # Convert back to string, clunky but probably necessary
                election_date = election_date.strftime("%m/%d/%Y")
                return election_date
            elif election_date.count(":") == 1:
                election_date = datetime.strptime(
                    election_date, "%m/%d/%y %H:%M"
                ).date()

                election_date = election_date.strftime("%m/%d/%Y")
                return election_date
            elif election_date.count("-") == 2:
                election_date = election_date.split(" ")[0]
                election_date = datetime.strptime(
                    election_date, "%Y-%m-%d"
                ).date()
                election_date = election_date.strftime("%m/%d/%Y")
                return election_date
        except IndexError:
            # In this case, the election code we are looking up does not exist
            # in the election code file, so instead approximate dates for the
            # general and primary elections, which have predictable formats.
            # The format for these will be like GN96 or PR00 Typically, these
            # are very old elections, like ones in the 80s though 00s.
            # There will be no value returned if the code is both not in the
            # election code file, and it's not some easily understandable code.
            # For instance, we skip codes like ADAMS or FN39Q
            try:
                temp_year = datetime.strptime(s[-2:], "%y").year
                if s[:2] == "GN":
                    election_date = f"11/05/{temp_year}"
                    return election_date
                if s[:2] == "PR":
                    election_date = f"05/15/{temp_year}"
                    return election_date
            except ValueError:
                logging.info(
                    f"election code {s} found in the voter file does not exist in election map file, skipping"
                )
            election_date = ""
        return election_date

    def add_history(self, main_df, hist_code_df) -> tuple:
        """
        Combines the history columns and data into their sorted codes, and
        sorted codes dictionary, containing the counts, codes and dates.

        Args:
            main_df: The main voter file, containing the history columns
            hist_code_df: The history code file
        Returns:
            sorted_codes: the codes used in NE's history columns (ex GN24)
                sorted by count
            sorted_codes_dict: the dictionary containing the decoding for the
                election codes, their counts and the date of the election
        """
        count_df = pd.DataFrame()
        for idx, hist in enumerate(self.config["hist_columns"]):
            unique_codes, counts = np.unique(
                main_df[hist].str.replace(" ", "").dropna().values,
                return_counts=True,
            )

            count_df_new = pd.DataFrame(
                index=unique_codes, data=counts, columns=["counts_" + hist]
            )
            count_df = pd.concat([count_df, count_df_new], axis=1)
        count_df["total_counts"] = count_df.sum(axis=1)
        unique_codes = count_df.index.values
        counts = count_df["total_counts"].values
        count_order = counts.argsort()
        unique_codes = unique_codes[count_order]
        counts = counts[count_order]
        sorted_codes = unique_codes.tolist()
        sorted_codes_dict = {}
        for i, k in enumerate(sorted_codes):
            election_date = self.ne_hist_date(k, hist_code_df)
            if election_date:
                sorted_codes_dict[k] = {
                    "index": i,
                    "count": int(counts[i]),
                    "date": election_date,
                }
            else:
                logging.info(
                    f"removing election {k} for not having an election date in the file"
                )
                sorted_codes.remove(k)

        return sorted_codes, sorted_codes_dict

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )
        history_code_df = pd.DataFrame()
        if not self.ignore_checks:
            self.file_check(len(new_files))
        for f in new_files:
            filename = f["name"].replace(" ", "").lower()
            if "psr" in filename and ".txt" in filename:
                logging.info(
                    "Reading Nebraska voter file from {}".format(f["name"])
                )
                df = self.read_csv_count_error_lines(
                    f["obj"],
                    sep="\t",
                    index_col=False,
                    on_bad_lines="warn",
                    encoding="latin-1",
                )
            if "historycode" in filename and "donotuse" not in filename:
                if ".csv" in filename:
                    logging.info("Reading csv Nebraska history code file")
                    history_code_df = self.read_csv_count_error_lines(
                        f["obj"], on_bad_lines="warn", encoding="latin-1"
                    )
                elif ".xlsx" in filename and "donotuse" not in filename:
                    logging.info("Reading Excel Nebraska history code file")
                    history_code_df = pd.read_excel(f["obj"], dtype=str)
        df[self.config["voter_status"]] = df[
            self.config["voter_status"]
        ].str.replace(" ", "")

        if history_code_df.empty:
            raise ValueError("History Code File Missing")

        history_code_df["text_election_code"] = history_code_df[
            "text_election_code"
        ].str.replace(" ", "")

        if "birth_year" in df.columns:
            df.rename(columns={"birth_year": "date_of_birth"}, inplace=True)

        sorted_codes, sorted_codes_dict = self.add_history(
            main_df=df, hist_code_df=history_code_df
        )

        df["all_history"] = df[self.config["hist_columns"]].apply(
            lambda x: list(x.dropna().str.replace(" ", "")), axis=1
        )

        def insert_code_bin(arr):
            """
            Takes the all history column and makes it into a sparse list, containing
            just the indexes corresponding to the sorted code dict, for each election code
            in the file
                Args:
                    arr: the array of values in the all history column
                Returns:
                    history_list: the list of sparse index values for the history file
            """
            history_list = []
            for k in arr:
                try:
                    history_list.append(sorted_codes_dict[k]["index"])
                except KeyError:
                    pass
            return history_list

        df["sparse_history"] = df["all_history"].apply(insert_code_bin)

        expected_cols = (
            self.config["ordered_columns"]
            + self.config["ordered_generated_columns"]
        )
        df = self.reconcile_columns(df, expected_cols)

        df = self.config.coerce_numeric(
            df,
            extra_cols=[
                "text_res_zip5",
                "text_mail_zip5",
                "text_mail_zip4",
                "polling_place_text_zip5",
                "polling_place_text_zip4",
            ],
        )
        df = self.config.coerce_strings(df)
        df = self.config.coerce_dates(df)

        # Check the file for all the proper locales
        self.locale_check(
            set(df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "nebraska_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
