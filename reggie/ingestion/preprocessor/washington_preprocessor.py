import datetime
import json
import logging

from datetime import datetime
from dateutil import parser
from io import StringIO

import numpy as np
import pandas as pd

from detect_delimiter import detect

from reggie.ingestion.download import (
    FileItem,
    Preprocessor,
    concat_and_delete,
    date_from_str,
)
from reggie.ingestion.utils import (
    MissingNumColumnsError,
    format_column_name,
)

class PreprocessWashington(Preprocessor):
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

        new_files = [
            n
            for n in self.unpack_files(self.main_file, compression="unzip")
            if ("pdf" not in n["name"].lower())
        ]

        # there should be only one voter file
        voter_file = [n for n in new_files if "vrdb" in n["name"].lower()][0]
        hist_files = [n for n in new_files if "history" in n["name"].lower()]

        # There are two possible separators. Detect it first.
        line = voter_file["obj"].readline().decode()
        delimiter = detect(line)
        voter_file["obj"].seek(0)
        df_voter = pd.read_csv(
            voter_file["obj"], sep=delimiter, encoding="latin-1", dtype=str
        )
        df_hist = pd.DataFrame()
        for hist_file in hist_files:
            line = hist_file["obj"].readline().decode()
            delimiter = detect(line)
            hist_file["obj"].seek(0)
            temp = pd.read_csv(
                hist_file["obj"], sep=delimiter, encoding="latin-1", dtype=str
            )
            df_hist = df_hist.append(temp, ignore_index=True)

        # --- handling the voter history file --- #

        # TODO: REMOVE THIS, Randomly selecting subset of rows for testing
        df_hist = df_hist.sample(100000)

        # Need to fix the differently named VoterHistoryID
        # and VotingHistoryID columns
        if {"VotingHistoryID", "VoterHistoryID"}.issubset(df_hist.columns):
            df_hist["VotingHistoryID"] = (
                df_hist.pop("VoterHistoryID").fillna(
                    df_hist.pop("VotingHistoryID")
                )
            )

        # can't find voter history documentation in any yaml, hardcoding column name
        election_dates = pd.to_datetime(
            df_hist.loc[:, "ElectionDate"], errors="coerce"
        ).dt

        elections, counts = np.unique(election_dates.date, return_counts=True)
        sorted_elections_dict = {
            str(k): {
                "index": i,
                "count": int(counts[i]),
                "date": k.strftime("%Y-%m-%d"),
            }
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "all_history"] = election_dates.date.apply(str)
        df_hist.loc[:, "sparse_history"] = df_hist.loc[:, "all_history"].map(
            lambda x: int(sorted_elections_dict[x]["index"])
        )
        df_hist.loc[:, "county_history"] = df_hist.loc[
            :, self.config["primary_locale_identifier"]
        ]

        voter_groups = df_hist.groupby(self.config["voter_id"])
        all_history = voter_groups["all_history"].apply(list)
        sparse_history = voter_groups["sparse_history"].apply(list)
        county_history = voter_groups["county_history"].apply(list)
        df_hist = pd.concat(
            [all_history, sparse_history, county_history], axis=1
        )

        # --- handling the voter file --- #

        # some columns have become obsolete
        df_voter = df_voter.loc[
            :, df_voter.columns.isin(self.config["column_names"])
        ]
        df_voter = df_voter.set_index(self.config["voter_id"])

        # pandas loads any numeric column with NaN values as floats
        # causing formatting trouble during execute() with a few columns
        # saw this solution in other states (arizona & texas)
        to_numeric = [
            df_voter.loc[:, col].str.isnumeric().all()
            for col in df_voter.columns
        ]
        df_voter.loc[:, to_numeric] = (
            df_voter.loc[:, to_numeric].fillna(-1).astype(int)
        )

        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_strings(
            df_voter,
            exclude=[
                self.config["primary_locale_identifier"],
                self.config["voter_id"],
            ],
        )
        df_voter = self.config.coerce_dates(df_voter)

        # add voter history
        df_voter = df_voter.join(df_hist)

        self.meta = {
            "message": "washington_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        # TODO: REMOVE THIS
        df_voter.to_csv("WAtest.csv", encoding="utf-8", index=True)

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
