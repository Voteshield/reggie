from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from dateutil import parser
from reggie.ingestion.utils import (
    MissingNumColumnsError,
    format_column_name,
)
import logging
import pandas as pd
import datetime
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
import numpy as np
from datetime import datetime
import gc
import json


class PreprocessAlaska(Preprocessor):
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

        new_files = self.unpack_files(self.main_file, compression="unzip")
        voter_file = [n for n in new_files if "voter" in n["name"].lower()][0]

        df_voter = pd.read_csv(voter_file["obj"], dtype=str).drop("UN", axis=1)
        df_hist = df_voter.loc[
            :, [self.config["voter_id"]] + self.config["election_columns"]
        ]

        # --- handling the vote history file --- #

        df_hist = (
            df_hist.set_index(self.config["voter_id"])
            .stack()
            .reset_index()
            .iloc[:, [0, 2]]
        )
        df_hist.columns = [self.config["voter_id"], "election"]

        df_hist = df_hist.join(df_hist.election.str.split(" ", expand=True))
        df_hist = df_hist.rename(
            {0: "all_history", 1: "votetype_history"}, axis=1
        )

        df_hist = df_hist.join(
            df_hist.all_history.str.split("(?<=^\d{2})", expand=True)
        )
        df_hist = df_hist.rename(
            {0: "election_year", 1: "election_type"}, axis=1
        )

        df_hist.election_year = "20" + df_hist.election_year

        elections, counts = np.unique(
            df_hist.loc[:, ["all_history", "election_year"]].apply(
                tuple, axis=1
            ),
            return_counts=True,
        )

        sorted_elections_dict = {
            k[0]: {"index": i, "count": int(counts[i]), "date": int(k[1])}
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.map(
            lambda x: sorted_elections_dict[x]["index"]
        )

        df_hist = pd.concat(
            [
                df_hist.groupby(self.config["voter_id"])[c].apply(list)
                for c in ["all_history", "votetype_history", "sparse_history"]
            ],
            axis=1,
        )

        # --- handling the voter file --- #

        df_voter = df_voter.loc[
            :, ~df_voter.columns.isin(self.config["election_columns"])
        ]
        df_voter = df_voter.set_index(self.config["voter_id"])

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.join(df_hist)

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voter[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "alaska_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
