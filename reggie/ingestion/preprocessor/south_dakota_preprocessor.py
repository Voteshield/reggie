import datetime
import gc
import json
import logging

from datetime import datetime
from dateutil import parser
from io import StringIO, BytesIO, SEEK_END, SEEK_SET

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import (
    format_column_name,
    MissingNumColumnsError,
)


class PreprocessSouthDakota(Preprocessor):
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

        voter_file = [
            n for n in new_files if "searchexport" in n["name"].lower()
        ][0]
        hist_file = [n for n in new_files if "history" in n["name"].lower()][0]

        # --- handling voter history --- #

        df_hist = pd.read_csv(hist_file["obj"], dtype=str).rename(
            self.config["election_columns"], axis=1
        )

        df_hist.loc[:, "all_history"] = (
            df_hist.date + "_" + df_hist.election.str.lower()
        )

        elections = (
            df_hist.groupby(["all_history", "date"])[self.config["voter_id"]]
            .count()
            .reset_index()
            .values
        )
        sorted_elections_dict = {
            k[0]: {"index": i, "count": int(k[2]), "date": k[1]}
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.loc[:, "all_history"].map(
            lambda x: sorted_elections_dict[x]["index"]
        )

        voter_groups = df_hist.groupby(self.config["voter_id"])
        df_hist = pd.concat(
            [
                voter_groups[c].apply(list)
                for c in ["all_history", "sparse_history", "votetype_history"]
            ],
            axis=1,
        )

        # --- handling voter file --- #

        df_voter = pd.read_csv(voter_file["obj"], skiprows=2, dtype=str)
        df_voter = self.config.coerce_strings(
            df_voter, exclude=[self.config["voter_id"]]
        )
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.set_index(self.config["voter_id"]).join(df_hist)

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voter[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "south_dakota_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
