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
    concat_and_delete,
)
from reggie.ingestion.utils import (
    MissingNumColumnsError,
    format_column_name,
)


class PreprocessDelaware(Preprocessor):
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
            n for n in new_files if "voter_reg" in n["name"].lower()
        ][0]
        df_voter = pd.read_csv(voter_file["obj"], sep="\t", dtype=str)

        # --- handling vote history --- #

        df_hist = (
            df_voter.set_index(self.config["voter_id"])
            .loc[:, self.config["election_columns"]]
            .stack()
            .reset_index(level=1, drop=True)
            .reset_index()
        )
        df_hist.columns = [self.config["voter_id"], "all_history"]

        elections, counts = np.unique(df_hist.all_history, return_counts=True)
        order = np.argsort(counts)[::-1]
        elections = elections[order]
        counts = counts[order]
        election_year = list(
            pd.Series(elections)
            .str[-2:]
            .apply(lambda x: "20" + x if int(x) < 50 else "19" + x)
        )

        sorted_elections_dict = {
            k: {
                "index": i,
                "count": int(counts[i]),
                "date": str(election_year[i]),
            }
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.apply(
            lambda x: sorted_elections_dict[x]["index"]
        )

        group = df_hist.groupby(self.config["voter_id"])
        df_hist = pd.concat(
            [group[c].apply(list) for c in ["all_history", "sparse_history"]],
            axis=1,
        )

        # --- handling voter file  --- #

        df_voter = df_voter.loc[
            :, ~df_voter.columns.isin(self.config["election_columns"])
        ].set_index(self.config["voter_id"])
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.join(df_hist)

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voter[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "delaware_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
