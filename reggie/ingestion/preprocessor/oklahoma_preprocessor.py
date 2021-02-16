from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from dateutil import parser
from reggie.ingestion.utils import MissingNumColumnsError, format_column_name
import logging
import pandas as pd
import datetime
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
import numpy as np
from datetime import datetime
import gc
import json


class PreprocessOklahoma(Preprocessor):
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

        new_files = self.unpack_files(self.main_file)

        voter_files = [n for n in new_files if "vr.csv" in n["name"].lower()]
        hist_files = [n for n in new_files if "vh.csv" in n["name"].lower()]
        precinct_file = [
            n for n in new_files if "precinct" in n["name"].lower()
        ][0]

        # --- handling the vote history file --- #

        df_hist = pd.concat(
            [pd.read_csv(n["obj"], dtype=str) for n in hist_files],
            ignore_index=True,
        )

        election_dates = pd.to_datetime(
            df_hist.loc[:, "ElectionDate"], errors="coerce"
        ).dt
        elections, counts = np.unique(election_dates.date, return_counts=True)
        sorted_elections_dict = {
            str(k): {
                "index": i,
                "count": int(counts[i]),
                "date": k.strftime("%m/%d/%Y"),
            }
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "all_history"] = election_dates.date.apply(str)
        df_hist.loc[:, "sparse_history"] = df_hist.loc[:, "all_history"].map(
            lambda x: int(sorted_elections_dict[x]["index"])
        )

        voter_groups = df_hist.groupby(self.config["voter_id"])
        all_history = voter_groups["all_history"].apply(list)
        sparse_history = voter_groups["sparse_history"].apply(list)
        votetype_history = (
            voter_groups["VotingMethod"].apply(list).rename("votetype_history")
        )
        df_hist = pd.concat(
            [all_history, sparse_history, votetype_history], axis=1
        )

        # --- handling the voter file --- #

        # no primary locale column, county code is in file name only
        dfs = []
        for n in voter_files:
            df = pd.read_csv(n["obj"], dtype=str)
            df.loc[:, "county_code"] = str(n["name"][-9:-7])
            dfs.append(df)

        df_voter = pd.concat(dfs, ignore_index=True)

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_strings(
            df_voter, exclude=[self.config["voter_id"]]
        )
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = df_voter.loc[:, ~df_voter.columns.str.contains("Hist\w+\d")]
        df_voter = df_voter.set_index(self.config["voter_id"])

        df_voter = df_voter.join(df_hist)

        self.meta = {
            "message": "oklahoma_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
