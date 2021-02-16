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


class PreprocessRhodeIsland(Preprocessor):
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

        hist_file = [
            n for n in new_files if "history.txt" in n["name"].lower()
        ][0]
        voter_file = [
            n for n in new_files if "voter.txt" in n["name"].lower()
        ][0]

        # --- handling voter history --- #

        df_hist = pd.read_csv(hist_file["obj"], skiprows=1, sep="|", dtype=str)

        election_keys = [
            "election_names",
            "election_dates",
            "election_precints",
            "election_party",
            "election_votetype",
        ]

        election_dfs = []
        for election in zip(*[self.config[k] for k in election_keys]):
            cols = [self.config["voter_id"]] + list(election)
            election_df = df_hist.loc[:, cols].dropna()
            election_df.columns = [
                self.config["voter_id"],
                "all_history",
                "date",
                "precinct_history",
                "party_history",
                "votetype_history",
            ]
            election_dfs.append(election_df)

        election_df = pd.concat(election_dfs, ignore_index=True)

        election_dates = pd.to_datetime(election_df.date)
        election_df.loc[:, "date"] = election_dates.astype(str)
        election_df.loc[:, "all_history"] = election_dates.dt.strftime(
            "%Y_%m_%d_"
        ) + election_df.all_history.str.split(" ").str.join("_")

        elections = (
            election_df.groupby(["all_history", "date"])[
                self.config["voter_id"]
            ]
            .count()
            .reset_index()
            .values
        )

        sorted_elections_dict = {
            k[0]: {"index": i, "count": int(k[2]), "date": k[1]}
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        election_df.loc[:, "sparse_history"] = election_df.loc[
            :, "all_history"
        ].map(lambda x: int(sorted_elections_dict[x]["index"]))

        df_group = election_df.sort_values("date", ascending=True).groupby(
            self.config["voter_id"]
        )
        df_hist = pd.concat(
            [
                df_group[c].apply(list)
                for c in election_df.columns
                if "history" in c
            ],
            axis=1,
        )

        # --- handling vote file --- #

        df_voter = pd.read_csv(
            voter_file["obj"], sep="|", skiprows=1, dtype=str
        )
        df_voter = self.config.coerce_strings(
            df_voter, exclude=[self.config["voter_id"]]
        )
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter.loc[:, "ZIP CODE"] = (
            df_voter.loc[:, "ZIP CODE"].astype(str).str.zfill(5).fillna("-")
        )
        df_voter.loc[:, "ZIP4 CODE"] = (
            df_voter.loc[:, "ZIP4 CODE"].fillna("0").astype(int).astype(str)
        )

        df_voter = df_voter.set_index(self.config["voter_id"]).join(df_hist)

        self.meta = {
            "message": "rhode_island_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
