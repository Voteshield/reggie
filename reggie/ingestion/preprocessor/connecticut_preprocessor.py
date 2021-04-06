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


class PreprocessConnecticut(Preprocessor):
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
        voter_files = [n for n in new_files if "EXT" in n["name"]]

        election_columns = self.config["election_columns"]
        electiontype_columns = self.config["electiontype_columns"]
        votetype_columns = self.config["votetype_columns"]
        election_date_columns = self.config["election_date_columns"]

        electiontype_codes = {
            v: k for k, v in self.config["election_type_code"].items()
        }
        votetype_codes = {
            v: k for k, v in self.config["absentee_ballot_code"].items()
        }

        df_voter = pd.concat(
            [
                pd.read_csv(
                    f["obj"],
                    names=self.config["column_names"],
                    index_col=False,
                    sep=",",
                    dtype=str,
                    skipinitialspace=True,
                )
                for f in voter_files
            ],
            ignore_index=True,
        )

        # --- handling the vote history file --- #

        df_hist = df_voter.set_index(self.config["voter_id"]).loc[
            :, election_columns
        ]

        election_df = []
        election_zip = list(
            zip(election_date_columns, electiontype_columns, votetype_columns)
        )

        for c in election_zip:
            election = df_hist.loc[~df_hist[c[0]].isna(), c]
            election.columns = ["electiondate", "electiontype", "votetype"]
            electiondate = pd.to_datetime(election.loc[:, "electiondate"])

            election.loc[:, "electiondate"] = electiondate
            election.loc[:, "electiontype"] = (
                election.loc[:, "electiontype"]
                .str.strip()
                .map(electiontype_codes)
                .fillna("NP")
                .str.lower()
            )
            election.loc[:, "votetype_history"] = (
                election.loc[:, "votetype"]
                .str.strip()
                .map(votetype_codes)
                .fillna("NP")
                .str.lower()
            )
            election.loc[:, "all_history"] = (
                electiondate.dt.strftime("%Y_%m_%d_")
                + election.loc[:, "electiontype"]
            )

            election = election.loc[
                :, ["electiondate", "votetype_history", "all_history"]
            ]

            election_df.append(election)

        df_hist = pd.concat(election_df).reset_index()

        elections = (
            df_hist.groupby(["all_history", "electiondate"])[
                self.config["voter_id"]
            ]
            .count()
            .reset_index()
            .values
        )
        sorted_elections_dict = {
            k[0]: {
                "index": i,
                "count": int(k[2]),
                "date": k[1].strftime("%Y-%m-%d"),
            }
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist = df_hist.drop("electiondate", axis=1)

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.map(
            lambda x: sorted_elections_dict[x]["index"]
        )

        group = df_hist.groupby(self.config["voter_id"])
        election_df = []

        for c in df_hist.columns[1:]:
            election_df.append(group[c].apply(list))

        df_hist = pd.concat(election_df, axis=1)

        # --- handling the voter file --- #

        df_voter = df_voter.loc[:, ~df_voter.columns.isin(election_columns)]
        df_voter = df_voter.set_index(self.config["voter_id"])

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = df_voter.join(df_hist)

        self.meta = {
            "message": "connecticut_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
