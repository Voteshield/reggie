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


class PreprocessMaryland(Preprocessor):
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
        voter_file = [n for n in new_files if "list.txt" in n["name"].lower()][
            0
        ]
        # separate file with extra data, absentee votes still counted in voter_file
        abs_file = [
            n for n in new_files if "absentee.txt" in n["name"].lower()
        ][0]

        # --- handling voter history --- #
        df_hist = pd.read_csv(hist_file["obj"], sep="\t", dtype=str)

        election_dates = pd.to_datetime(df_hist.loc[:, "Election Date"])
        election_names = (
            df_hist.loc[:, "Election Description"]
            .str.extract("(\D+)", expand=False)
            .str.strip()
            .str.replace(" ", "_")
        )

        df_hist.loc[:, "all_history"] = (
            election_dates.dt.strftime("%Y_%m_%d_")
            + election_names.str.lower()
        )
        df_hist.loc[:, "earlyvote_history"] = "N"
        df_hist.loc[
            ~df_hist.loc[:, "Early Voting Location"].isna(),
            "earlyvote_history",
        ] = "Y"
        df_hist.loc[:, "votetype_history"] = (
            df_hist.loc[:, "Voting Method"].str.replace(" ", "_").str.lower()
        )
        df_hist.loc[:, "party_history"] = df_hist.loc[
            :, "Political Party"
        ].str.lower()
        df_hist.loc[:, "jurisd_history"] = (
            df_hist.loc[:, "Jurisdiction Code"].astype(str).str.strip()
        )
        df_hist.loc[:, "Election Date"] = pd.to_datetime(
            df_hist.loc[:, "Election Date"]
        )

        elections = (
            df_hist.groupby(["all_history", "Election Date"])["Voter ID"]
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

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.apply(
            lambda x: sorted_elections_dict[x]["index"]
        )
        history = list(df_hist.loc[:, "all_history":].columns)
        df_hist = df_hist.loc[:, ["Voter ID"] + history].rename(
            {"Voter ID": self.config["voter_id"]}, axis=1
        )
        group = df_hist.groupby(self.config["voter_id"])
        df_hist = pd.concat(
            [group[c].apply(list) for c in df_hist.columns[1:]], axis=1
        )

        # --- handling voter file --- #
        df_voter = (
            pd.read_csv(voter_file["obj"], sep="\t", dtype=str)
            .iloc[:, : len(self.config["column_names"])]
            .set_index(self.config["voter_id"])
        )
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.join(df_hist)

        self.meta = {
            "message": "maryland_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
