from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from dateutil import parser
from reggie.ingestion.utils import MissingNumColumnsError
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime
import gc
import json


class PreprocessMontana(Preprocessor):
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

        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(self.main_file, compression="unzip")

        voter_file = [n for n in new_files if "voter_ex" in n["name"].lower()][
            0
        ]
        hist_file = [n for n in new_files if "voter_his" in n["name"].lower()][
            0
        ]

        # --- handling voter history --- #

        df_hist = pd.read_csv(hist_file["obj"], dtype=str).rename(
            {"Voter ID": self.config["voter_id"]}, axis=1
        )

        election_codes = {
            str(v): k for k, v in self.config["election_codes"].items()
        }
        votetype_codes = {
            str(v): k for k, v in self.config["votetype_codes"].items()
        }

        df_hist.loc[:, "BALLOTSTAGE/STATUS"] = (
            df_hist.loc[:, "BALLOTSTAGE/STATUS"]
            .map(
                {
                    "Processed/Accepted": "absentee-ACCEPTED",
                    "Sent": "absentee-SENT",
                    "Processed/Rejected": "absentee-REJECTED",
                    "Undeliverable": "absentee-UNDELIVERABLE",
                }
            )
            .fillna("non-absentee")
        )

        df_hist = df_hist.loc[
            df_hist["BALLOTSTAGE/STATUS"].isin(
                ["non-absentee", "absentee-ACCEPTED"]
            ),
            :,
        ]

        df_hist.loc[:, "ELECTION_TYPE"] = df_hist.loc[:, "ELECTION_TYPE"].map(
            election_codes
        )

        # if the election code does not exist, take a clean version of the election description
        df_hist.loc[
            df_hist["ELECTION_TYPE"].isna(), "ELECTION_DESCRIPTION"
        ] = (
            df_hist.loc[
                df_hist["ELECTION_TYPE"].isna(), "ELECTION_DESCRIPTION"
            ]
            .str.lower()
            .str.split(" ")
            .str.join("_")
        )
        # will use later
        election_dates = pd.to_datetime(df_hist.loc[:, "ELECTION_DATE"])
        df_hist.loc[:, "ELECTION_DATE"] = election_dates.dt.strftime(
            "%Y-%m-%d"
        )

        # creating election ids
        df_hist.loc[:, "all_history"] = (
            election_dates.dt.strftime("%Y_%m_%d_")
            + df_hist.loc[:, "ELECTION_TYPE"]
        )
        df_hist.loc[:, "votetype_history"] = df_hist.loc[:, "VVM_ID"].map(
            votetype_codes
        )
        df_hist.loc[:, "county_history"] = df_hist.loc[:, "JS_CODE"].fillna(0)

        elections = (
            df_hist.groupby(["all_history", "ELECTION_DATE"])[
                self.config["voter_id"]
            ]
            .count()
            .reset_index()
            .values
        )

        sorted_elections_dict = {
            k[0]: {"index": i, "count": int(k[2]), "date": str(k[1])}
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.loc[:, "all_history"].map(
            lambda x: sorted_elections_dict[x]["index"]
        )

        df_hist = df_hist.loc[
            :,
            [
                self.config["voter_id"],
                "all_history",
                "votetype_history",
                "county_history",
                "sparse_history",
            ],
        ]

        df_group = df_hist.groupby(self.config["voter_id"])
        groups = []
        for col in df_hist.columns[1:]:
            group = df_group[col].apply(list)
            groups.append(group)

        df_hist = pd.concat(groups, axis=1)

        # --- handling voter file --- #

        df_voter = pd.read_csv(voter_file["obj"], sep="\t", index_col=False)
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.set_index(self.config["voter_id"]).join(df_hist)

        self.meta = {
            "message": "montana_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
