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


class PreprocessWyoming(Preprocessor):
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
            n for n in new_files if "statewide" in n["name"].lower()
        ][0]
        hist_files = [n for n in new_files if "history" in n["name"].lower()]

        # --- handling voter history --- #

        election_col = self.config["election_columns"]
        elections = self.config["elections"]

        df_hist = []

        for file in hist_files:
            text = file["obj"].readline()
            file["obj"].seek(0)

            if b"\t" in text:
                df = pd.read_csv(file["obj"], sep="\t", dtype=str)
            elif b"," in text:
                df = pd.read_csv(file["obj"], sep=",", dtype=str)

            election_type = file["name"][: file["name"].find(" Vot")]

            if not election_type in elections:
                print(
                    "Warning:",
                    election_type,
                    "not in documentation. Some fields may be excluded.",
                )

            for var, names in election_col.items():
                for col in df.columns:
                    if col in names:
                        df = df.rename({col: var}, axis=1)
                if var not in df.columns:
                    df.loc[:, var] = "NP"

            df = df.loc[:, election_col.keys()]

            df.loc[:, "election_type"] = election_type

            df_hist.append(df)

        df_hist = (
            pd.concat(df_hist, ignore_index=True)
            .dropna(how="any")
            .applymap(lambda x: str(x).strip())
        )

        df_hist.loc[:, "all_history"] = (
            df_hist.loc[:, "election_type"]
            .str.lower()
            .str.extract("(\d+\s+[g|p]\w+)", expand=False)
            .str.split("\s")
            .str.join("_")
        )
        df_hist.loc[:, "election_date"] = pd.to_datetime(
            df_hist.loc[:, "election_date"].replace("NP", pd.NaT)
        ).dt.strftime("%m/%d/%Y")

        election_dates_dict = (
            df_hist.groupby("all_history")["election_date"].first().to_dict()
        )
        elections, counts = np.unique(
            df_hist.loc[:, "all_history"], return_counts=True
        )

        sorted_elections_dict = {
            str(k): {
                "index": i,
                "count": int(counts[i]),
                "date": election_dates_dict[k],
            }
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.loc[:, "all_history"].map(
            lambda x: int(sorted_elections_dict[x]["index"])
        )

        voter_groups = df_hist.sort_values("election_type").groupby(
            self.config["voter_id"]
        )

        all_history = voter_groups["all_history"].apply(list)
        sparse_history = voter_groups["sparse_history"].apply(list)
        votetype_history = (
            voter_groups["vote_method"].apply(list).rename("votetype_history")
        )
        party_history = (
            voter_groups[self.config["party_identifier"]]
            .apply(list)
            .rename("party_history")
        )
        precinct_history = (
            voter_groups["precinct"].apply(list).rename("precinct_history")
        )

        df_hist = pd.concat(
            [
                all_history,
                sparse_history,
                votetype_history,
                party_history,
                precinct_history,
            ],
            axis=1,
        )

        # --- handling voter file --- #

        df_voter = pd.read_csv(voter_file["obj"], dtype=str)

        df_voter = self.config.coerce_strings(
            df_voter, exclude=[self.config["voter_id"]]
        )
        df_voter = self.config.coerce_numeric(
            df_voter,
            extra_cols=[
                "Zip (RA)",
                "Split",
                "Precinct",
                "ZIP (MA)",
                "House",
                "Senate",
            ],
        )
        df_voter = self.config.coerce_dates(df_voter)

        df_voter.loc[:, self.config["voter_id"]] = (
            df_voter.loc[:, self.config["voter_id"]].str.zfill(9).astype(str)
        )
        df_voter = df_voter.set_index(self.config["voter_id"])

        df_voter = df_voter.join(df_hist)

        self.meta = {
            "message": "wyoming_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.is_compressed = False

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="latin-1", index=True)),
            s3_bucket=self.s3_bucket,
        )
