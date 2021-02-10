from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime
import json


class PreprocessArkansas(Preprocessor):
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

        voter_file = [n for n in new_files if "vr.csv" == n["name"].lower()][0]
        hist_file = [n for n in new_files if "vh.csv" == n["name"].lower()][0]

        # --- handling the vote history file --- #
        df_hist = pd.read_csv(hist_file["obj"], dtype=str)

        elections = pd.Series(self.config["elections"])
        election_votetype = elections + "HowVoted"
        election_party = elections + "PartyVoted"
        election_county = elections + "CountyVotedIn"

        election_cols = zip(
            *[elections, election_votetype, election_party, election_county]
        )

        election_dfs = []
        for e in election_cols:
            election_df = df_hist.set_index(self.config["voter_id"])
            election_df = election_df.loc[:, election_df.columns.isin(e)]
            election_df = election_df.dropna(how="all")
            election_df.columns = [
                "all_history",
                "county_history",
                "party_history",
                "votetype_history",
            ]
            election_df.loc[:, "all_history"] = e[0]
            election_dfs.append(election_df.reset_index())

        df_hist = pd.concat(election_dfs, ignore_index=True)
        df_hist = df_hist.fillna("NP").applymap(lambda x: x.strip(" "))

        elections, counts = np.unique(df_hist.all_history, return_counts=True)
        order = np.argsort(counts)[::-1]
        counts = counts[order]
        elections = elections[order]
        election_years = list(
            pd.to_datetime(
                (
                    "20"
                    + pd.Series(elections).str.extract(
                        "(\d{2}(?!\d))", expand=False
                    )
                )
            ).dt.year
        )

        sorted_elections_dict = {
            k: {
                "index": i,
                "count": int(counts[i]),
                "date": str(election_years[i]),
            }
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.map(
            lambda x: int(sorted_elections_dict[x]["index"])
        )

        group = df_hist.groupby(self.config["voter_id"])
        df_hist = pd.concat(
            [group[col].apply(list) for col in df_hist.columns[1:]], axis=1
        )

        # --- handling the voter file --- #
        df_voter = pd.read_csv(voter_file["obj"], dtype=str)

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_strings(
            df_voter, exclude=[self.config["voter_id"]]
        )

        df_voter = df_voter.set_index(self.config["voter_id"]).join(df_hist)

        self.meta = {
            "message": "arkansas_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
