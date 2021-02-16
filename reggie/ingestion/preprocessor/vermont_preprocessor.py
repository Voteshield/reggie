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


class PreprocessVermont(Preprocessor):
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
            n for n in new_files if "voter file" in n["name"].lower()
        ][0]

        df_voter = pd.read_csv(voter_file["obj"], sep="|", dtype=str).iloc[
            :, : len(self.config["column_names"])
        ]
        df_voter.columns = self.config["column_names"]

        df_hist = df_voter.loc[
            :, [self.config["voter_id"]] + self.config["election_columns"]
        ]
        for c in df_hist.columns[1:]:
            df_hist.loc[:, c] = df_hist.loc[:, c].map(
                {"T": c[: c.find(" Part")].replace(" ", "_"), "F": np.nan}
            )

        df_hist = (
            df_hist.set_index(self.config["voter_id"])
            .stack()
            .reset_index(level=1, drop=True)
            .reset_index()
        )
        df_hist.columns = [self.config["voter_id"], "all_history"]

        elections, counts = np.unique(df_hist.all_history, return_counts=True)

        sorted_elections_dict = {
            k: {"index": i, "count": int(counts[i]), "date": str(k[:4])}
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.apply(
            lambda x: sorted_elections_dict[x]["index"]
        )

        group = df_hist.groupby(self.config["voter_id"])
        df_hist = pd.concat(
            [group[col].apply(list) for col in df_hist.columns[1:]], axis=1
        )

        df_voter = df_voter.loc[
            :, ~df_voter.columns.isin(self.config["election_columns"])
        ]
        df_voter = df_voter.set_index(self.config["voter_id"])

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = df_voter.join(df_hist)

        self.meta = {
            "message": "vermont_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
