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

        def hist_map(df, columns):
            def mapping(li):
                li = [x for x in li if x != "nan"]
                return li

            return pd.Series(
                map(mapping, df[columns].values.astype(str).tolist())
            )

        new_files = self.unpack_files(self.main_file, compression="unzip")
        self.file_check(len(new_files))
        voter_file = [
            n for n in new_files if "voter file" in n["name"].lower()
        ][0]

        vdf = pd.read_csv(voter_file["obj"], sep="|", dtype=str)
        unnamed_cols = vdf.columns[vdf.columns.str.contains("Unnamed")]
        vdf.drop(columns=unnamed_cols, inplace=True)
        election_columns = [
            col for col in vdf.columns if "election" in col.lower()
        ]
        cols_to_check = [x for x in vdf.columns if x not in election_columns]
        self.column_check(cols_to_check)
        # Will probably need to drop election columns for snapshot differencer

        vdf[self.config["party_identifier"]] = np.nan

        rename_dict = {
            col: col.replace(" Participation", "").replace(" ", "_")
            for col in election_columns
        }

        vdf.rename(columns=rename_dict, inplace=True)

        # Just need to sort once
        election_columns = sorted(list(rename_dict.values()))
        # Replacing the boolean values in the cells with the election name for processing
        for c in list(rename_dict.values()):
            vdf.loc[:, c] = vdf.loc[:, c].map(
                {"T": c.replace(" ", "_"), "F": np.nan}
            )

        election_counts = vdf[election_columns].count()
        sorted_codes_dict = {
            election_counts.index[i]: {
                "index": i,
                "count": k,
                "date": str(
                    datetime.strptime(election_counts.index[i][:4], "%Y")
                    .date()
                    .strftime("%m/%d/%Y")
                ),
            }
            for i, k in enumerate(election_counts)
        }
        sorted_elections = list(sorted_codes_dict.keys())

        vdf["all_history"] = hist_map(vdf[election_columns], election_columns)

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]["index"] for k in arr]
            else:
                return np.nan

        vdf.loc[:, "sparse_history"] = vdf.loc[:, "all_history"].map(
            insert_code_bin
        )

        vdf = vdf.set_index(self.config["voter_id"])

        vdf = self.config.coerce_strings(vdf)
        vdf = self.config.coerce_numeric(vdf)
        vdf = self.config.coerce_dates(vdf)

        self.meta = {
            "message": "vermont_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(vdf.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
