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


class PreprocessNevada(Preprocessor):
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

        if not self.ignore_checks:
            self.file_check(len(new_files))
        voter_file = (
            new_files[0] if "ElgbVtr" in new_files[0]["name"] else new_files[1]
        )
        hist_file = (
            new_files[0] if "VtHst" in new_files[0]["name"] else new_files[1]
        )

        df_hist = self.read_csv_count_error_lines(
            hist_file["obj"], header=None, error_bad_lines=False
        )
        df_hist.columns = self.config["hist_columns"]
        df_voters = self.read_csv_count_error_lines(
            voter_file["obj"], header=None, error_bad_lines=False
        )

        try:
            df_voters.columns = self.config["ordered_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for Nevada")
            raise MissingNumColumnsError(
                "{} state is missing columns".format(self.state),
                self.state,
                len(self.config["ordered_columns"]),
                len(df_voters.columns),
            )

        sorted_codes = df_hist.date.unique().tolist()
        sorted_codes.sort(key=lambda x: datetime.strptime(x, "%m/%d/%Y"))
        counts = df_hist.date.value_counts()
        sorted_codes_dict = {
            k: {
                "index": i,
                "count": int(counts.loc[k]),
                "date": date_from_str(k),
            }
            for i, k in enumerate(sorted_codes)
        }

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]["index"] for k in arr]
            else:
                return np.nan

        df_voters = df_voters.set_index("VoterID", drop=False)
        voter_id_groups = df_hist.groupby("VoterID")
        df_voters["all_history"] = voter_id_groups["date"].apply(list)
        df_voters["votetype_history"] = voter_id_groups["vote_code"].apply(
            list
        )
        df_voters["sparse_history"] = df_voters["all_history"].map(
            insert_code_bin
        )

        # create compound string for unique voter ID from county ID
        df_voters["County_Voter_ID"] = (
            df_voters["County"].str.replace(" ", "").str.lower()
            + "_"
            + df_voters["County_Voter_ID"].astype(int).astype(str)
        )
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(
            df_voters,
            extra_cols=[
                "Zip",
                "Phone",
                "Congressional_District",
                "Senate_District",
                "Assembly_District",
                "Education_District",
                "Regent_District",
                "Registered_Precinct",
            ],
        )
        df_voters = self.config.coerce_strings(df_voters)

        self.meta = {
            "message": "nevada_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voters.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
