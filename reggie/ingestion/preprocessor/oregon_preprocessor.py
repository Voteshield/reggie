import datetime
import gc
import json
import logging

from datetime import datetime
from dateutil import parser
from io import StringIO, BytesIO, SEEK_END, SEEK_SET

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import (
    format_column_name,
    MissingNumColumnsError,
)


class PreprocessOregon(Preprocessor):
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

        new_files = [
            f
            for f in self.unpack_files(self.main_file, compression="unzip")
            if "readme" not in f["name"].lower()
        ]

        voter_file = [
            n for n in new_files if not "readme" in n["name"].lower()
        ][0]
        # don't have access to them yet
        # hist_files = ...

        # --- handling voter file --- #

        df_voter = pd.read_csv(voter_file["obj"], sep="\t", dtype=str).dropna(
            how="all", axis=1
        )

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_strings(df_voter, exclude=["STATE"])
        df_voter = self.config.coerce_numeric(df_voter)

        df_voter.loc[:, self.config["voter_id"]] = (
            df_voter.loc[:, self.config["voter_id"]].str.zfill(9).astype("str")
        )
        df_voter.loc[:, "UNLISTED"] = df_voter.loc[:, "UNLISTED"].map(
            {"yes": True, "no": False}
        )
        # when vote history is received

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voter[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "oregon_{}".format(datetime.now().isoformat()),
            # vote history not available yet
            # 'array_encoding': json.dumps(),
            # 'array_decoding': json.dumps()
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
