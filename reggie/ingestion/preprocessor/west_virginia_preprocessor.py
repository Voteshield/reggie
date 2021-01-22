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


class PreprocessSTATE(Preprocessor):
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
            n for n in self.unpack_files(self.main_file, compression="unzip")
        ]

        # only one voter file, no voter history
        voter_file = [n for n in new_files if "wv" in n["name"].lower()][0]

        df_voter = pd.read_csv(
            voter_file["obj"], sep="|", encoding="latin-1", dtype=str, header=0
        )

        # --- handling voter file --- #

        gender_conversion_dict = {}
        for c, v in self.config["gender_codes"].items():
            for i in v:
                if i is None:
                    gender_conversion_dict[" "] = c
                gender_conversion_dict[i] = c

        df_voter.loc[:, "SEX"] = df_voter.loc[:, "SEX"].map(
            gender_conversion_dict
        )

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_strings(
            df_voter, exclude=[self.config["voter_id"]]
        )

        # coerce_strings does not convert party_identifier but conversion is needed in this instance
        df_voter.loc[:, self.config["party_identifier"]] = (
            df_voter.loc[:, self.config["party_identifier"]]
            .str.replace("\W", " ")
            .str.strip()
        )

        df_voter = df_voter.set_index(self.config["voter_id"])

        self.meta = {
            "message": "west_virginia_{}".format(datetime.now().isoformat())
            # vote history not available
            #            'array_encoding': json.dumps(),
            #            'array_decoding': json.dumps()
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
