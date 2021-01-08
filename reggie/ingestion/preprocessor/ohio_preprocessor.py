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
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
import numpy as np
from datetime import datetime
import gc
import json


class PreprocessOhio(Preprocessor):
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

        new_files = self.unpack_files(file_obj=self.main_file)

        if not self.ignore_checks:
            self.file_check(len(new_files))

        for i in new_files:
            logging.info("Loading file {}".format(i))
            if "_22" in i["name"]:
                df = self.read_csv_count_error_lines(
                    i["obj"],
                    encoding="latin-1",
                    compression="gzip",
                    error_bad_lines=False,
                )
            elif ".txt" in i["name"]:
                temp_df = self.read_csv_count_error_lines(
                    i["obj"],
                    encoding="latin-1",
                    compression="gzip",
                    error_bad_lines=False,
                )
                df = pd.concat([df, temp_df], axis=0)

        # create history meta data
        voting_history_cols = list(
            filter(
                lambda x: any(
                    [pre in x for pre in ("GENERAL-", "SPECIAL-", "PRIMARY-")]
                ),
                df.columns.values,
            )
        )
        self.column_check(list(set(df.columns) - set(voting_history_cols)))
        total_records = df.shape[0]
        sorted_codes = voting_history_cols
        sorted_codes_dict = {
            k: {
                "index": i,
                "count": int(total_records - df[k].isna().sum()),
                "date": date_from_str(k),
            }
            for i, k in enumerate(voting_history_cols)
        }
        self.meta = {
            "message": "ohio_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
