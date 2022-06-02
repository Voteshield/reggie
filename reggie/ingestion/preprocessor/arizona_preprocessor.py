import datetime
import gc
import json
import logging

from datetime import datetime
from dateutil import parser
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from reggie.ingestion.utils import (
    MissingNumColumnsError,
    df_to_postgres_array_string,
)


class PreprocessArizona(Preprocessor):
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

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )
        new_files = [f for f in new_files if "LEGEND.xlsx" not in f["name"]]

        combined_file = self.concat_file_segments(new_files)

        main_df = self.read_csv_count_error_lines(
            combined_file, error_bad_lines=False
        )

        voting_action_cols = list(
            filter(lambda x: "party_voted" in x, main_df.columns.values)
        )
        voting_method_cols = list(
            filter(lambda x: "voting_method" in x, main_df.columns.values)
        )
        all_voting_history_cols = voting_action_cols + voting_method_cols

        main_df["all_history"] = df_to_postgres_array_string(
            main_df, voting_action_cols
        )
        main_df["all_voting_methods"] = df_to_postgres_array_string(
            main_df, voting_method_cols
        )
        main_df[self.config["birthday_identifier"]] = pd.to_datetime(
            main_df[self.config["birthday_identifier"]]
            .fillna(-1)
            .astype(int)
            .astype(str),
            format=self.config["date_format"],
            errors="coerce",
        )
        elections_key = [c.split("_")[-1] for c in voting_action_cols]

        main_df.drop(all_voting_history_cols, axis=1, inplace=True)

        main_df.columns = main_df.columns.str.strip(" ")
        main_df = self.config.coerce_numeric(
            main_df,
            extra_cols=[
                "text_mail_zip5",
                "text_mail_zip4",
                "text_phone_last_four",
                "text_phone_exchange",
                "text_phone_area_code",
                "precinct_part_text_name",
                "precinct_part",
                "occupation",
                "text_mail_carrier_rte",
                "text_res_address_nbr",
                "text_res_address_nbr_suffix",
                "text_res_unit_nbr",
                "text_res_carrier_rte",
                "text_mail_address1",
                "text_mail_address2",
                "text_mail_address3",
                "text_mail_address4",
            ],
        )

        # Check the file for all the proper locales
        self.locale_check(
            set(main_df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "arizona_{}".format(datetime.now().isoformat()),
            "array_dates": json.dumps(elections_key),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(main_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
