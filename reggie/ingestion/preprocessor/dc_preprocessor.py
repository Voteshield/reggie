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
    concat_and_delete,
)
from reggie.ingestion.utils import (
    MissingLocaleError,
    MissingNumColumnsError,
    format_column_name,
)


class PreprocessDC(Preprocessor):
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
        voter_file = [n for n in new_files if "voters" in n["name"].lower()][0]

        df_voter = pd.read_excel(
            voter_file["obj"], sheet_name="DC VH EXPORT (ALL)", dtype=str
        )

        df_voter.loc[:, "REGISTERED"] = pd.to_datetime(
            df_voter.loc[:, "REGISTERED"]
        ).dt.strftime("%m/%d/%Y")

        # --- handling vote history --- #
        df_hist = df_voter.loc[
            :, df_voter.columns.isin(self.config["election_columns"])
        ]
        elections = df_hist.columns
        elections = elections.str.extract("(?P<date>\d+)-(?P<type>\D+)")
        election_dates = elections.date.apply(
            lambda x: x[:2] + "/20" + x[-2:]
            if int(x[-2:]) < 90
            else x[:2] + "/19" + x[-2:]
        )
        election_types = elections.type.map(
            {"P": "primary", "G": "general", "S": "special"}
        )
        election_keys = list(
            pd.to_datetime(election_dates, format="%m/%Y").dt.strftime(
                "%Y_%m_"
            )
            + election_types
        )

        df_hist.columns = election_keys

        votetype_codes = self.config["votetype_codes"]
        votetype_codes["N"] = np.nan
        votetype_codes["E"] = np.nan

        df_hist = (
            df_hist.apply(lambda x: x.map(votetype_codes))
            .stack()
            .reset_index()
        )
        df_hist.columns = ["index", "all_history", "votetype_history"]

        elections, counts = np.unique(df_hist.all_history, return_counts=True)
        election_dates = {k: v for k, v in zip(election_keys, election_dates)}
        sorted_elections_dict = {
            k: {
                "index": i,
                "count": int(counts[i]),
                "date": pd.to_datetime(election_dates[k]).strftime("%Y-%m-%d"),
            }
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.apply(
            lambda x: sorted_elections_dict[x]["index"]
        )
        df_hist = pd.concat(
            [
                df_hist.groupby("index")[c].apply(list)
                for c in ["all_history", "votetype_history", "sparse_history"]
            ],
            axis=1,
        )

        # --- handling voter file --- #
        df_voter = df_voter.loc[
            :, ~df_voter.columns.isin(self.config["election_columns"])
        ]
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = df_voter.join(df_hist).rename_axis("temp_id")

        # Check the file for all the proper locales
        try:
            self.locale_check(
                set(df_voter[self.config["primary_locale_identifier"]]),
            )
        except MissingLocaleError as mle:
            # Save the error for future reference
            self.missing_locale_error = mle
            logging.error(mle)

        self.meta = {
            "message": "dc_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
