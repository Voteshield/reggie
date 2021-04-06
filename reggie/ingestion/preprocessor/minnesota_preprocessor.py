from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime
import gc
import json


class PreprocessMinnesota(Preprocessor):
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

        logging.info("Minnesota: loading voter file")
        new_files = self.unpack_files(
            compression="unzip", file_obj=self.main_file
        )

        if not self.ignore_checks:
            self.file_check(len(new_files))
        voter_reg_df = pd.DataFrame(columns=self.config["ordered_columns"])
        voter_hist_df = pd.DataFrame(columns=self.config["hist_columns"])
        for i in new_files:
            if "election" in i["name"].lower():
                voter_hist_df = pd.concat(
                    [
                        voter_hist_df,
                        self.read_csv_count_error_lines(
                            i["obj"], error_bad_lines=False
                        ),
                    ],
                    axis=0,
                )
            elif "voter" in i["name"].lower():
                voter_reg_df = pd.concat(
                    [
                        voter_reg_df,
                        self.read_csv_count_error_lines(
                            i["obj"], encoding="latin-1", error_bad_lines=False
                        ),
                    ],
                    axis=0,
                )
        voter_reg_df[self.config["voter_status"]] = np.nan
        voter_reg_df[self.config["party_identifier"]] = np.nan

        # if the dataframes are assigned columns to begin with, there will be nans due to concat if the columns are off
        self.column_check(list(voter_reg_df.columns))

        voter_reg_df["DOBYear"] = voter_reg_df["DOBYear"].astype(str).str[0:4]

        voter_hist_df["election_name"] = (
            voter_hist_df["ElectionDate"] + "_" + voter_hist_df["VotingMethod"]
        )
        valid_elections, counts = np.unique(
            voter_hist_df["election_name"], return_counts=True
        )
        date_order = [
            idx
            for idx, election in sorted(
                enumerate(valid_elections),
                key=lambda x: datetime.strptime(x[1][:-2], "%m/%d/%Y"),
                reverse=True,
            )
        ]
        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {
            k: {"index": i, "count": int(counts[i]), "date": date_from_str(k)}
            for i, k in enumerate(sorted_codes)
        }

        voter_hist_df["array_position"] = voter_hist_df["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"])
        )

        logging.info("Minnesota: history apply")
        voter_groups = voter_hist_df.groupby("VoterId")
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["VotingMethod"].apply(list)

        voter_reg_df = voter_reg_df.set_index(self.config["voter_id"])

        voter_reg_df["all_history"] = all_history
        voter_reg_df["vote_type"] = vote_type
        gc.collect()

        voter_reg_df = self.config.coerce_strings(voter_reg_df)
        voter_reg_df = self.config.coerce_dates(voter_reg_df)
        voter_reg_df = self.config.coerce_numeric(voter_reg_df)

        self.meta = {
            "message": "minnesota_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        gc.collect()
        logging.info("Minnesota: writing out")

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(
                voter_reg_df.to_csv(encoding="utf-8")
            ),
            s3_bucket=self.s3_bucket,
        )
