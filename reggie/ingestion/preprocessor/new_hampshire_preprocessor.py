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


class PreprocessNewHampshire(Preprocessor):
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

        config = config_file
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )
        if not self.ignore_checks:
            self.file_check(len(new_files))

        for f in new_files:
            # ignore ".mdb" files
            if (".xlsx" in f["name"]) or (".csv" in f["name"]):

                if "history" in f["name"].lower():
                    logging.info("Found history file: {}".format(f["name"]))
                    if ".xlsx" in f["name"]:
                        hist_df = pd.read_excel(f["obj"])
                    else:
                        hist_df = self.read_csv_count_error_lines(
                            f["obj"], error_bad_lines=False
                        )
                    hist_df.drop_duplicates(inplace=True)

                elif ("checklist" in f["name"].lower()) or (
                    "voters" in f["name"].lower()
                ):
                    logging.info("Found voter file: {}".format(f["name"]))
                    if ".xlsx" in f["name"]:
                        voters_df = pd.read_excel(f["obj"])
                    else:
                        voters_df = self.read_csv_count_error_lines(
                            f["obj"], error_bad_lines=False
                        )

        # add dummy columns for birthday and voter_status
        voters_df[self.config["birthday_identifier"]] = 0
        voters_df[self.config["voter_status"]] = np.nan

        self.column_check(list(voters_df.columns))
        voters_df = self.config.coerce_strings(voters_df)
        voters_df = self.config.coerce_numeric(
            voters_df, extra_cols=["ad_str3", "mail_str3"]
        )

        # collect histories
        hist_df["combined_name"] = (
            hist_df["election_name"].str.replace(" ", "_").str.lower()
            + "_"
            + hist_df["election_date"]
        )

        sorted_codes = hist_df["combined_name"].unique().tolist()
        sorted_codes.sort(
            key=lambda x: datetime.strptime(x.split("_")[-1], "%m/%d/%Y")
        )
        counts = hist_df["combined_name"].value_counts()
        sorted_codes_dict = {
            k: {
                "index": i,
                "count": int(counts.loc[k]),
                "date": k.split("_")[-1],
            }
            for i, k in enumerate(sorted_codes)
        }

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]["index"] for k in arr]
            else:
                return np.nan

        voters_df = voters_df.set_index("id_voter", drop=False)
        voter_id_groups = hist_df.groupby("id_voter")
        voters_df["all_history"] = voter_id_groups["combined_name"].apply(list)
        voters_df["sparse_history"] = voters_df["all_history"].map(
            insert_code_bin
        )
        voters_df["election_type_history"] = voter_id_groups[
            "election_type"
        ].apply(list)
        voters_df["election_category_history"] = voter_id_groups[
            "election_category"
        ].apply(list)
        voters_df["votetype_history"] = voter_id_groups["ballot_type"].apply(
            list
        )
        voters_df["party_history"] = voter_id_groups["cd_part_voted"].apply(
            list
        )
        voters_df["town_history"] = voter_id_groups["town"].apply(list)

        self.meta = {
            "message": "new_hampshire_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(voters_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
