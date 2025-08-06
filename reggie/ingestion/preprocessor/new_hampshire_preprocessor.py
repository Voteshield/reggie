import datetime
import json
import logging

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)


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
        self.processed_file = None

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )
        if not self.ignore_checks:
            self.file_check(len(new_files))

        for f in new_files:
            # ignore ".mdb" files
            file_type_list = [".xlsx", ".csv", ".txt"]
            if any(x in f["name"] for x in file_type_list):
                if ("history" in f["name"].lower()) or ("vh" in f["name"].lower()):
                    logging.info("Found history file: {}".format(f["name"]))
                    if ".xlsx" in f["name"]:
                        hist_df = pd.read_excel(f["obj"])
                    else:
                        hist_df = self.read_csv_count_error_lines(
                            f["obj"], on_bad_lines="warn"
                        )
                    hist_df.drop_duplicates(inplace=True)

                elif (
                    ("checklist" in f["name"].lower())
                    or ("voters" in f["name"].lower())
                    or ("voter file" in f["name"].lower())
                ):
                    logging.info("Found voter file: {}".format(f["name"]))
                    if ".xlsx" in f["name"]:
                        voters_df = pd.read_excel(f["obj"])
                    else:
                        voters_df = self.read_csv_count_error_lines(
                            f["obj"],
                            on_bad_lines="warn",
                            encoding="latin-1",
                        )

        # April 2025 file has changed voter and history file headers.
        # So reset to original ones.
        voters_df.rename(
            columns=self.config["column_aliases_voter_file"],
            inplace=True,
        )
        hist_df.rename(
            columns=self.config["column_aliases_history_file"],
            inplace=True,
        )
        # Also April 2025 election date data has format
        # "3/12/2024 0:00:00" instead of "3/12/2024",
        # so strip that time off.
        hist_df["election_date"] = hist_df["election_date"].map(
            lambda x: x.split()[0]
        )

        # add dummy columns for birthday and voter_status
        voters_df[self.config["birthday_identifier"]] = 0
        voters_df[self.config["voter_status"]] = np.nan

        self.column_check(list(voters_df.columns))

        voters_df = self.config.coerce_strings(voters_df)
        voters_df = self.config.coerce_numeric(
            voters_df, extra_cols=["ad_str3", "mail_str3"]
        )

        # Also April 2025 data has some bad rows that distribute data
        # incorrectly and are missing at least County fields,
        # most likely other fields as well.
        # So, just drop these for now.
        valid_counties = list(self.config.primary_locale_names["county"].keys())
        orig_size = voters_df.shape[0]
        voters_df = voters_df[voters_df["County"].isin(valid_counties)]
        dropped = orig_size - voters_df.shape[0]
        logging.info(
            f"Dropped {dropped} rows without valid county names,"
            f"due to mangled data."
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

        # Check the file for all the proper locales
        self.locale_check(
            set(voters_df[self.config["primary_locale_identifier"]]),
        )

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
