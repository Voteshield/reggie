import datetime
import gc
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

HIST_REQUIRED_COLUMNS = {
    "IDENTIFICATION_NUMBER",
    "ELECTION_NAME",
    "ELECTION_DATE",
    "ELECTION_TYPE",
    "PRIMARY_TYPE_CODE_NAME",
}
VOTETYPE_FLAG_COLUMNS = ["VOTE_IN_PERSON", "PROTECTED", "ABSENTEE", "PROVISIONAL"]


def read_header(file_obj):
    """Return the set of column names on the first line, rewinding the file."""
    line = file_obj.readline()
    file_obj.seek(0)
    if isinstance(line, bytes):
        line = line.decode("ISO-8859-1")
    return {c.strip().strip('"') for c in line.strip().split(",")}


class PreprocessVirginia(Preprocessor):
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

        # History arrives as a cumulative "Vote_History" file and, since
        # 2026-10-03, also as one file per election whose name does not
        # contain "history", so history files are identified by header
        voters_df = None
        hist_dfs = []
        for f in new_files:
            if "registered" in f["name"].lower():
                logging.info("voter file found")
                voters_df = self.read_csv_count_error_lines(
                    f["obj"], on_bad_lines="warn", encoding="ISO-8859-1"
                )
            elif HIST_REQUIRED_COLUMNS.issubset(read_header(f["obj"])):
                logging.info("vote history found: {}".format(f["name"]))
                hist_dfs.append(
                    self.read_csv_count_error_lines(
                        f["obj"],
                        on_bad_lines="warn",
                        encoding="ISO-8859-1",
                        usecols=lambda c: c in self.config["hist_columns"],
                    )
                )
            else:
                logging.warning(
                    "skipping {}: neither voter file nor vote history".format(
                        f["name"]
                    )
                )

        # throw exception if missing one of the two files needed for processing
        if voters_df is None or not hist_dfs:
            raise ValueError("must supply both history and voter file")
        hist_df = pd.concat(hist_dfs, ignore_index=True)
        del hist_dfs

        voters_df[self.config["party_identifier"]] = np.nan
        voters_df = self.reconcile_columns(voters_df, self.config["columns"])
        self.column_check(list(voters_df.columns))
        voters_df = self.config.coerce_strings(voters_df)
        voters_df = self.config.coerce_numeric(
            voters_df,
            extra_cols=[
                "TOWNPREC_CODE_VALUE",
                "SUPERDIST_CODE_VALUE",
                "HOUSE_NUMBER",
                "MAILING_ZIP",
            ],
        )
        voters_df = self.config.coerce_dates(voters_df)

        hist_df["combined_name"] = (
            hist_df["ELECTION_NAME"].str.replace(" ", "_").str.lower()
            + "_"
            + hist_df["ELECTION_DATE"]
        )

        # 2024-06-04 file removed VOTE_IN_PERSON and PROTECTED from history file.
        # The per-election files (first received 2026-10-03) have no ABSENTEE or
        # PROVISIONAL either, so their rows get NaN votetype_history
        for col in VOTETYPE_FLAG_COLUMNS:
            if col not in hist_df.columns:
                hist_df[col] = False
            else:
                hist_df[col] = hist_df[col].astype("boolean").fillna(False).astype(bool)

        # Gathers the votetype columns that are initially boolean and replaces them with the word version of their name
        # collect all the columns where the value is True, combine to one votetype history separated by underscores
        # parsing in features will pull out the appropriate string
        hist_df["votetype_history"] = np.where(
            hist_df["VOTE_IN_PERSON"], "inPerson_", ""
        )
        hist_df["votetype_history"] += np.where(
            hist_df["PROTECTED"], "protected_", ""
        )
        hist_df["votetype_history"] += np.where(
            hist_df["ABSENTEE"], "absentee_", ""
        )
        hist_df["votetype_history"] += np.where(
            hist_df["PROVISIONAL"], "provisional_", ""
        )
        # replace the empty strings with nan for cleaner db cell values
        hist_df["votetype_history"] = hist_df["votetype_history"].replace("", np.nan)

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

        voters_df = voters_df.set_index("IDENTIFICATION_NUMBER", drop=False)
        voter_id_groups = hist_df.groupby("IDENTIFICATION_NUMBER")
        voters_df["all_history"] = voter_id_groups["combined_name"].apply(list)
        voters_df["sparse_history"] = voters_df["all_history"].map(
            insert_code_bin
        )
        voters_df["election_type_history"] = voter_id_groups[
            "ELECTION_TYPE"
        ].apply(list)
        voters_df["party_history"] = voter_id_groups[
            "PRIMARY_TYPE_CODE_NAME"
        ].apply(list)
        voters_df["votetype_history"] = voter_id_groups[
            "votetype_history"
        ].apply(list)
        gc.collect()

        # Check the file for all the proper locales
        self.locale_check(
            set(voters_df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "virginia_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(voters_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
