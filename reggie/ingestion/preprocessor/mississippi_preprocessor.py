import datetime
import gc
import json
import logging

from datetime import datetime
from io import StringIO

import numpy as np

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)


def drop_unnamed_columns(df):
    """Drop the empty trailing header column, which pandas reads as "Unnamed: N"."""
    return df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed:")]


class PreprocessMississippi(Preprocessor):
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
        logging.info("downloading and unpacking the raw file")
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )
        logging.info("unpacked {} files".format(len(new_files)))

        if not self.ignore_checks:
            self.file_check(len(new_files))

        # the history file always contains "VotingHistory" or "Voting History"
        # the other file is assumed to be the voter file. 
        hist_file = [
            n
            for n in new_files
            if "votinghistory" in n["name"].lower().replace(" ", "")
        ][0]
        voter_file = [n for n in new_files if n is not hist_file][0]
        logging.info(
            "voter file {}, history file {}".format(
                voter_file["name"], hist_file["name"]
            )
        )
        del new_files

        voter_df = self.read_csv_count_error_lines(
            voter_file["obj"], on_bad_lines="warn", dtype=str
        )
        voter_df = drop_unnamed_columns(voter_df)
        hist_df = self.read_csv_count_error_lines(
            hist_file["obj"], on_bad_lines="warn", dtype=str
        )
        hist_df = drop_unnamed_columns(hist_df)
        del voter_file, hist_file
        gc.collect()
        logging.info(
            "Read {} voter rows and {} history rows".format(
                voter_df.shape[0], hist_df.shape[0]
            )
        )

        logging.info(
            "preparing the voter file"
        )
        # No birth date or party in the voter file, add dummy columns
        voter_df[self.config["birthday_identifier"]] = np.nan
        voter_df[self.config["party_identifier"]] = np.nan

        if "IPD" not in voter_df.columns:
            voter_df["IPD"] = np.nan
        self.column_check(list(voter_df.columns))
        logging.info("voter column check passed")

        voter_df = self.config.coerce_strings(voter_df)
        voter_df = self.config.coerce_dates(voter_df)
        logging.info("coerced voter columns")
        logging.info(
            "status values: {}".format(
                voter_df[self.config["voter_status"]]
                .value_counts(dropna=False)
                .to_dict()
            )
        )

        logging.info(
            "preparing the history file"
        )
        # Rows without an election are dropped before coercion, which would
        # turn missing values into the string "nan"
        hist_df = hist_df.dropna(subset=["ELECTION NAME", "ELECTION DATE"])
        # A missing party is recorded as no party, a la az2
        hist_df["POLITICAL PARTY"] = hist_df["POLITICAL PARTY"].fillna(
            self.config["no_party_affiliation"]
        )
        raw_election_dates = hist_df["ELECTION DATE"].copy()
        hist_df = self.config.coerce_strings(
            hist_df, col_list="hist_columns_types"
        )
        hist_df = self.config.coerce_dates(
            hist_df, col_list="hist_columns_types"
        )
        # Coercion turns dates it cannot read, or that are out of range, into
        # nulls. Those rows are malformed, and would make a null election id
        unreadable_dates = hist_df["ELECTION DATE"].isnull()
        if unreadable_dates.any():
            logging.warning(
                "dropping {} history rows with an election date that could "
                "not be read: {}".format(
                    unreadable_dates.sum(),
                    raw_election_dates[unreadable_dates]
                    .value_counts()
                    .head(20)
                    .to_dict(),
                )
            )
            hist_df = hist_df[~unreadable_dates]
        hist_df = hist_df.sort_values("ELECTION DATE")
        logging.info(
            "coerced history columns, {} history rows".format(
                hist_df.shape[0]
            )
        )

        logging.info(
            "building voter history"
        )
        hist_df["combined_name"] = hist_df["ELECTION NAME"].str.replace(
            " ", "_"
        ) + hist_df["ELECTION DATE"].dt.strftime("_%m/%d/%Y")

        voter_df = voter_df.set_index(self.config["voter_id"], drop=False)
        voter_groups = hist_df.groupby(self.config["voter_id"])
        voter_df["all_history"] = voter_groups["combined_name"].apply(list)
        voter_df["party_history"] = voter_groups["POLITICAL PARTY"].apply(list)
        del voter_groups
        gc.collect()
        logging.info(
            "built history for {} voters".format(
                voter_df["all_history"].notnull().sum()
            )
        )

        sorted_codes = hist_df["combined_name"].unique().tolist()
        counts = hist_df["combined_name"].value_counts()
        del hist_df
        gc.collect()

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

        voter_df["sparse_history"] = voter_df["all_history"].map(
            insert_code_bin
        )

        logging.info(
            "ordering columns and checking locales"
        )
        expected_cols = (
            self.config["ordered_columns"]
            + self.config["ordered_generated_columns"]
        )
        voter_df = self.reconcile_columns(voter_df, expected_cols)
        voter_df = voter_df[expected_cols]
        # The voter id was also the index, which made its label ambiguous
        voter_df = voter_df.reset_index(drop=True)

        # Check the file for all the proper locales
        self.locale_check(
            set(voter_df[self.config["primary_locale_identifier"]]),
        )
        logging.info("locale check done")

        logging.info(
            "building metadata and the processed file"
        )
        self.meta = {
            "message": "mississippi_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        csv_obj = voter_df.to_csv(encoding="utf-8", index=False)
        del voter_df
        gc.collect()

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_obj),
            s3_bucket=self.s3_bucket,
        )
        del csv_obj
        gc.collect()
