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
from reggie.ingestion.utils import (
    format_column_name,
    MissingNumColumnsError,
)


class PreprocessMaine(Preprocessor):
    def __init__(self, raw_s3_file, config_file, force_date=None, **kwargs):

        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        super().__init__(
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            **kwargs,
        )
        self.raw_s3_file = raw_s3_file
        self.processed_file = None

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        def keep_most_recent_record(voters_df, cancelled_df):
            """
            A small number of voter ids will appear across tthe two files. We 
            should keep whichever one is most recently updated. The DT CHG 
            field contains the date the entry in each file was most recently 
            changed.
            :param voters_df: the voter file dataframe
            :param cancelled_df: the cancelled dataframe
            :return: The combined dataframe keeping the most recent entry across the two files.
            """

            voters_df_ids = voters_df[self.config["voter_id"]].to_list()
            cancelled_df_ids = cancelled_df[self.config["voter_id"]].to_list()
            intersetion_ids = set(voters_df_ids).intersection(
                set(cancelled_df_ids)
            )
            print(intersetion_ids)
            cancelled_df_intersection = cancelled_df[
                cancelled_df[self.config["voter_id"]].isin(intersetion_ids)
            ][[self.config["voter_id"], "DT CHG"]]
            voter_df_intersection = voters_df[
                voters_df[self.config["voter_id"]].isin(intersetion_ids)
            ][[self.config["voter_id"], "DT CHG"]]
            print(cancelled_df_intersection)
            print(voter_df_intersection)
            print(voter_df_intersection)
            # rename date cancelled to avoid the confusing _x _y merge
            cancelled_df_intersection.rename(
                columns={"DT CHG": "DT CHG CANCELLED"}, inplace=True
            )
            merged_df = pd.merge(
                cancelled_df_intersection,
                voter_df_intersection,
                on=self.config["voter_id"],
            )

            # Make datetimes to make the comparison make sense.
            merged_df["DT CHG CANCELLED"] = pd.to_datetime(
                merged_df["DT CHG CANCELLED"]
            )
            merged_df["DT CHG"] = pd.to_datetime(
                merged_df["DT CHG"]
            )
            
            # Comparisons against missing data (like NaT are always false)
            # This means that when there are NaT values, they are not dropped
            # This should keep both entries in the file
            merged_df["Cancelled_Recent"] = (
                merged_df["DT CHG CANCELLED"] > merged_df["DT CHG"]
            )
            # Grab the ids to drop from the cancelled file
            ids_to_drop = merged_df[merged_df["Cancelled_Recent"] == True][
                self.config["voter_id"]
            ]
            logging.info(
                f"Dropped {len(ids_to_drop)} records from the active file with a more recent cancellation entry"
            )
            voters_df = voters_df[
                ~voters_df[self.config["voter_id"]].isin(list(ids_to_drop))
            ]
            return pd.concat([cancelled_df, voters_df])

        new_files = self.unpack_files(self.main_file, compression="unzip")
        # Due to the way the history files are done for this state, there are a
        # variable number of correct files, so checking the number of files is
        # not possible.
        # self.file_check(len(new_files))
        voter_df = pd.DataFrame()
        cancelled_df = pd.DataFrame()
        hist_df = pd.DataFrame()
        for file in new_files:
            if "voter.txt" in file["name"].lower():  # needsextension
                logging.info("voter file found")
                voter_df = self.read_csv_count_error_lines(
                    file["obj"], sep="|", dtype="str", on_bad_lines="warn"
                )
                voter_df_shape_before = voter_df.shape
                voter_df.dropna(subset=["VOTER ID"], inplace=True)
                voter_df_shape_after = voter_df.shape
                logging.info(
                    f"Dropped {voter_df_shape_before[0] - voter_df_shape_after[0]} rows due to NaN ID values"
                )
                voter_df_shape_before = voter_df.shape
                voter_df.dropna(subset=["CTY"], inplace=True)
                voter_df_shape_after = voter_df.shape
                logging.info(
                    f"Dropped {voter_df_shape_before[0] - voter_df_shape_after[0]} rows due to NaN county values"
                )
            elif "history" in file["name"].lower():
                # Maine Voter History seems to come one file per election,
                logging.info("vote history found")
                new_hist = self.read_csv_count_error_lines(
                    file["obj"], sep="|", dtype="str", on_bad_lines="warn"
                )
                logging.info(f"concatenating {file['name']}")
                hist_df = pd.concat([hist_df, new_hist])
            elif "cancelled" in file["name"].lower():
                logging.info("cancelled file found")
                # Note: the cancelled file does not have a county column
                cancelled_df = self.read_csv_count_error_lines(
                    file["obj"], sep="|", dtype="str", on_bad_lines="warn"
                )
                cancelled_df.rename(
                    columns=self.config["cancelled_columns"], inplace=True
                )
        if not cancelled_df.empty:
            # For Some reason there are no counties in the cancelled df file
            # Derive them from zip codes found in main file?
            zip_dict = dict(zip(voter_df["ZIP"], voter_df["CTY"]))
            cancelled_df["CTY"] = cancelled_df["ZIP"].map(zip_dict)

            # also for some reason they give your full birthday in the cancelld file?
            cancelled_df[["MONTH", "DAY", "YOB"]] = cancelled_df[
                "YOB"
            ].str.split("/", expand=True)
            cancelled_df.drop(columns=["MONTH", "DAY"], inplace=True)

        # there are several entries in the cancelled file, that have an active
        # status in the main file
        voter_df = keep_most_recent_record(
            voters_df=voter_df, cancelled_df=cancelled_df
        )
        del cancelled_df
        unnamed_cols = voter_df.columns[
            voter_df.columns.str.contains("Unnamed")
        ]
        voter_df.drop(columns=unnamed_cols, inplace=True)
        self.column_check(voter_df.columns)
        if hist_df.empty:
            raise ValueError("must supply a file containing voter history")

        # Drop NA dates, because there are very few or so entries that don't have them
        hist_df.dropna(subset=["ELECTION DATE"], inplace=True)
        hist_df["ELECTION NAME"] = (
            hist_df["ELECTION NAME"].str.replace(" ", "_").str.lower()
        )
        hist_df["combined_name"] = (
            hist_df["ELECTION NAME"] + "_" + hist_df["ELECTION DATE"]
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

        voter_df = voter_df.set_index(self.config["voter_id"], drop=False)
        voter_id_groups = hist_df.groupby(self.config["voter_id"])
        del hist_df

        voter_df["all_history"] = voter_id_groups["combined_name"].apply(list)
        voter_df["sparse_history"] = voter_df["all_history"].map(
            insert_code_bin
        )
        voter_df["votetype_history"] = voter_id_groups["BALLOT TYPE"].apply(
            list
        )
        voter_df["election_type_history"] = voter_id_groups[
            "ELECTION TYPE"
        ].apply(list)

        del voter_id_groups
        logging.info("Coercing Strings")
        voter_df = self.config.coerce_strings(voter_df)

        logging.info("Coercing Numbers")
        voter_df = self.config.coerce_numeric(voter_df)
        logging.info("Coercing Dates")
        voter_df = self.config.coerce_dates(voter_df)

        # Check the file for all the proper locales
        self.locale_check(
            set(voter_df[self.config["primary_locale_identifier"]]),
        )
        self.meta = {
            "message": "maine_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        logging.info("Processed Maine")

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(voter_df.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
