import chardet
import datetime
import gc
import json
import logging

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    FileItem,
    Preprocessor,
    date_from_str,
)
from reggie.ingestion.utils import (
    MissingNumColumnsError,
    date_from_str,
)


class PreprocessColorado(Preprocessor):
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

        new_files = self.unpack_files(compression="unzip", file_obj=self.main_file)

        # Since Colorado gives us multiple sets of the
        # "Registered_Voters_List" files, we first need to remove
        # all of these except the most recent set.
        # We can do this heuristically by removing sets with "Public"
        # in their name, and sets with previous years in their file path.
        # If there are still multiple sets left, we take the one
        # that is highest in the path hierarchy.
        reg_voter_files = [
            f for f in new_files if "Registered_Voters_List" in f["name"]
        ]
        other_files = [
            f for f in new_files if "Registered_Voters_List" not in f["name"]
        ]

        def remove_files_with_previous_years_in_path(file_list):
            current_year = int(date_from_str(self.raw_s3_file).split("-")[0])
            exclude_yrs = [str(current_year - i) for i in range(1, 6)]
            for y in exclude_yrs:
                file_list = [f for f in file_list if y not in f["name"]]
            return file_list

        reg_voter_files = [f for f in reg_voter_files if "Public" not in f["name"]]
        reg_voter_files = remove_files_with_previous_years_in_path(reg_voter_files)
        levels_down = [f["name"].count("/") for f in reg_voter_files]
        reg_voter_files = [
            f for f in reg_voter_files if f["name"].count("/") == min(levels_down)
        ]

        new_files = reg_voter_files + other_files

        df_voter = pd.DataFrame(columns=self.config.raw_file_columns())
        df_hist = pd.DataFrame(columns=self.config["hist_columns"])
        df_master_voter = pd.DataFrame(columns=self.config["master_voter_columns"])
        master_vf_version = True

        def master_to_reg_df(df):
            df.columns = self.config["master_voter_columns"]
            df["STATUS"] = df["VOTER_STATUS"]
            df["PRECINCT"] = df["PRECINCT_CODE"]
            df["VOTER_NAME"] = (
                df["LAST_NAME"] + ", " + df["FIRST_NAME"] + " " + df["MIDDLE_NAME"]
            )
            df = pd.concat([df, pd.DataFrame(columns=self.config["blacklist_columns"])])
            df = df[self.config.processed_file_columns()]
            return df

        for i in new_files:
            if "Registered_Voters_List" in i["name"]:
                master_vf_version = False

        # Starting in May 2023, Colorado has consolidated to a single
        # voter file with the name "Master_Voter_List" but it does
        # NOT have the same columns associated with the older
        # master voter files, so we need to differentiate this case.
        current_file_date = datetime.strptime(
            date_from_str(self.raw_s3_file), "%Y-%m-%d"
        )
        if current_file_date > datetime(2023, 5, 1):
            master_vf_version = False

        for i in new_files:
            # The word Public used to indicate a file that had duplicate (older) voter information/files in it that
            # that could not be differentiated by name from the actual voter files, so it led to duplicates. The
            # autodownloader automatically filters out the "archive" subdirectory that contained the duplicate voter
            # information in the history directory, so this is less of an issue. In addition, after March in 2024, they
            # started adding the word public to actual history files e.g.
            # "024_Presidential_Primary_EX-002_Congressional_District_8_Public_Voting_History_List_Part5.zip" so we
            # can no longer filter on the word public...but should not need to.
            if "Public" not in i["name"] or current_file_date > datetime(2024, 2, 1):
                if (
                    "Registered_Voters_List" in i["name"] and not master_vf_version
                ) or (
                    "EX-003_Master_Voter_List" in i["name"] and not master_vf_version
                ):
                    # Colorado sometimes includes us a split district file in the same folder as the master voting
                    # list file, so the key looks like "EX-003_Master_Voter_List/EX-001_Split_District_Report_###.csv"
                    # this gets read in here as a voter file, and adds a few extra columns to the dataframe.
                    # Going forward with the automated code, this file won't be included so the columns are excluded
                    # from the diff in the no_diff_columns part of the yaml
                    logging.info("reading in {}".format(i["name"]))
                    # Colorado has a couple different encodings they send us, the format that is detected as ascii will
                    # error out if not read in as latin-1
                    # The format that is typically detected as utf-8-sig needs to have the index col explicitly set to
                    # false, or else pandas will attempt to read the voterid column
                    # in as the index and the history won't apply
                    encoding_result = chardet.detect(i["obj"].read(10000))
                    if encoding_result["encoding"] == "ascii":
                        encoding = "latin-1"
                        index_col = None
                    else:
                        encoding = encoding_result["encoding"]
                        index_col = False
                    i["obj"].seek(0)
                    df_voter = pd.concat(
                        [
                            df_voter,
                            self.read_csv_count_error_lines(
                                i["obj"],
                                encoding=encoding,
                                on_bad_lines="warn",
                                index_col=index_col,
                            ),
                        ],
                        axis=0,
                    )

                elif ("Voting_History" in i["name"]) or (
                    "Coordinated_Voter_Details" in i["name"]
                ):
                    if i["name"].split(".")[-1].lower() == "txt":
                        compression = None
                    elif i["name"].split(".")[-1].lower() == "zip":
                        compression = "zip"
                    else:
                        compression = "gzip"

                    if "Voter_Details" not in i["name"]:
                        logging.info("reading in {}".format(i["name"]))

                        new_df = self.read_csv_count_error_lines(
                            i["obj"],
                            compression=compression,
                            on_bad_lines="warn",
                        )
                        df_hist = pd.concat([df_hist, new_df], axis=0)

                    if "Voter_Details" in i["name"] and master_vf_version:
                        logging.info("reading in {}".format(i["name"]))
                        new_df = self.read_csv_count_error_lines(
                            i["obj"],
                            compression=compression,
                            on_bad_lines="warn",
                        )
                        if len(new_df.columns) < len(
                            self.config["master_voter_columns"]
                        ):
                            new_df.insert(10, "PHONE_NUM", np.nan)
                        try:
                            new_df.columns = self.config["master_voter_columns"]
                        except ValueError:
                            logging.info(
                                "Incorrect number of columns found for Colorado for file: {}".format(
                                    i["name"]
                                )
                            )
                            raise MissingNumColumnsError(
                                "{} state is missing columns".format(self.state),
                                self.state,
                                len(self.config["master_voter_columns"]),
                                len(new_df.columns),
                            )
                        df_master_voter = pd.concat([df_master_voter, new_df], axis=0)

        if df_voter.empty:
            df_voter = master_to_reg_df(df_master_voter)
        if df_hist.empty:
            raise ValueError("must supply a file containing voter history")
        df_hist["VOTING_METHOD"] = df_hist["VOTING_METHOD"].replace(np.nan, "")
        df_hist["ELECTION_DATE"] = pd.to_datetime(
            df_hist["ELECTION_DATE"], format="%m/%d/%Y", errors="coerce"
        )
        df_hist.dropna(subset=["ELECTION_DATE"], inplace=True)
        df_hist["election_name"] = (
            df_hist["ELECTION_DATE"].astype(str) + "_" + df_hist["VOTING_METHOD"]
        )

        valid_elections, counts = np.unique(
            df_hist["election_name"], return_counts=True
        )

        date_order = [
            idx
            for idx, election in sorted(
                enumerate(valid_elections),
                key=lambda x: datetime.strptime(x[1][0:10], "%Y-%m-%d"),
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

        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"])
        )

        logging.info("Colorado: history apply")
        voter_groups = df_hist.groupby(self.config["voter_id"])
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["VOTING_METHOD"].apply(list)

        df_voter.dropna(subset=[self.config["voter_id"]], inplace=True)
        df_voter = df_voter.set_index(self.config["voter_id"])
        df_voter.sort_index(inplace=True)

        df_voter["all_history"] = all_history
        df_voter["vote_type"] = vote_type
        gc.collect()

        # at some point mailing address field names changed
        for num in ["1", "2", "3"]:
            if f"MAIL_ADDR{num}" in df_voter.columns:
                # if both are present, combine them
                if f"MAILING_ADDRESS_{num}" in df_voter.columns:
                    df_voter[f"MAILING_ADDRESS_{num}"] = np.where(
                        df_voter[f"MAILING_ADDRESS_{num}"].isnull(),
                        df_voter[f"MAIL_ADDR{num}"],
                        df_voter[f"MAILING_ADDRESS_{num}"],
                    )
                else:
                    df_voter[f"MAILING_ADDRESS_{num}"] = df_voter[f"MAIL_ADDR{num}"]
                df_voter.drop(columns=[f"MAIL_ADDR{num}"], inplace=True)

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(
            df_voter,
            extra_cols=[
                "HOUSE_NUM",
                "UNIT_NUM",
                "RESIDENTIAL_ZIP_CODE",
                "RESIDENTIAL_ZIP_PLUS",
                "MAILING_ZIP_CODE",
                "MAILING_ZIP_PLUS",
                "PRECINCT_NAME",
                "PRECINCT",
                "MAILING_ADDRESS_3",
                "PHONE_NUM",
            ],
        )

        self.meta = {
            "message": "Colorado_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        gc.collect()

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voter[self.config["primary_locale_identifier"]]),
        )

        logging.info("Colorado: writing out")
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8")),
            s3_bucket=self.s3_bucket,
        )
