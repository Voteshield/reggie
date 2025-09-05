"""
West Virginia preprocessor.
"""

import chardet
import csv
import datetime
import gc
import json
import logging
import re

from datetime import datetime
from dateutil import parser
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
from math import isnan

import numpy as np
import pandas as pd

from slugify import slugify

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import (
    InvalidDataError,
    MissingNumColumnsError,
    UnexpectedNumberOfFilesError,
    format_column_name,
)


# Constants
VOTER_FILE_REGEX = re.compile(
    "^[^.]*(statewide.*vr|wv [0-9]+-[0-9]+-[0-9]+.txt|protect.*democracy[0-9]+|voter_data_[0-9]+.txt)",
    flags=re.I,
)
VOTER_HISTORY_REGEX = re.compile("^[^.]*(statewide.*vh|vote history*|.*voter_history.*)", flags=re.I)


class PreprocessWestVirginia(Preprocessor):
    def __init__(self, raw_s3_file, config_file, force_date=None, **kwargs):
        # Attempt to get date from file if not specifically given
        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        # Call the main Preprocessor init
        super().__init__(
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            **kwargs,
        )

        # Initialize some properties
        self.raw_s3_file = raw_s3_file
        self.processed_file = None

    def execute(self):
        """
        Main preprocessor function.  Looks through files and converts voter files
        and history files.  Should set self.main_file with combined dataframe.
        """
        # Shortcut to config
        config = self.config.data

        # Download if not file not provided
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        # Get the files from the archive
        new_files = [n for n in self.unpack_files(self.main_file, compression="unzip")]

        # Should only be one voter file
        voter_files = [n for n in new_files if VOTER_FILE_REGEX.match(n["name"])]
        if len(voter_files) > 1:
            raise UnexpectedNumberOfFilesError(
                f"{config['state']} has too many voter files."
            )
        if len(voter_files) < 1:
            raise UnexpectedNumberOfFilesError(
                f"Unable to find voter file in {config['state']} import."
            )
        voter_file = voter_files[0]

        # April 2025 file was unexpectedly encoded as utf-8-sig
        # so we need to detect and correctly read in that case.
        encoding_result = chardet.detect(voter_file["obj"].read(1000))
        voter_file["obj"].seek(0)
        if encoding_result["encoding"] == "UTF-8-SIG":
            voter_file_encoding = encoding_result["encoding"]
        else:
            voter_file_encoding = "latin-1"

        # May 2025 also has unmatched quotation marks, so we
        # need to start ignoring quotation marks:
        if (self.raw_s3_file is not None) and (
            date_from_str(self.raw_s3_file) > "2025-04-01"
        ):
            quoting = csv.QUOTE_NONE
        else:
            # Leave default for older data, in case it matters
            quoting = csv.QUOTE_MINIMAL

        # Read voter file into pandas dataframe
        logging.info(f'[{config["state"]}] Loading voter file.')
        df_voters = pd.read_csv(
            voter_file["obj"],
            sep=config["delimiter"],
            encoding=voter_file_encoding,
            dtype=str,
            header=0 if config["has_headers"] else None,
            quoting=quoting,
        )

        df_voters.rename(
            columns={
                "COUNTY_NAME": "County_Name",
                "MID": "Mid",
                "STATUS": "Status",
                "SUFFIX": "Suffix",
                "PARTYAFFILIATION": "PartyAffiliation",
                "CONGRESSIONAL DISTRICT": "Congressional District",
                "CONGRESSIONAL": "Congressional District",
                "MAGISTERIAL DISTRICT": "Magisterial District",
                "PRECINCT_NUMBER": "Precinct_Number",
                # New field names as of 2024-01-08
                "VOTER_ID": "ID_VOTER",
                "FIRST_NAME": "FIRST NAME",
                "MIDDLE_NAME": "Mid",
                "LAST_NAME": "LAST NAME",
                "DATE_OF_BIRTH": "DATE OF BIRTH",
                "HOUSE_NO": "HOUSE NO",
                "MAIL_HOUSE_NO": "MAIL HOUSE NO",
                "MAIL_STREET": "MAIL STREET",
                "MAIL_STREET2": "MAIL STREET2",
                "MAIL_UNIT": "MAIL UNIT",
                "MAIL_CITY": "MAIL CITY",
                "MAIL_STATE": "MAIL STATE",
                "MAIL_ZIP": "MAIL ZIP",
                "REGISTRATION_DATE": "REGISTRATION DATE",
                "PARTY_AFFILIATION": "PartyAffiliation",
                "MAGISTERIAL_DISTRICT": "Magisterial District",
                "COUNTY_PRECINCT": "Precinct_Number",
                # Some more changes as of 2025-09-04
                "POLL_PLACE_NAME": "POLL_NAME"
            },
            inplace=True,
            errors="ignore"
        )

        # adds dummy column to wv to match format of older files
        if "mail unit" not in df_voters.columns.str.lower():
            df_voters["MAIL UNIT"] = np.nan

        # 2025-09-04 adding dummy suffix column here as well
        if "suffix" not in df_voters.columns.str.lower():
            df_voters["Suffix"] = np.nan

        # Apparently during redistricting the relevant fields will be dropped entirely
        # and then re-added to the file after updates.
        if "Senatorial District" not in df_voters.columns:
            if "GIS Senatorial District" in df_voters.columns:
                df_voters.rename(
                    columns={
                        "GIS Senatorial District": "Senatorial District"
                    },
                    inplace=True
                )
            else:
                df_voters["Senatorial District"] = np.nan
        if "Delegate District" not in df_voters.columns:
            if "GIS Delegate District" in df_voters.columns:
                df_voters.rename(
                    columns={
                        "GIS Delegate District": "Delegate District"
                    },
                    inplace=True
                )
            else:
                df_voters["Delegate District"] = np.nan

        # Add county id column
        df_voters.insert(
            1,
            "County_ID",
            df_voters.County_Name.map(slugify),
        )

        # Make sure counties are valid as defined in config
        # counties = df_voters.County_ID.unique()
        self.check_column_has_valid_values(
            df_voters,
            "County_ID",
            self.config_lookup_to_valid_list(config["locales_counties"]),
        )

        # Gender rename and clean.  There are some historical files that
        # dont have this column so make blank.
        if ("SEX" in df_voters.columns) or ("GENDER" in df_voters.columns):
            df_voters.rename(
                columns={"SEX": "gender", "GENDER": "gender"},
                inplace=True,
                errors="ignore",
            )
            gender_dict = self.config_lookup_to_dict(self.config["gender_codes"])
            df_voters.loc[:, "gender"] = df_voters.loc[:, "gender"].map(gender_dict)
            df_voters["gender"] = df_voters["gender"].fillna("unknown")
        else:
            birth_index = df_voters.columns.get_loc("DATE OF BIRTH")
            df_voters.insert(birth_index + 1, "gender", "unknown")

        # Gender check
        self.check_column_has_valid_values(
            df_voters,
            "gender",
            self.config_lookup_to_valid_list(config["gender_codes"]),
        )

        # Party clean
        df_voters.PartyAffiliation = df_voters.PartyAffiliation.apply(
            self.clean_party_value
        )

        # Status clean up
        status_dict = self.config_lookup_to_dict(self.config["status_codes"])
        df_voters.loc[:, "Status"] = df_voters.loc[:, "Status"].map(status_dict)

        # Clean up districts by removing leading zeroes, but keep as strings
        for field in [
            "Congressional District",
            "Senatorial District",
            "Delegate District",
            "Magisterial District",
            "Precinct_Number",
        ]:
            df_voters[field] = df_voters[field].apply(
                lambda x: x.lstrip("0") if isinstance(x, str) else x
            )

        # Coerce values for cleanup
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_strings(
            df_voters, exclude=[self.config["voter_id"], "County_Name"]
        )

        # Set index
        logging.info(f'[{config["state"]}] Setting index on voter file.')
        df_voters = df_voters.set_index(config["voter_id"], drop=False)

        # Check columns with what is expected
        self.column_check(df_voters.columns)

        # Make sure any unexpected columns are dropped
        df_voters = self.reconcile_columns(df_voters, self.config["ordered_columns"])

        # Get voter history file
        voter_histories = [n for n in new_files if VOTER_HISTORY_REGEX.match(n["name"])]
        if len(voter_histories) > 1:
            raise UnexpectedNumberOfFilesError(
                f"[{config['state']}] Too many voter history files."
            )
        if len(voter_histories) < 1:
            logging.info(f"[{config['state']}] Unable to find a history file.")

            # Create empty rows
            df_voters["all_history"] = None
            df_voters["votetype_history"] = None
            df_voters["challenged_history"] = None
            df_voters["sparse_history"] = None

            # Create meta data
            self.meta = {
                "message": "{}_{}".format(config["state"], datetime.now().isoformat())
            }
        else:
            voter_history = voter_histories[0]

            # Read voter file into pandas dataframe
            logging.info(f'[{config["state"]}] Loading history file.')
            df_history = pd.read_csv(
                voter_history["obj"],
                sep=config["delimiter"],
                encoding="latin-1",
                dtype=str,
                header=0 if config["has_headers"] else None,
            )

            # Starting transformations
            logging.info(f'[{config["state"]}] Starting history transformations.')

            # As of Sept 2022, the history file changed its format
            if "CD_ENTRY_TYPE" in df_history.columns:
                history_2022_format = True
                df_history.rename(
                    columns={
                        "ID_VOTER": "id_voter",
                        "ID_ELECTION": "id_election",
                        "DT_ELECTION": "dt_election",
                        "NM_ELECTION": "Election_Name",
                        "CD_ELECTION_TYPE": "cd_election_type",
                        "CD_ELECTION_CAT": "cd_election_cat",
                    },
                    inplace=True,
                    errors="ignore"
                )
            else:
                history_2022_format = False

            # Create new column that aggregrates the type of vote
            # NOTE: This assumes that "Y" is always used for yes
            df_history["votetype"] = "unknown"
            if history_2022_format:
                df_history.votetype[df_history.CD_ENTRY_TYPE == "A"] = "absentee"
                df_history.votetype[df_history.CD_ENTRY_TYPE == "E"] = "early"
                df_history.votetype[df_history.CD_ENTRY_TYPE == "R"] = "regular"
            else:
                df_history.votetype[df_history.fl_absentee == "Y"] = "absentee"
                df_history.votetype[df_history.fl_early_voting == "Y"] = "early"
                df_history.votetype[df_history.fl_regular == "Y"] = "regular"

            # Clean the challenged flag, which looks to only be checked
            # if the voter voted absentee
            # NOTE: Assuming empty is not challenged i.e. False
            if history_2022_format:
                df_history["fl_challenged"] = "unknown"
            else:
                df_history.fl_challenged = df_history.fl_challenged == "Y"

            # Create dataframe of valid elections
            logging.info(f'[{config["state"]}] Grouping history by election.')
            election_columns = ["id_election", "dt_election", "Election_Name"]
            df_elections_all = df_history[election_columns]
            df_elections = (
                df_elections_all.reset_index()
                .groupby(election_columns)
                .count()
                .rename({"index": "count"}, axis=1)
                .reset_index()
            )

            # Clean up
            df_elections["Election_Name"] = df_elections["Election_Name"].str.strip()
            df_elections = self.config.coerce_dates(
                df_elections, col_list="hist_columns_types"
            )

            # Sort by ascending date
            df_elections = df_elections.sort_values(by=["dt_election"]).reset_index()

            # Create dict of elections keyed by election id
            logging.info(
                f'[{config["state"]}] Creating dictionary from {df_elections.size} elections for history encoding.'
            )
            elections_by_id = {}
            for k, row in df_elections.iterrows():
                try:
                    # This should be MM/DD/YYYY
                    # See: https://github.com/Voteshield/Inspector/wiki/Adding-a-State
                    election_date = row["dt_election"].strftime("%m/%d/%Y")
                except ValueError:
                    election_date = ""

                elections_by_id[row["id_election"]] = {
                    "index": k,
                    "count": row["count"],
                    "date": election_date
                    # "name": row["Election_Name"]
                }

            # Group history by voter id, then attach relevant group data to
            # voter dataframe
            logging.info(f'[{config["state"]}] Grouping history by voter.')
            df_history_grouped = df_history.groupby("id_voter")
            df_voters["all_history"] = df_history_grouped["id_election"].apply(list)
            df_voters["votetype_history"] = df_history_grouped["votetype"].apply(list)
            df_voters["challenged_history"] = df_history_grouped["fl_challenged"].apply(
                list
            )
            df_voters["sparse_history"] = df_voters["all_history"].map(
                lambda x: [elections_by_id[id]["index"] for id in x]
                if type(x) is list
                else []
            )

            # Fill any nan values in history
            df_voters["all_history"] = df_voters["all_history"].apply(
                lambda x: x if type(x) is list else []
            )
            df_voters["votetype_history"] = df_voters["votetype_history"].apply(
                lambda x: x if type(x) is list else []
            )
            df_voters["challenged_history"] = df_voters["challenged_history"].apply(
                lambda x: x if type(x) is list else []
            )

            # Create meta data
            logging.info(f'[{config["state"]}] Compiling meta output.')
            self.meta = {
                "message": "{}_{}".format(config["state"], datetime.now().isoformat()),
                "array_encoding": json.dumps(elections_by_id),
                "array_decoding": json.dumps(list(df_elections["id_election"])),
            }

        # Attach for testing and more direct access
        # See: https://github.com/Voteshield/reggie/issues/50
        self.processed_df = df_voters

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voters[self.config["primary_locale_identifier"]]),
        )

        # Create file from processed dataframe
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voters.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )

    def clean_party_value(self, value):
        """
        Party is very variable, maybe allows for write-in,
        so we try to clean up, but default to unknown.
        """
        value = str(value).strip().lower()
        lookup = {
            "democrat": re.compile("democrat"),
            "republican": re.compile("republican"),
            "libertarian": re.compile("libertarian"),
            "independent": re.compile("independent"),
            "green": re.compile("green"),
            "unaffiliated": re.compile("(unaffiliated|no.*affiliation)"),
        }

        for k, v in lookup.items():
            if v.match(value):
                return k

        return "unknown"

    def config_lookup_to_dict(self, config_values):
        """
        Takes a config like:
        gender_codes:
            Male:
                - M
                - m
            Female:
                - F
                - f
            Unknown:
                - N
                - nan
                - ""

        And turns it into:
            { "M": "Male", "m": "Male" ...}
        """
        converted = {}
        for c, v in config_values.items():
            for i in v:
                if i is None:
                    converted[""] = c
                elif i == "nan":
                    # converted[np.nan] = c
                    converted[float("nan")] = c
                    converted["nan"] = c
                else:
                    converted[i] = c

        return converted

    def config_lookup_to_valid_list(self, config_values):
        """
        Takes a config like:
        gender_codes:
            Male:
                - M
                - m
            Female:
                - F
                - f
            Unknown:
                - N
                - nan
                - ""

        And turns it into:
            ["Male", "Female", "Unknown"]
        """
        return config_values.keys()

    def check_column_has_valid_values(self, df, column, valid_values):
        config = self.config.data
        df_invalid_values = df[~df[column].isin(valid_values)]
        if len(df_invalid_values) > 0:
            raise InvalidDataError(
                f"{config['state']} found the following invalid values in the '{column}' column': {df_invalid_values[column].unique()}"
            )
