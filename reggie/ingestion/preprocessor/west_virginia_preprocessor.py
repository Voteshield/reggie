"""
West Virginia preprocessor.
"""

import re
import logging
import datetime
from datetime import datetime
from dateutil import parser
import gc
import json
from io import StringIO, BytesIO, SEEK_END, SEEK_SET

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from reggie.ingestion.utils import (
    UnexpectedNumberOfFilesError,
    MissingNumColumnsError,
    InvalidDataError,
    format_column_name,
)

from slugify import slugify
import pandas as pd
import numpy as np


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
        Main preprocessor function.
        """
        # Shortcut to config
        config = self.config.data

        # Download if not file not provided
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        # Get the files from the archive
        new_files = [n for n in self.unpack_files(self.main_file, compression="unzip")]

        # Should only be one voter file
        voter_file_regex = re.compile(".*statewide.*vr")
        voter_files = [
            n for n in new_files if voter_file_regex.match(n["name"].lower())
        ]
        if len(voter_files) > 1:
            raise UnexpectedNumberOfFilesError(
                f"{config['state']} has too many voter files."
            )
        if len(voter_files) < 1:
            raise UnexpectedNumberOfFilesError(
                f"Unable to find voter file in {config['state']} import."
            )
        voter_file = voter_files[0]

        # Read voter file into pandas dataframe
        df_voters = pd.read_csv(
            voter_file["obj"],
            sep=config["delimiter"],
            encoding="latin-1",
            dtype=str,
            header=0 if config["has_headers"] else None,
        )

        # Add county id column
        df_voters.insert(
            1,
            "County_ID",
            df_voters.apply(lambda row: slugify(row.County_Name), axis=1),
        )

        # Make sure counties are valid as defined in confid
        # counties = df_voters.County_ID.unique()
        self.check_column_has_valid_values(
            df_voters,
            "County_ID",
            self.config_lookup_to_valid_list(config["locales_counties"]),
        )

        # Gender clean
        gender_dict = self.config_lookup_to_dict(self.config["gender_codes"])
        df_voters.loc[:, "SEX"] = df_voters.loc[:, "SEX"].map(gender_dict)

        # Gender check
        self.check_column_has_valid_values(
            df_voters, "SEX", self.config_lookup_to_valid_list(config["gender_codes"])
        )

        # Party clean
        parties = df_voters.PartyAffiliation.unique()
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

        # Coerce dates
        df_voters = self.config.coerce_dates(df_voters)

        # Set index
        # TODO: Is this necessary?  Is this helpful higher up?
        df_voters = df_voters.set_index(config["voter_id"])

        # TODO: Voter history

        # Create meta data
        self.meta = {
            "message": "{}_{}".format(config["state"], datetime.now().isoformat())
            # vote history not available
            #            'array_encoding': json.dumps(),
            #            'array_decoding': json.dumps()
        }

        # Create file from processed dataframe
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voters.to_csv(encoding="utf-8", index=True)),
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
            "unaffliated": re.compile("(unaffliated|no.*affliation)"),
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
