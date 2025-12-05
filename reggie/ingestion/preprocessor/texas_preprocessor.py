import datetime
import gc
import json
import logging

from datetime import datetime
from io import StringIO, SEEK_END, SEEK_SET

import numpy as np
import pandas as pd
import usaddress

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import (
    MissingNumColumnsError,
)


class PreprocessTexas(Preprocessor):
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
        widths_one = [
            3,
            10,
            10,
            50,
            50,
            50,
            50,
            4,
            1,
            8,
            9,
            12,
            2,
            50,
            12,
            2,
            12,
            12,
            50,
            9,
            110,
            50,
            50,
            20,
            20,
            8,
            1,
            1,
            8,
            2,
            3,
            6,
        ]
        widths_two = [
            3,
            4,
            10,
            50,
            50,
            50,
            50,
            4,
            1,
            8,
            9,
            12,
            2,
            50,
            12,
            2,
            12,
            12,
            50,
            9,
            110,
            50,
            50,
            20,
            20,
            8,
            1,
            1,
            8,
            2,
            3,
            6,
        ]
        df_voter = pd.DataFrame(columns=self.config.raw_file_columns())
        df_hist = pd.DataFrame(columns=self.config.raw_file_columns())
        have_length = False
        for i in new_files:
            file_len = i["obj"].seek(SEEK_END)
            i["obj"].seek(SEEK_SET)
            if "count" not in i["name"] and file_len != 0:

                if not have_length:
                    line_length = len(i["obj"].readline())
                    i["obj"].seek(SEEK_END)
                    have_length = True
                    if line_length == 686:
                        widths = widths_one
                    elif line_length == 680:
                        widths = widths_two
                    else:
                        raise ValueError(
                            "Width possibilities have changed,"
                            "new width found: {}".format(line_length)
                        )
                    have_length = True
                logging.info("Loading file {}".format(i))
                new_df = pd.read_fwf(i["obj"], widths=widths, header=None)
                try:
                    new_df.columns = self.config.raw_file_columns()
                except ValueError:
                    logging.info("Incorrect number of columns found for texas")
                    raise MissingNumColumnsError(
                        "{} state is missing columns".format(self.state),
                        self.state,
                        len(self.config.raw_file_columns()),
                        len(new_df.columns),
                    )
                if new_df["Election_Date"].head(n=100).isnull().sum() > 75:
                    df_voter = pd.concat(
                        [df_voter, new_df], axis=0, ignore_index=True
                    )
                else:
                    df_hist = pd.concat(
                        [df_hist, new_df], axis=0, ignore_index=True
                    )
            del i["obj"]
            gc.collect()
        if df_hist.empty:
            logging.info("This file contains no voter history")
        df_voter["Effective_Date_of_Registration"] = (
            df_voter["Effective_Date_of_Registration"]
            .fillna(-1)
            .astype(int, errors="ignore")
            .astype(str)
            .replace("-1", np.nan)
        )
        df_voter[self.config["party_identifier"]] = "npa"
        df_hist[self.config["hist_columns"]] = df_hist[
            self.config["hist_columns"]
        ].replace(np.nan, "", regex=True)
        df_hist["election_name"] = (
            df_hist["Election_Date"].astype(str)
            + "_"
            + df_hist["Election_Type"].astype(str)
            + "_"
            + df_hist["Election_Party"].astype(str)
        )

        valid_elections, counts = np.unique(
            df_hist["election_name"], return_counts=True
        )

        def texas_datetime(x):
            try:
                return datetime.strptime(x[0:8], "%Y%m%d")
            except (ValueError):
                return datetime(1970, 1, 1)

        date_order = [
            idx
            for idx, election in sorted(
                enumerate(valid_elections),
                key=lambda x: texas_datetime(x[1]),
                reverse=True,
            )
        ]
        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {
            k: {
                "index": i,
                "count": int(counts[i]),
                "date": str(texas_datetime(k).date()),
            }
            for i, k in enumerate(sorted_codes)
        }

        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"])
        )
        logging.info("Texas: history apply")
        voter_groups = df_hist.groupby(self.config["voter_id"])
        sparse_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["Election_Voting_Method"].apply(list)

        df_voter = df_voter.set_index(self.config["voter_id"])
        df_voter["sparse_history"] = sparse_history
        df_voter["all_history"] = voter_groups["election_name"].apply(list)
        df_voter["vote_type"] = vote_type
        gc.collect()
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(
            df_voter,
            extra_cols=[
                "Permanent_Zipcode",
                "Permanent_House_Number",
                "Mailing_Zipcode",
            ],
        )
        df_voter.drop(self.config["hist_columns"], axis=1, inplace=True)

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voter[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "texas_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        gc.collect()
        logging.info("Texas: writing out")
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8")),
            s3_bucket=self.s3_bucket,
        )

    def parse_and_map_address(self, address):
        """
        Attempt to parse a single address string
        and map it back into the existing database
        fields, if at all possible.
        """
        def parsed_to_dict(parsed_address):
            """
            Turn parser pieces into a dictionary.
            If multiple parts parse to the same type of
            piece, solve by concatenating pieces in order.
            """
            d = {}
            for piece, piece_type in parsed_address:
                if piece_type not in d:
                    d[piece_type] = piece
                else:
                    d[piece_type] += " " + piece
            return d

        def map_to_existing_fields(address_dict):
            """
            Convert the parsed address dictionary
            back to the fields that exist in Texas'
            older files (and VoteShield's database).
            """
            d = {}
            for old, new_list in self.config["address_parser_mapping"]:
                d[old] = " ".join(
                    filter(
                        None,
                        [address_dict[x] if (x in address_dict) else None for x in new_list]
                    )
                )
            return d

        parsed_tuples = usaddress.parse(address)

        # If contains an unmanageable type, just return whole
        # string in default field (Permanent_Street_Name).
        piece_types = [piece[1] for piece in parsed_tuples]
        if len(set(piece_types).intersection(self.config["unmanageable_address_fields"])):
            return {
                self.config["default_address_field"]: address
            }
        else:
            # Consolidate parsed tuples into dict
            parsed_dict = parsed_to_dict(parsed_tuples)
            return map_to_existing_fields(parsed_dict)

# DO THIS in main function
# parsed_addresses = df['residential_address'].map(self.parse_and_map_address)
# addr_df = pd.DataFrame(parsed_addresses.tolist())
