import datetime
import gc
import json
import logging

from datetime import datetime
from io import StringIO

import numpy as np

from reggie.configs.configs import Config
from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import MissingNumColumnsError


class PreprocessNorthCarolina(Preprocessor):
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
        self.config = Config(file_name=config_file)

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            file_obj=self.main_file
        )  # array of dicts
        del self.main_file, self.temp_files
        gc.collect()

        if not self.ignore_checks:
            self.file_check(len(new_files))

        for i in new_files:
            if ("ncvhis" in i["name"]) and (".txt" in i["name"]):
                vote_hist_file = i
            elif ("ncvoter" in i["name"]) and (".txt" in i["name"]):
                voter_file = i
        voter_df = self.read_csv_count_error_lines(
            voter_file["obj"],
            sep="\t",
            quotechar='"',
            encoding="latin-1",
            on_bad_lines="warn",
        )
        del voter_file
        gc.collect()

        vote_hist = self.read_csv_count_error_lines(
            vote_hist_file["obj"],
            sep="\t",
            quotechar='"',
            on_bad_lines="warn",
        )
        del vote_hist_file, new_files
        gc.collect()

        # In February 2022, NC changed the name of 5 columns, dropped 4 columns,
        # and rearranged the rest of the columns somewhat.
        # Since none of the dropped columns are crucial to VoteShield,
        # we just convert the file back to the old format.
        if "age_at_year_end" in voter_df.columns:
             voter_df.rename(columns={"age_at_year_end": "birth_age"}, inplace=True)
        if "birth_state" in voter_df.columns:
             voter_df.rename(columns={"birth_state": "birth_place"}, inplace=True)
        if "middle_name" in voter_df.columns:
             voter_df.rename(columns={"middle_name": "midl_name"}, inplace=True)
        if "name_suffix_lbl" in voter_df.columns:
             voter_df.rename(columns={"name_suffix_lbl": "name_sufx_cd"}, inplace=True)
        if "confidential_ind" in voter_df.columns:
             voter_df.rename(columns={"confidential_ind": "Confidential_ind"}, inplace=True)
        for field in ["absent_ind", "dist_2_abbrv", "dist_2_desc", "name_prefx_cd"]:
            if field not in voter_df.columns:
                voter_df[field] = np.nan

        if len(voter_df.columns) == len(self.config["ordered_columns"]):
            # Rearrange into original order
            voter_df = voter_df[self.config["ordered_columns"]]
        else:
            logging.info(
                "Incorrect number of columns found for the voter file in North Carolina"
            )
            raise MissingNumColumnsError(
                "{} state is missing columns".format(self.state),
                self.state,
                len(self.config["ordered_columns"]),
                len(voter_df.columns),
            )
        try:
            vote_hist.columns = self.config["hist_columns"]
        except ValueError:
            logging.info(
                "Incorrect number of columns found for the history file in North Carolina"
            )
            raise

        valid_elections, counts = np.unique(
            vote_hist["election_desc"], return_counts=True
        )
        count_order = counts.argsort()[::-1]
        valid_elections = valid_elections[count_order]
        counts = counts[count_order]

        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {
            k: {"index": i, "count": int(counts[i]), "date": date_from_str(k)}
            for i, k in enumerate(sorted_codes)
        }
        vote_hist["array_position"] = vote_hist["election_desc"].map(
            lambda x: int(sorted_codes_dict[x]["index"])
        )
        del valid_elections, counts, count_order
        gc.collect()

        voter_groups = vote_hist.groupby(self.config["voter_id"])
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["voting_method"].apply(list)

        voter_df = voter_df.set_index(self.config["voter_id"])

        voter_df["all_history"] = all_history
        voter_df["vote_type"] = vote_type
        del voter_groups, vote_hist, all_history, vote_type
        gc.collect()

        # In May 2024, NC started using "XX-XX-XXXX" for
        # registration dates of some cancelled and
        # inactive voters. Need to convert these to
        # explicitly null for our system.
        voter_df["registr_dt"] = voter_df["registr_dt"].map(
            lambda x: x if x != "XX-XX-XXXX" else ""
        )

        voter_df = self.config.coerce_strings(voter_df)
        voter_df = self.config.coerce_dates(voter_df)
        voter_df = self.config.coerce_numeric(
            voter_df,
            extra_cols=[
                "county_commiss_abbrv",
                "fire_dist_abbrv",
                "full_phone_number",
                "judic_dist_abbrv",
                "munic_dist_abbrv",
                "municipality_abbrv",
                "precinct_abbrv",
                "precinct_desc",
                "school_dist_abbrv",
                "super_court_abbrv",
                "township_abbrv",
                "township_desc",
                "vtd_abbrv",
                "vtd_desc",
                "ward_abbrv",
            ],
        )

        # Check the file for all the proper locales
        self.locale_check(
            set(voter_df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "north_carolina_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        self.is_compressed = False

        csv_obj = voter_df.to_csv(encoding="utf-8", index=True)
        del voter_df
        gc.collect()

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_obj),
            s3_bucket=self.s3_bucket,
        )
        del csv_obj
        gc.collect()
