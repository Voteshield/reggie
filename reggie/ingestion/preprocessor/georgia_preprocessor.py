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
    concat_and_delete,
)
from reggie.ingestion.utils import (
    MissingNumColumnsError,
)


class PreprocessGeorgia(Preprocessor):
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
        self.config_file = config_file
        self.processed_file = None

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        logging.info("GEORGIA: loading voter and voter history file")
        new_files = self.unpack_files(
            compression="unzip", file_obj=self.main_file
        )
        del self.main_file, self.temp_files
        gc.collect()

        voter_files = []
        vh_files = []

        # Georgia likes changing the name of its voter file
        possible_voterfile_names = [
            "georgia_daily_voterbase",
            "statewidevoterlist",
            "statewide_voter_list",
            "gdvb",
            "georgia_state_wide_voter_file",
        ]

        for i in new_files:
            if any(name in i["name"].lower() for name in possible_voterfile_names):
                logging.info("Detected voter file: " + i["name"])
                voter_files.append(i)
            elif "txt" in i["name"].lower():
                vh_files.append(i)

        logging.info("Detected {} history files".format(len(vh_files)))
        del new_files
        gc.collect()

        if not self.ignore_checks:
            self.file_check(len(voter_files))

        # Georgia voter files around Dec 2022 have had no consistency
        # in terms of having a header or not - so need to explicitly
        # discover this:
        header = voter_files[0]["obj"].readline().decode()
        voter_files[0]["obj"].seek(0)
        if "county_code" in header.lower():
            header_arg = 0
        else:
            header_arg = None

        df_voters = self.read_csv_count_error_lines(
            voter_files[0]["obj"],
            sep="|",
            header=header_arg,
            quotechar='"',
            quoting=3,
            on_bad_lines="warn",
        )
        del voter_files
        gc.collect()

        # Nov 2022 file contains an extra "County Name" col at the
        # beginning, so remove this before applying header:
        if len(df_voters.columns) == len(self.config["ordered_columns"]) + 1:
             df_voters.drop(columns=["County Name"], inplace=True)

        try:
            df_voters.columns = self.config["ordered_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for Georgia")
            raise MissingNumColumnsError(
                "{} state is missing columns".format(self.state),
                self.state,
                len(self.config["ordered_columns"]),
                len(df_voters.columns),
            )
        df_voters["Registration_Number"] = (
            df_voters["Registration_Number"].astype(str).str.zfill(8)
        )

        concat_history_file = concat_and_delete(vh_files)
        del vh_files
        gc.collect()

        logging.info("Performing GA history manipulation")

        history = self.read_csv_count_error_lines(
            concat_history_file,
            sep="  ",
            names=["Concat_str", "Other"],
            on_bad_lines="warn",
        )
        del concat_history_file
        gc.collect()

        history["County_Number"] = history["Concat_str"].str[0:3]
        history["Registration_Number"] = history["Concat_str"].str[3:11]
        history["Election_Date"] = history["Concat_str"].str[11:19]
        history["Election_Type"] = history["Concat_str"].str[19:22]
        history["Party"] = history["Concat_str"].str[22:24]
        history["Absentee"] = history["Other"].str[0]
        history["Provisional"] = history["Other"].str[1]
        history["Supplimental"] = history["Other"].str[2]
        type_dict = {
            "001": "GEN_PRIMARY",
            "002": "GEN_PRIMARY_RUNOFF",
            "003": "GEN",
            "004": "GEN_ELECT_RUNOFF",
            "005": "SPECIAL_ELECT",
            "006": "SPECIAL_RUNOFF",
            "007": "NON-PARTISAN",
            "008": "SPECIAL_NON-PARTISAN",
            "009": "RECALL",
            "010": "PPP",
        }
        history = history.replace({"Election_Type": type_dict})
        history["Combo_history"] = history["Election_Date"].str.cat(
            others=history[
                [
                    "Election_Type",
                    "Party",
                    "Absentee",
                    "Provisional",
                    "Supplimental",
                ]
            ],
            sep="_",
        )
        history = history.filter(
            items=[
                "County_Number",
                "Registration_Number",
                "Election_Date",
                "Election_Type",
                "Party",
                "Absentee",
                "Provisional",
                "Supplimental",
                "Combo_history",
            ]
        )
        history = history.dropna()

        logging.info("Creating GA sparse history")

        valid_elections, counts = np.unique(
            history["Combo_history"], return_counts=True
        )

        date_order = [
            idx
            for idx, election in sorted(
                enumerate(valid_elections),
                key=lambda x: datetime.strptime(x[1][0:8], "%Y%m%d"),
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
                "date": datetime.strptime(k[0:8], "%Y%m%d"),
            }
            for i, k in enumerate(sorted_codes)
        }
        history["array_position"] = history["Combo_history"].map(
            lambda x: int(sorted_codes_dict[x]["index"])
        )

        voter_groups = history.groupby("Registration_Number")
        all_history = voter_groups["Combo_history"].apply(list)
        all_history_indices = voter_groups["array_position"].apply(list)
        df_voters = df_voters.set_index("Registration_Number")
        df_voters["party_identifier"] = "npa"
        df_voters["all_history"] = all_history
        df_voters["sparse_history"] = all_history_indices
        del history, voter_groups, all_history, all_history_indices
        gc.collect()

        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_strings(df_voters, exclude=["Race", "Race_desc", "Gender"])
        df_voters = self.config.coerce_numeric(
            df_voters,
            extra_cols=[
                "Land_district",
                "Mail_house_nbr",
                "Land_lot",
                "Commission_district",
                "School_district",
                "Ward city council_code",
                "County_precinct_id",
                "Judicial_district",
                "County_district_a_value",
                "County_district_b_value",
                "City_precinct_id",
                "Mail_address_2",
                "Mail_address_3",
                "Mail_apt_unit_nbr",
                "Mail_country",
                "Residence_apt_unit_nbr",
            ],
        )

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voters[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "georgia_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(
                sorted_codes_dict, indent=4, sort_keys=True, default=str
            ),
            "array_decoding": json.dumps(sorted_codes),
            "election_type": json.dumps(type_dict),
        }

        csv_obj = df_voters.to_csv(encoding="utf-8")
        del df_voters
        gc.collect()

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_obj),
            s3_bucket=self.s3_bucket,
        )
        del csv_obj
        gc.collect()
