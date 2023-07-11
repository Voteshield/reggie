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
        vh_files_old_style = []
        vh_files_new_style = []

        # Georgia likes changing the name of its voter file
        possible_voterfile_names = [
            "georgia_daily_voterbase",
            "statewidevoterlist",
            "statewide_voter_list",
            "gdvb",
            "georgia_state_wide_voter_file",
            "statewide voter file",
        ]

        for i in new_files:
            if any(name in i["name"].lower() for name in possible_voterfile_names):
                logging.info("Detected voter file: " + i["name"])
                voter_files.append(i)
            elif "txt" in i["name"].lower():
                vh_files_old_style.append(i)
            # new-style history files are CSV's
            elif "csv" in i["name"].lower():
                vh_files_new_style.append(i)

        logging.info(
            f"Detected {len(vh_files_old_style)} old-style history files "
            f"and {len(vh_files_new_style)} new-style history files"
        )
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

        # Georgia fully overhauled its voter reg system in 2023, with
        # several different interim formats coming through over several months
        sep = "|"
        quoting = 3
        file_date = datetime.strptime(date_from_str(self.raw_s3_file), "%Y-%m-%d")
        if file_date > datetime(2023, 2, 5):
            header_arg = 0
        if file_date > datetime(2023, 3, 13):
            sep = ","
            quoting = 0

        df_voters = self.read_csv_count_error_lines(
            voter_files[0]["obj"],
            sep=sep,
            header=header_arg,
            quotechar='"',
            quoting=quoting,
            on_bad_lines="warn",
        )
        del voter_files
        gc.collect()

        # Nov 2022 file contains an extra "County Name" col at the
        # beginning, so remove this before applying header:
        if len(df_voters.columns) == len(self.config["ordered_columns"]) + 1:
             df_voters.drop(columns=["County Name"], inplace=True)

        # Should only try to apply old headers to pre- 2023 overhaul files
        if file_date <= datetime(2023, 2, 5):
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
        else:
            # New 2023 files need a lot of renaming of columns
            df_voters.rename(
                columns=self.config["column_aliases"],
                inplace=True,
            )
            df_voters = self.reconcile_columns(df_voters, self.config["columns"])
            df_voters["Race_desc"] = df_voters["Race"]

            # Convert county back to numbers to match existing system
            county_dict = self.config.primary_locale_names[self.config.primary_locale_type]
            county_dict = {v.lower().replace(" ",""): int(k) for k, v in county_dict.items()}
            df_voters["County_code"] = df_voters["County_code"].str.lower().map(county_dict)

            # Convert voter status to match existing system
            df_voters["Voter_status"] = df_voters["Voter_status"].map(
                {"Active": "A", "Inactive": "I"}
            )

        df_voters["Registration_Number"] = (
            df_voters["Registration_Number"].astype(str).str.zfill(8)
        )

        # Need to read both old-style and new-style (w header) history files
        concat_history_file_old = concat_and_delete(
            vh_files_old_style, has_headers=False
        )
        del vh_files_old_style
        gc.collect()

        logging.info("Performing GA history manipulation")

        # Read old-style history files
        logging.info("Reading old-style history files")
        history = self.read_csv_count_error_lines(
            concat_history_file_old,
            names=["Concat_str"],
            on_bad_lines="warn",
        )
        del concat_history_file_old
        gc.collect()

        # this is never used
        # history["County_Number"] = history["Concat_str"].str[0:3]

        history["Registration_Number"] = history["Concat_str"].str[3:11]
        history["Election_Date"] = history["Concat_str"].str[11:19]
        history["Election_Type"] = history["Concat_str"].str[19:22]
        history["Party"] = history["Concat_str"].str[22:24]

        history["Absentee"] = history["Concat_str"].str[24]
        history["Provisional"] = history["Concat_str"].str[25]
        history["Supplemental"] = history["Concat_str"].str[26]
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

        # Year the date format switched from "%m%d%Y" to "%Y%m%d"
        date_format_switch = "2013"
        history["Election_Date"] = history["Election_Date"].map(
            lambda x: x[4:8] + x[0:2] + x[2:4] if x[0:4] < date_format_switch else x
        )

        # If they exist, read new-style history files
        if len(vh_files_new_style) > 0:
            concat_history_file_new = concat_and_delete(
                vh_files_new_style, has_headers=True
            )
            del vh_files_new_style
            gc.collect()

            logging.info("Reading new-style history files")
            history_new = self.read_csv_count_error_lines(
                concat_history_file_new,
                sep=",",
                header=0,
            )
            del concat_history_file_new
            gc.collect()

            # Convert to match old style
            history_new.dropna(subset=["Voter Registration Number"], inplace=True)
            history_new["Registration_Number"] = (
                history_new["Voter Registration Number"].astype(int).astype(str).str.zfill(8)
            )
            history_new["Election_Date"] = pd.to_datetime(
                history_new["Election Date"]).map(lambda x: x.strftime("%Y%m%d")
            )
            history_new["Election_Type"] = history_new["Election Type"]

            # Concat all old and new history together
            history = pd.concat([history, history_new])
            history.reset_index(drop=True, inplace=True)

        for c in ["Party", "Absentee", "Provisional", "Supplemental"]:
            history[c] = history[c].fillna("N")

        history["Election_Type"] = history["Election_Type"].fillna("unknown")

        history["Party"] = history["Party"].str.strip()

        history["Combo_history"] = history["Election_Date"].str.cat(
            others=history[
                [
                    "Election_Type",
                    "Party",
                    "Absentee",
                    "Provisional",
                    "Supplemental",
                ]
            ],
            sep="_",
        )
        # Remove erroneous "'" single quote chars
        history["Combo_history"] = history["Combo_history"].str.replace("'","")

        history = history.filter(
            items=[
                "Registration_Number",
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
