import datetime
import json

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
import re

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)


HISTORY_COLUMN_REGEX = re.compile(
    "^(\d|GENERAL|PRIMARY|PRESIDENTIAL)",
    flags=re.I,
)
YEAR_REGEX = re.compile("\d{4}")


class PreprocessArizona2(Preprocessor):
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

        def file_is_active(filename):
            for word in ["Canceled", "Suspense", "Inactive", "Cancelled", "NotReg"]:
                if word in filename:
                    return False
            return True

        def add_files_to_main_df(main_df, file_list):
            alias_dict = self.config["column_aliases"]
            for f in file_list:
                if f["name"].split(".")[-1] in ["csv", "txt"]:
                    new_df = self.read_csv_count_error_lines(
                        f["obj"], on_bad_lines="warn"
                    )
                else:
                    new_df = pd.read_excel(f["obj"])

                for c in new_df.columns:
                    # files vary in consistent use of spaces in headers,
                    # and some headers have different aliases for headers
                    if c.replace(" ", "") in alias_dict.keys():
                        new_df.rename(
                            columns={c: alias_dict[c.replace(" ", "")]},
                            inplace=True,
                        )
                    else:
                        new_df.rename(columns={c: c.replace(" ", "")}, inplace=True)
                new_df.rename(columns={"YearofBirth": "DOB"}, inplace=True)
                main_df = pd.concat([main_df, new_df], sort=False)
            return main_df

        def insert_code_bin(arr):
            return [sorted_codes_dict[k]["index"] for k in arr]

        new_files = self.unpack_files(file_obj=self.main_file, compression="unzip")

        active_files = [f for f in new_files if file_is_active(f["name"])]

        # Starting from Feb 2024, the cancelled voters and active voters are all included in one big file
        other_files = [f for f in new_files if not file_is_active(f["name"])]

        main_df = pd.DataFrame()
        main_df = add_files_to_main_df(main_df, active_files)
        if other_files:
            main_df = add_files_to_main_df(main_df, other_files)
            main_df.reset_index(drop=True, inplace=True)

        # In Dec 2024, file contained 2 copies of "RegistrantID" - removing one
        if len(np.where(main_df.columns == "RegistrantID")[0]) > 1:
            main_df = main_df.loc[:, ~main_df.columns.duplicated()].copy()

        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_strings(main_df)
        main_df = self.config.coerce_numeric(
            main_df,
            extra_cols=[
                "HouseNumber",
                "UnitNumber",
                "ResidenceZip",
                "MailingZip",
                "Phone",
                "PrecinctPart",
                "VRAZVoterID",
            ],
        )
        # Starting in Feb 2024 occupation was removed from the columns they send us
        if "Occupation" not in main_df.columns:
            main_df["Occupation"] = np.nan

        # Keep values in new columns FedIDOnly and FedNoID consistent (true/false, not yes/no):
        def convert_to_boolean(x):
            if x == "yes":
                return "true"
            elif x == "no":
                return "false"
            else:
                return x

        if "FedIDOnly" in main_df.columns:
            main_df["FedIDOnly"] = main_df["FedIDOnly"].map(convert_to_boolean)
        if "FedNoID" in main_df.columns:
            main_df["FedNoID"] = main_df["FedNoID"].map(convert_to_boolean)

        # In spring 2025, they replaced election names with non-descript column headers.
        # However, we can tell that the 5 non-descript headers (EL1 through EL5) correspond
        # with the 5 known elections (in 2022 & 2024) from the previous files.
        # Since elections seem to only be added during even years, we can assume that
        # these 5 elections will stay constant at least over the rest of 2025.
        # However, we need to trigger manual intervention to re-evaluate the situation
        # in 2026, if we have not received better data by then.
        if date_from_str(self.raw_s3_file) <= "2026-01-01":
            main_df.rename(
                columns={
                    "EL1": "PRIMARY2022",
                    "EL2": "GENERAL2022",
                    "EL3": "2024PRESIDENTIALPREFERENCE",
                    "EL4": "PRIMARY2024",
                    "EL5": "GENERAL2024",
                },
                inplace=True,
            )
        else:
            raise ValueError(
                "Arizona2 has unreliable election codes that we have been "
                "assuming (during 2025) refer to 2024 and 2022 elections. "
                "Since it is now 2026, Arizona may have recently or "
                "will soon change the set of elections reported in the data. "
                "Please take a look at the current file and see what the state "
                "of the election data is."
            )

        voter_columns = [c for c in main_df.columns if not HISTORY_COLUMN_REGEX.match(c)]
        history_columns = [c for c in main_df.columns if HISTORY_COLUMN_REGEX.match(c)]

        # self.column_check(voter_columns)
        to_normalize = history_columns + [
            self.config["party_identifier"],
            self.config["voter_status"],
        ]
        for c in to_normalize:
            s = main_df[c].astype(str).str.strip().str.lower()
            s = s.str.encode("utf-8", errors="ignore").str.decode("utf-8")
            main_df.loc[(~main_df[c].isna()), c] = s.loc[(~main_df[c].isna())]
        for c in history_columns:
            main_df[c] = main_df[c].str.replace(" - ", "_")

        main_df[self.config["party_identifier"]] = main_df[
            self.config["party_identifier"]
        ].map(
            lambda x: self.config["party_aliases"][x]
            if x in self.config["party_aliases"]
            else x
        )

        # 2025 April - some truncated data is causing trouble, where
        # RegistrantID = 0, so drop any row(s) that look like that:
        bad_rows = main_df[main_df["RegistrantID"] == 0].index
        main_df.drop(index=bad_rows, inplace=True)

        # AZ removed full history dates from column names in
        # Sept 2024, so have to handle a little more manually now.
        def handle_history_dates(col_name):
            # Try original method, which works if full dates
            d = date_from_str(col_name)
            if d is not None:
                return d
            # Try manual matching to known elections
            if col_name in self.config["election_dates"]:
                return self.config["election_dates"][col_name]
            # Default placeholder otherwise is just Jan 1
            d = YEAR_REGEX.search(col_name)
            if d is not None:
                return f"01/01/{d.group(0)}"
            return None

        # handle history:
        sorted_codes = history_columns[::-1]
        hist_df = main_df[sorted_codes]
        voter_df = main_df[voter_columns]

        del main_df

        counts = (~hist_df.isna()).sum()
        sorted_codes_dict = {
            k: {
                "index": int(i),
                "count": int(counts[k]),
                "date": handle_history_dates(k),
            }
            for i, k in enumerate(sorted_codes)
        }

        def filter_vote_codes(x):
            if type(x) is list:
                return [c for c in x if not pd.isna(c)]
            return []

        hist_df.loc[:, "vote_codes"] = pd.Series(hist_df.values.tolist())
        hist_df.loc[:, "vote_codes"] = hist_df.loc[:, "vote_codes"].map(filter_vote_codes)

        voter_df.loc[:, "votetype_history"] = hist_df.loc[:, "vote_codes"].map(
            lambda x: [c.split("_")[0] for c in x]
        )
        voter_df.loc[:, "party_history"] = hist_df.loc[:, "vote_codes"].map(
            lambda x: [
                c.split("_")[1]
                if len(c.split("_")) > 1
                else self.config["no_party_affiliation"]
                for c in x
            ]
        )

        hist_df.drop(columns=["vote_codes"], inplace=True)
        for c in hist_df.columns:
            hist_df.loc[:, c] = hist_df.loc[:, c].map(
                lambda x: c if not pd.isna(x) else np.nan
            )
        voter_df.loc[:, "all_history"] = pd.Series(hist_df.values.tolist())
        voter_df.loc[:, "all_history"] = voter_df.loc[:, "all_history"].map(filter_vote_codes)
        voter_df.loc[:, "sparse_history"] = voter_df.loc[:, "all_history"].map(
            insert_code_bin
        )

        expected_cols = (
            self.config["ordered_columns"] + self.config["ordered_generated_columns"]
        )
        voter_df = self.reconcile_columns(voter_df, expected_cols)

        # At some point in time they added in new columns we wanted to track
        # These columns must be added to the end of the file to match the schema
        # Or else will need to reconstuct the state. 

        # Todo: figure out a better way?
        ordered_cols = [col for col in expected_cols if col not in self.config["new_columns"]]
        ordered_cols += self.config["new_columns"]
        voter_df = voter_df[ordered_cols]

        # Check the file for all the proper locales
        self.locale_check(
            set(voter_df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "arizona2_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(voter_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
