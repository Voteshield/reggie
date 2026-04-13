import json
import logging
import re
from datetime import datetime
from io import StringIO

import pandas as pd

from reggie.ingestion.download import FileItem, Preprocessor, date_from_str
from reggie.ingestion.utils import ensure_int_string


class PreprocessNewMexico(Preprocessor):
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

        new_files = self.unpack_files(file_obj=self.main_file)

        if not self.ignore_checks:
            self.file_check(len(new_files))

        df = pd.DataFrame()
        for f in new_files:
            if ".csv" in f["name"] and "._" not in f["name"]:
                temp_df = self.read_csv_count_error_lines(
                    f["obj"],
                    encoding="utf-8-sig", # problematic UTF-8 BOM, couldn't use latin-1
                    on_bad_lines="warn",
                )
                df = pd.concat([df, temp_df], axis=0)
        df.reset_index(drop=True, inplace=True)

        # Drop unnamed columns from trailing commas in CSV
        df = df.drop(columns=[c for c in df.columns if "Unnamed" in str(c)])

        # Detect and separate out election columns (format = "11/03/2020-GENERAL ELECTION") 
        date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}-")
        election_cols = [c for c in df.columns if date_pattern.match(c)]

        # Verify fixed voter columns match yaml
        self.column_check(list(set(df.columns) - set(election_cols)))

        # Parse date and election type from a column header string
        # e.g. "11/03/2020-2020 GENERAL ELECTION" -> (datetime, "general_2020-11-03")
        def parse_election_col(col_name):
            date_str, election_name = col_name.split("-", 1)
            dt = datetime.strptime(date_str, "%m/%d/%Y")
            # Skip extra year if restated in election name
            words = election_name.strip().lower().split()
            election_type = next((w for w in words if not w.isdigit()), words[0])
            election_id = "{}_{}".format(election_type, dt.strftime("%Y-%m-%d"))
            return dt, election_id

        # Sort election columns ascending by date (oldest first)
        election_cols_sorted = sorted(
            election_cols,
            key=lambda c: datetime.strptime(c.split("-")[0], "%m/%d/%Y"),
        )
        col_to_id = {col: parse_election_col(col)[1] for col in election_cols_sorted}

        # Build array_encoding metadata: election_id -> {index, count, date}
        sorted_codes = [col_to_id[c] for c in election_cols_sorted]
        sorted_codes_dict = {
            col_to_id[c]: {
                "index": i,
                "count": int(df[c].notna().sum()),
                "date": c.split("-")[0],
            }
            for i, c in enumerate(election_cols_sorted)
        }

        # Build voter history arrays from the columnar election data
        voter_id = self.config["voter_id"]  # "VoterID"
        hist_df = df[[voter_id] + election_cols_sorted].melt(
            id_vars=[voter_id],
            value_vars=election_cols_sorted,
            var_name="election_col",
            value_name="vote_value",
        )
        hist_df = hist_df.dropna(subset=["vote_value"])
        hist_df = hist_df[hist_df["vote_value"].str.strip() != ""]

        hist_df["election_id"] = hist_df["election_col"].map(col_to_id)
        # Extract just the code before the first dash (eg E from "E-SANTA FE COUNTY")
        hist_df["vote_type"] = hist_df["vote_value"].str.split("-").str[0].str.strip()

        df = df.set_index(voter_id, drop=False)
        df["all_history"] = hist_df.groupby(voter_id)["election_id"].apply(list)
        df["votetype_history"] = hist_df.groupby(voter_id)["vote_type"].apply(list)

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[e]["index"] for e in arr]
            return float("nan")

        df["sparse_history"] = df["all_history"].map(insert_code_bin)
        df = df.reset_index(drop=True)

        # Drop the raw election columns now that history arrays are built
        df = df.drop(columns=election_cols)

        # Coerce dates, numerics, and strings to standard types
        df = self.config.coerce_dates(df)
        df = self.config.coerce_numeric(
            df,
            extra_cols=[
                "HouseNumber",
                "UnitNumber",
                "Zip",
                "MailingZip",
                "TelephoneNum",
                "PrecinctPart",
                "Congressional",
                "Legislative",
                "Senate",
                "CountyCommissioner",
            ],
        )
        df = self.config.coerce_strings(df)

        # Normalize district columns to clean integer strings e.g. "1" not "1.0"
        for col in ["Congressional", "Legislative", "Senate", "CountyCommissioner"]:
            df[col] = df[col].map(ensure_int_string)

        # Verify all locale values in the file are recognized
        self.locale_check(set(df[self.config["primary_locale_identifier"]]))

        self.meta = {
            "message": "new_mexico_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
