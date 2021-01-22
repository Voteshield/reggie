import json
from reggie.ingestion.download import Preprocessor, date_from_str, FileItem
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime


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
            for word in ["Canceled", "Suspense", "Inactive"]:
                if word in filename:
                    return False
            return True

        def add_files_to_main_df(main_df, file_list):
            alias_dict = self.config["column_aliases"]
            for f in file_list:
                if f["name"].split(".")[-1] == "csv":
                    new_df = self.read_csv_count_error_lines(
                        f["obj"], error_bad_lines=False
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
                        new_df.rename(
                            columns={c: c.replace(" ", "")}, inplace=True
                        )
                new_df.rename(columns={"YearofBirth": "DOB"}, inplace=True)
                main_df = pd.concat([main_df, new_df], sort=False)
            return main_df

        def insert_code_bin(arr):
            return [sorted_codes_dict[k]["index"] for k in arr]

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )

        active_files = [f for f in new_files if file_is_active(f["name"])]
        other_files = [f for f in new_files if not file_is_active(f["name"])]

        main_df = pd.DataFrame()
        main_df = add_files_to_main_df(main_df, active_files)
        main_df = add_files_to_main_df(main_df, other_files)
        main_df.reset_index(drop=True, inplace=True)

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

        voter_columns = [c for c in main_df.columns if not c[0].isdigit()]
        history_columns = [c for c in main_df.columns if c[0].isdigit()]

        self.column_check(voter_columns)
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

        # handle history:
        sorted_codes = history_columns[::-1]
        hist_df = main_df[sorted_codes]
        voter_df = main_df[voter_columns]
        counts = (~hist_df.isna()).sum()
        sorted_codes_dict = {
            k: {
                "index": int(i),
                "count": int(counts[i]),
                "date": date_from_str(k),
            }
            for i, k in enumerate(sorted_codes)
        }

        hist_df.loc[:, "vote_codes"] = pd.Series(hist_df.values.tolist())
        hist_df.loc[:, "vote_codes"] = hist_df.loc[:, "vote_codes"].map(
            lambda x: [c for c in x if not pd.isna(c)]
        )
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
        voter_df.loc[:, "all_history"] = voter_df.loc[:, "all_history"].map(
            lambda x: [c for c in x if not pd.isna(c)]
        )
        voter_df.loc[:, "sparse_history"] = voter_df.loc[:, "all_history"].map(
            insert_code_bin
        )

        expected_cols = (
            self.config["ordered_columns"]
            + self.config["ordered_generated_columns"]
        )
        voter_df = self.reconcile_columns(voter_df, expected_cols)
        voter_df = voter_df[expected_cols]

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
