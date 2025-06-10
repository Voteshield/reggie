import datetime
import gc
import json
import logging

from datetime import datetime
from dateutil import parser
from io import StringIO, BytesIO, SEEK_END, SEEK_SET

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import (
    format_column_name,
    MissingNumColumnsError,
)


class PreprocessVermont(Preprocessor):
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

        def hist_map(df, columns):
            def mapping(li):
                li = [x for x in li if x != "nan"]
                return li

            return pd.Series(
                map(mapping, df[columns].values.astype(str).tolist())
            )

        new_files = self.unpack_files(self.main_file, compression="unzip")
        self.file_check(len(new_files))
        
        try:
            voter_file = [
                n
                for n in new_files
                if ("voter file" in n["name"].lower()) or ("statewidevoters" in n["name"].lower())
            ][0]
        except IndexError:
            voter_file = [n for n in new_files if ".csv" in n["name"].lower()][0]
        vdf = pd.read_csv(voter_file["obj"], sep="|", dtype=str)

        # Sometimes, instead of the | seperator it is a tab
        if len(vdf.columns) == 1:
            voter_file["obj"].seek(0)
            vdf = pd.read_csv(voter_file["obj"], sep="\t", dtype=str)
        # And sometimes, it's a CSV
        if len(vdf.columns) == 1:
            voter_file["obj"].seek(0)
            vdf = pd.read_csv(voter_file["obj"], sep=",", dtype=str)
        
        # June 2025, some of the columns have extra single quotes around the column names
        for col in vdf.columns:
            if "'" in col:
                vdf.rename(columns={col: col.strip("'")}, inplace=True)
        
        vdf.rename(columns=self.config["rename_columns"], inplace=True)        
        vdf[self.config["voter_id"]] = vdf[self.config["voter_id"]].str.replace("'", "")
        
        # Note, in June 2025 Vermont added around 50 extra commas to the file
        unnamed_cols = vdf.columns[vdf.columns.str.contains("Unnamed")]
        vdf.drop(columns=unnamed_cols, inplace=True)
        election_columns = [
            col for col in vdf.columns if "election" in col.lower() or "statewide" in col.lower()
        ]
        # added some columns that are new now, so needs reconciliaiton?
        vdf = self.reconcile_columns(vdf, self.config["columns"])
        vdf[self.config["party_identifier"]] = np.nan

        cols_to_check = [x for x in vdf.columns if x not in election_columns]
        self.column_check(cols_to_check)


        # In June of 2025 they changed the format of the file slightly and
        # changed election column names.
        # The new election column names look like: 11/02/2010-2010 STATEWIDE GENERAL
        # when we expect: 2008 Gen Election Participation, to rename them just
        rename_dict = {}
        for election_column in election_columns:
            try:
                election_year = election_column.split("-")[0].split("/")[-1]
                # Check to see if you can make the substring a year
                datetime.datetime.strptime(election_year, "%Y")
                election_string = f"{election_year}_Gen_Election"
                rename_dict[election_column] = election_string
            except ValueError:
                logging.info("election columns in original format")
                break

        # strip the word "participation" and replace spaces with underscores
        # for consistency
        rename_dict = {
            col: col.replace(" Participation", "").replace(" ", "_")
            for col in election_columns
        }

        vdf.rename(columns=rename_dict, inplace=True)

        election_columns = list(rename_dict.values())
        # Replacing the boolean values in the cells with the election name for
        # processing
        for c in list(rename_dict.values()):
            vdf.loc[:, c] = vdf.loc[:, c].map(
                {"T": c.replace(" ", "_"), "F": np.nan}
            )

        # election_counts is a pandas series containing the general elections
        # as an index how many people voted in each general election
        election_counts = vdf[election_columns].count().sort_index()

        # Iterates through the election series, and extracts the information
        # necessary to create metadata, the index is
        sorted_codes_dict = {
            election_counts.index[i]: {
                "index": i,
                "count": k,
                # Vermont only lists general elections, so use an approx date in Nov
                "date": str(
                    datetime.strptime(election_counts.index[i][:4], "%Y")
                    .date()
                    .strftime("11/05/%Y")
                ),
            }
            for i, k in enumerate(election_counts)
        }
        sorted_elections = sorted(list(sorted_codes_dict.keys()))
        vdf["all_history"] = hist_map(vdf[election_columns], election_columns)

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]["index"] for k in arr]
            else:
                return np.nan

        vdf.loc[:, "sparse_history"] = vdf.loc[:, "all_history"].map(
            insert_code_bin
        )

        # In Nov 2022, Vermont started sometimes (but not always) dropping
        # leading zeros that had always existed in their previous data:
        # 1. voter IDs which had always been zero-padded to 9 digits
        # 2. legal/mail address zip codes which had always been full 5 digits
        # To avoid tracking spurious modifications, we zero-pad these back
        # to their normal lengths.
        def pad_zips(z):
            """
            Pad likely zip codes out to 5 digits,
            while skipping NaNs and non-numeric values,
            e.g. some mail zip codes look like "x1j4"
            """
            try:
                return str(int(z)).rjust(5,"0")
            except ValueError:
                return z

        vdf[self.config["voter_id"]] = vdf[self.config["voter_id"]].str.rjust(9,"0")
        vdf["Legal Address Zip"] = vdf["Legal Address Zip"].map(pad_zips)
        vdf["Mailing Address Zip"] = vdf["Mailing Address Zip"].map(pad_zips)

        vdf = vdf.set_index(self.config["voter_id"])

        vdf = self.config.coerce_strings(vdf)
        vdf = self.config.coerce_numeric(vdf)
        vdf = self.config.coerce_dates(vdf)

        # Check the file for all the proper locales
        self.locale_check(
            set(vdf[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "vermont_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_elections),
        }
        logging.info("Processed Vermont")
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(vdf.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
