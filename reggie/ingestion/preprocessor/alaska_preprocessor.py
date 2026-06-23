import datetime
import json
import logging

import numpy as np
import pandas as pd

from datetime import datetime
from dateutil import parser
from io import StringIO

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)


class PreprocessAlaska(Preprocessor):
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

        new_files = self.unpack_files(self.main_file, compression="unzip")

        # Filter for csv files only
        new_files = [
            n for n in new_files if ".csv" in n["name"].lower()
        ]

        # Inactive voter file always contains the word "inactive" somewhere
        inactive_file_list = [
            n for n in new_files if "inactive" in n["name"].lower()
        ]

        # In all earlier packages (and some later ones), inactive file is not present
        if len(inactive_file_list) == 0:
            inactive_file = None
            # Active voter file should be the only csv
            active_file = new_files[0]
            logging.info(
                f"Found active voter file: {active_voter_file}"
                f"and no inactive voter file."
            )
        else:
            inactive_file = inactive_file_list[0]
            # Active voter file should be the only other csv
            active_file = [
                n for n in new_files if n != inactive_file["name"]
            ][0]
            logging.info(
                f"Found active voter file: {active_voter_file}"
                f"and inactive voter file: {inactive_voter_file}"
            )

        df_voter = pd.read_csv(active_file["obj"])
        df_voter["STATUS"] = "active"

        if inactive_file:
            df_inactive = pd.read_csv(inactive_file["obj"])
            df_inactive["STATUS"] = "inactive"
            df_voter = pd.concat([df_voter, df_inactive], axis=0)
            df_voter.reset_index(drop=True, inplace=True)

        # Normalize: sometimes column names are missing underscore
        # or are otherwise irregular from file to file
        for c in df_voter.columns:
            df_voter.rename(
                columns={c: c.replace(" ", "_")},
                inplace=True
        )
        df_voter.rename(
            columns=self.config["column_aliases"],
            inplace=True,
        )

        # Add dummy birth date column, to prevent errors
        df_voter["BIRTH_DATE"] = None

        # If no inactive columns, add them, to match 2-file version
        for c in ["CONDITION_DATE", "CC"]:
            if c not in df_voter.columns:
                df[c] = None

        # Split out "state house district" and "precinct" into
        # 2 separate columns.
        # They are contained together in "DP" column.
        df_voter["STATE_HOUSE_DISTRICT"] = df_voter["DP"].str.split("-")[0][0]
        df_voter["PRECINCT"] = df_voter["DP"].str.split("-")[0][1]

        # Party codes N and U both correspond to "no party",
        # so consolidate them both as U.
        df_voter["PARTY"] = df_voter["PARTY"].map(
            lambda x: "U" if x == "N" else x
        )



        # todo finish...


        # --- handling the vote history file --- #

        df_hist = (
            df_hist.set_index(self.config["voter_id"])
            .stack()
            .reset_index()
            .iloc[:, [0, 2]]
        )
        df_hist.columns = [self.config["voter_id"], "election"]

        df_hist = df_hist.join(df_hist.election.str.split(" ", expand=True))
        df_hist = df_hist.rename(
            {0: "all_history", 1: "votetype_history"}, axis=1
        )

        df_hist = df_hist.join(
            df_hist.all_history.str.split("(?<=^\d{2})", expand=True)
        )
        df_hist = df_hist.rename(
            {0: "election_year", 1: "election_type"}, axis=1
        )

        df_hist.election_year = "20" + df_hist.election_year

        elections, counts = np.unique(
            df_hist.loc[:, ["all_history", "election_year"]].apply(
                tuple, axis=1
            ),
            return_counts=True,
        )

        sorted_elections_dict = {
            k[0]: {"index": i, "count": int(counts[i]), "date": int(k[1])}
            for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, "sparse_history"] = df_hist.all_history.map(
            lambda x: sorted_elections_dict[x]["index"]
        )

        df_hist = pd.concat(
            [
                df_hist.groupby(self.config["voter_id"])[c].apply(list)
                for c in ["all_history", "votetype_history", "sparse_history"]
            ],
            axis=1,
        )

        # --- handling the voter file --- #

        df_voter = df_voter.loc[
            :, ~df_voter.columns.isin(self.config["election_columns"])
        ]
        df_voter = df_voter.set_index(self.config["voter_id"])

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.join(df_hist)

        # Check the file for all the proper locales
        self.locale_check(
            set(df_voter[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "alaska_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_elections_dict),
            "array_decoding": json.dumps(sorted_elections),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
