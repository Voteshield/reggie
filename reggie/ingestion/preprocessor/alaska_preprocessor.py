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

from reggie.ingestion.utils import (
    UnknownCityError,
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
                f"Found active voter file: {active_file['name']}, "
                f"and no inactive voter file."
            )
        else:
            inactive_file = inactive_file_list[0]
            # Active voter file should be the only other csv
            active_file = [
                n for n in new_files if n != inactive_file["name"]
            ][0]
            logging.info(
                f"Found active voter file: {active_file['name']}, "
                f"and inactive voter file: {inactive_file['name']}"
            )

        df_voter = self.read_csv_count_error_lines(
            active_file["obj"],
            sep=",",
            on_bad_lines="warn",
        )
        df_voter["STATUS"] = "active"

        if inactive_file:
            df_inactive = self.read_csv_count_error_lines(
                inactive_file["obj"],
                sep=",",
                on_bad_lines="warn",
            )
            df_inactive["STATUS"] = "inactive"
            df_voter = pd.concat([df_voter, df_inactive], axis=0)
            df_voter.reset_index(drop=True, inplace=True)

        # Normalize: sometimes column names are missing underscore
        # or are otherwise irregular from file to file
        df_voter.rename(
            columns=self.config["column_aliases"],
            inplace=True,
        )
        for c in df_voter.columns:
            df_voter.rename(
                columns={c: c.replace(" ", "_")},
                inplace=True
        )

        # Add dummy birth date column, to prevent errors
        df_voter["BIRTH_DATE"] = None

        # Add borough column, which we will populate with a lookup later
        df_voter["BOROUGH"] = None

        # If no inactive columns, add them, to match 2-file version
        for c in ["CONDITION_DATE", "CC"]:
            if c not in df_voter.columns:
                df_voter[c] = None

        # Split out "state house district" and "precinct" into
        # 2 separate columns.
        # They are contained together in "DP" column.
        df_voter["STATE_HOUSE_DISTRICT"] = df_voter["DP"].str.split("-")[0][0]
        df_voter["STATE_HOUSE_DISTRICT"] = df_voter["STATE_HOUSE_DISTRICT"].map(lambda x: str(int(x)))
        df_voter["PRECINCT"] = df_voter["DP"].str.split("-")[0][1]
        df_voter["PRECINCT"] = df_voter["PRECINCT"].map(lambda x: str(int(x)))
        df_voter.drop(columns=["DP"], inplace=True)

        # Party codes N and U both correspond to "no party",
        # so consolidate them both as U.
        df_voter["PARTY"] = df_voter["PARTY"].map(
            lambda x: "U" if x == "N" else x
        )

        # Vote history
        hist_columns = [f"VH{x}" for x in range(1,17)]

        # Check that we have ended up with all the expected columns
        self.column_check(list(set(df_voter.columns) - set(hist_columns)))

        def collect_history_codes(column_data, election_code=True):
            codes = []
            for c in column_data:
                if (c is not np.nan) and (c is not None):
                    # Make election codes slightly more reader-friendly
                    if election_code:
                        elec = c.split()[0]
                        codes.append("20" + elec[:2] + "_" + elec[2:])
                    else:
                        codes.append(c.split()[1])
            return codes

        df_voter["all_history"] = df_voter[hist_columns].apply(
            lambda x: collect_history_codes(x, election_code=True),
            axis=1,
        )
        df_voter["votetype_history"] = df_voter[hist_columns].apply(
            lambda x: collect_history_codes(x, election_code=False),
            axis=1,
        )


        # Alaska primary is always mid-August, so assign Aug 15 for primary dates
        # All other elections, we have no way of knowing, so stick them in the middle of the year ? July 1 ?

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




        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        # Map cities to boroughs / census areas:
        def borough_lookup(row):
            city = row["RESIDENCE_CITY"]
            # There are ~50k null residence cities, but almost no null mailing cities.
            # Fallback to mailing city if residence city is unusable
            if (row["RESIDENCE_CITY"] is np.nan) or (row["RESIDENCE_CITY"] is None) or (
                row["RESIDENCE_CITY"] in ['federal', 'fedeal', 'overseas', '-']):
                city = row["MAILING_CITY"]
            if (city is np.nan) or (city is None) or (
                city in ['federal', 'fedeal', 'overseas', '-']):
                return np.nan
            if city in self.config["cities_to_boroughs"]:
                return self.config["cities_to_boroughs"][city]
            else:
                raise UnknownCityError(
                    f"Encountered unknown city ("{city}") in Alaska, "
                    f"that is not associated with a known borough or census area. "
                    f"Please add {city} to the cities_to_boroughs dictionary in alaska.yaml, "
                    f"and then reprocess this file."
                )

        df_voter["BOROUGH"] = df_voter[["RESIDENCE_CITY", "MAILING_CITY"]].apply(
            borough_lookup, axis=1
        )

        # Reorder voter columns
        df_voter = [
            self.config(["ordered_columns"]) + self.config(
                ["ordered_generated_columns"]
            )
        ]

        # Set voter ID as index
        df_voter = df_voter.set_index(self.config["voter_id"])

        self.meta = {
            "message": "alaska_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
