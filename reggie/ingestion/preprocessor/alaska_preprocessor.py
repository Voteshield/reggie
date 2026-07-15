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
                n for n in new_files if n["name"] != inactive_file["name"]
            ][0]
            logging.info(
                f"Found active voter file: {active_file['name']}, "
                f"and inactive voter file: {inactive_file['name']}"
            )

        def normalize_headers(df):
            # Normalize: sometimes column names are missing an
            # underscore or are otherwise irregular from file to file,
            # including varying between the active and inactive file.
            df.rename(
                columns=self.config["column_aliases"],
                inplace=True,
            )
            for c in df.columns:
                df.rename(
                    columns={c: c.replace(" ", "_")},
                    inplace=True
            )
            return df

        df_voter = self.read_csv_count_error_lines(
            active_file["obj"],
            sep=",",
            on_bad_lines="warn",
        )
        df_voter["STATUS"] = "active"
        df_voter = normalize_headers(df_voter)

        if inactive_file:
            df_inactive = self.read_csv_count_error_lines(
                inactive_file["obj"],
                sep=",",
                on_bad_lines="warn",
            )
            df_inactive["STATUS"] = "inactive"
            df_inactive = normalize_headers(df_inactive)
            df_voter = pd.concat([df_voter, df_inactive], axis=0)
            df_voter.reset_index(drop=True, inplace=True)

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
        df_voter["STATE_HOUSE_DISTRICT"] = df_voter["DP"].map(
            lambda x: str(int(x.split("-")[0])) if not pd.isna(x) else x
        )
        df_voter["PRECINCT"] = df_voter["DP"].map(
            lambda x: str(int(x.split("-")[1])) if not pd.isna(x) else x
        )
        df_voter.drop(columns=["DP"], inplace=True)

        # Ensure voter ID and zips are also ints
        df_voter["ASCENSION"] = df_voter["ASCENSION"].map(
            lambda x: str(int(x)) if not pd.isna(x) else x
        )
        df_voter["RESIDENCE_ZIP"] = df_voter["RESIDENCE_ZIP"].map(
            lambda x: str(int(x)) if not pd.isna(x) else x
        )

        # Party codes N and U both correspond to "no party",
        # so consolidate them both as U.
        df_voter["PARTY"] = df_voter["PARTY"].map(
            lambda x: "U" if x == "N" else x
        )

        # Vote history
        hist_columns = [f"VH{x}" for x in range(1,17)]

        # Check that we have ended up with all the expected columns
        self.column_check(list(set(df_voter.columns) - set(hist_columns)))

        # Collect unique elections and their counts,
        # while populating vote history arrays
        elections = {}
        def collect_vote_history(column_data, election_code=True):
            codes = []
            for c in column_data:
                if (not pd.isna(c)) and (c is not None):
                    # Make election codes slightly more reader-friendly
                    if election_code:
                        election = c.split()[0]
                        election = "20" + election[:2] + "_" + election[2:]
                        codes.append(election)
                        if election in elections:
                            elections[election]["count"] += 1
                        else:
                            elections[election] = {}
                            elections[election]["name"] = election
                            elections[election]["count"] = 1
                    else:
                        codes.append(c.split()[1])
            return codes

        df_voter["all_history"] = df_voter[hist_columns].apply(
            lambda x: collect_vote_history(x, election_code=True),
            axis=1,
        )
        df_voter["votetype_history"] = df_voter[hist_columns].apply(
            lambda x: collect_vote_history(x, election_code=False),
            axis=1,
        )
        df_voter.drop(columns=hist_columns, inplace=True)

        # Alaska doesn't provide full election dates, so best we can do is guess:
        # Generals are in early November: Assign Nov 4
        # Alaska primaries are always in mid-August: Assign Aug 15
        # Other local elections we really have no good way of guessing, so let's put them
        # in the middle of the year: Assign July 1
        for election in elections:
            if election.split("_")[1] == "GENR":
                elections[election]["date"] = election.split("_")[0] + "-11-04"
            elif election.split("_")[1] == "PRIM":
                elections[election]["date"] = election.split("_")[0] + "-08-15"
            else:
                elections[election]["date"] = election.split("_")[0] + "-07-01"

        elections = list(elections.values())
        sorted_elections = sorted(elections, key=lambda x: x["date"], reverse=True)
        sorted_elections_dict = {
            elec["name"]: {"index": idx, "count": int(elec["count"]), "date": elec["date"]}
            for idx, elec in enumerate(sorted_elections)
        }
        sorted_elections = [e["name"] for e in sorted_elections]

        df_voter["sparse_history"] = df_voter["all_history"].map(
            lambda x: [sorted_elections_dict[e]["index"] for e in x]
        )

        # coerce_strings is resulting in literal "nan", which doesn't appear
        # to happen in any other state. I'm hesitant to change the general behavior
        # for other states which seem to be working ok, so I'll just prevent it
        # here by replacing np.nan with empty string.
        text_fields = [
            c for c, v in self.config.data["columns"].items()
            if v == "text" or "char" in v
        ]
        for f in text_fields:
            df_voter[f] = df_voter[f].fillna("")

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        # Map cities to boroughs / census areas:
        def borough_lookup(city):
            # There are sometimes up to ~80k null residence cities,
            # due to residence addresses being marked as "private".
            # Also, sometimes city is listed as "federal" or "overseas".
            # In all of these cases, we assign the voter to fictional borough, "unknown".
            if pd.isna(city) or (city is None) or (
                city in ["federal", "fedeal", "overseas", "-", ""]):
                return "unknown"

            if city in self.config["cities_to_boroughs"]:
                return self.config["cities_to_boroughs"][city]
            else:
                # City set seems well-normalized. If a new city appears,
                # we probably need someone to take a look and
                # add it to the borough lookup, or possibly assign it to "unknown".
                raise UnknownCityError(
                    f"Encountered unknown city '{city}' in Alaska, "
                    f"that is not associated with a known borough or census area. "
                    f"Please add {city} to the cities_to_boroughs dictionary in alaska.yaml, "
                    f"and then reprocess this file."
                )

        df_voter["BOROUGH"] = df_voter["RESIDENCE_CITY"].map(borough_lookup)

        # Reorder voter columns into canonical order
        df_voter = df_voter[
            self.config["ordered_columns"] + self.config["ordered_generated_columns"]
        ]

        # Set voter ID as index
        df_voter = df_voter.set_index(self.config["voter_id"])

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
