from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from dateutil import parser
from reggie.ingestion.utils import (
    get_surrounding_dates,
    get_metadata_for_key,
    MissingElectionCodesError,
)
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime


class PreprocessMichigan(Preprocessor):
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

        # config = Config('michigan')
        new_files = self.unpack_files(file_obj=self.main_file)
        if not self.ignore_checks:
            self.file_check(len(new_files))
        voter_file = (
            [
                n
                for n in new_files
                if "entire_state_v" in n["name"]
                or "EntireStateVoters" in n["name"]
            ]
            + [None]
        )[0]
        hist_file = (
            [
                n
                for n in new_files
                if "entire_state_h" in n["name"]
                or "EntireStateVoterHistory" in n["name"]
            ]
            + [None]
        )[0]
        elec_codes = (
            [n for n in new_files if "electionscd" in n["name"]] + [None]
        )[0]

        logging.info("Loading voter file: " + voter_file["name"])
        if voter_file["name"][-3:] == "lst":
            vcolspecs = [
                [0, 35],
                [35, 55],
                [55, 75],
                [75, 78],
                [78, 82],
                [82, 83],
                [83, 91],
                [91, 92],
                [92, 99],
                [99, 103],
                [103, 105],
                [105, 135],
                [135, 141],
                [141, 143],
                [143, 156],
                [156, 191],
                [191, 193],
                [193, 198],
                [198, 248],
                [248, 298],
                [298, 348],
                [348, 398],
                [398, 448],
                [448, 461],
                [461, 463],
                [463, 468],
                [468, 474],
                [474, 479],
                [479, 484],
                [484, 489],
                [489, 494],
                [494, 499],
                [499, 504],
                [504, 510],
                [510, 516],
                [516, 517],
                [517, 519],
            ]
            vdf = pd.read_fwf(
                voter_file["obj"],
                colspecs=vcolspecs,
                names=self.config["fwf_voter_columns"],
                na_filter=False,
            )
        elif voter_file["name"][-3:] == "csv":
            vdf = self.read_csv_count_error_lines(
                voter_file["obj"],
                encoding="latin-1",
                na_filter=False,
                error_bad_lines=False,
            )
            # rename 'STATE' field to not conflict with our 'state' field
            vdf.rename(columns={"STATE": "STATE_ADDR"}, inplace=True)
        else:
            raise NotImplementedError("File format not implemented")

        def column_is_empty(col):
            total_size = col.shape[0]
            if (sum(col.isna()) == total_size) or (sum(col == "")):
                return True
            return False

        def fill_empty_columns(df):
            # Dummy values for newly added data fields
            if column_is_empty(df["STATUS_USER_CODE"]):
                df["STATUS_USER_CODE"] = "_"
            if column_is_empty(df["VOTER_ID"]):
                df["VOTER_ID"] = 0
            if column_is_empty(df["STATUS_DATE"]):
                df["STATUS_DATE"] = "1970-01-01 00:00:00"
            return df

        vdf = self.reconcile_columns(vdf, self.config["columns"])
        vdf = fill_empty_columns(vdf)
        vdf = vdf.reindex(columns=self.config["ordered_columns"])
        vdf[self.config["party_identifier"]] = "npa"

        logging.info("Loading history file: " + hist_file["name"])
        if hist_file["name"][-3:] == "lst":
            hcolspecs = [
                [0, 13],
                [13, 15],
                [15, 20],
                [20, 25],
                [25, 38],
                [38, 39],
            ]
            hdf = pd.read_fwf(
                hist_file["obj"],
                colspecs=hcolspecs,
                names=self.config["fwf_hist_columns"],
                na_filter=False,
            )
        elif hist_file["name"][-3:] == "csv":
            hdf = self.read_csv_count_error_lines(
                hist_file["obj"], na_filter=False, error_bad_lines=False
            )
            if ("IS_ABSENTEE_VOTER" not in hdf.columns) and (
                "IS_PERMANENT_ABSENTEE_VOTER" in hdf.columns
            ):
                hdf.rename(
                    columns={
                        "IS_PERMANENT_ABSENTEE_VOTER": "IS_ABSENTEE_VOTER"
                    },
                    inplace=True,
                )
        else:
            raise NotImplementedError("File format not implemented")

        # If hdf has ELECTION_DATE (new style) instead of ELECTION_CODE,
        # then we don't need to do election code lookups
        elec_code_dict = dict()
        missing_history_dates = False
        if "ELECTION_DATE" in hdf.columns:
            try:
                hdf["ELECTION_NAME"] = pd.to_datetime(
                    hdf["ELECTION_DATE"]
                ).map(lambda x: x.strftime("%Y-%m-%d"))
            except ValueError:
                missing_history_dates = True
                hdf["ELECTION_NAME"] = hdf["ELECTION_DATE"]
        else:
            if elec_codes:
                # If we have election codes in this file
                logging.info(
                    "Loading election codes file: " + elec_codes["name"]
                )
                if elec_codes["name"][-3:] == "lst":
                    ecolspecs = [[0, 13], [13, 21], [21, 46]]
                    edf = pd.read_fwf(
                        elec_codes["obj"],
                        colspecs=ecolspecs,
                        names=self.config["elec_code_columns"],
                        na_filter=False,
                    )
                    edf["Date"] = pd.to_datetime(edf["Date"], format="%m%d%Y")
                elif elec_codes["name"][-3:] == "csv":
                    # I'm not sure if this would actually ever happen
                    edf = self.read_csv_count_error_lines(
                        elec_codes["obj"],
                        names=self.config["elec_code_columns"],
                        na_filter=False,
                        error_bad_lines=False,
                    )
                else:
                    raise NotImplementedError("File format not implemented")

                # make a code dictionary that will be stored with meta data
                for idx, row in edf.iterrows():
                    d = row["Date"].strftime("%Y-%m-%d")
                    elec_code_dict[row["Election_Code"]] = {
                        "Date": d,
                        "Slug": d
                        + "_"
                        + str(row["Election_Code"])
                        + "_"
                        + row["Title"].replace(" ", "-").replace("_", "-"),
                    }
            else:
                # Get election codes from most recent meta data
                this_date = parser.parse(
                    date_from_str(self.raw_s3_file)
                ).date()
                pre_date, post_date, pre_key, post_key = get_surrounding_dates(
                    this_date, self.state, self.s3_bucket, testing=self.testing
                )
                if pre_key is not None:
                    nearest_meta = get_metadata_for_key(
                        pre_key, self.s3_bucket
                    )
                    elec_code_dict = nearest_meta["elec_code_dict"]
                    if len(elec_code_dict) == 0:
                        raise MissingElectionCodesError(
                            "No election codes in nearby meta data."
                        )
                else:
                    raise MissingElectionCodesError(
                        "No election code file or nearby meta data found."
                    )

            # Election code lookup
            hdf["ELECTION_NAME"] = hdf["ELECTION_CODE"].map(
                lambda x: elec_code_dict[str(x)]["Slug"]
                if str(x) in elec_code_dict
                else str(x)
            )

        # Create meta data
        counts = hdf["ELECTION_NAME"].value_counts()
        counts.sort_index(inplace=True)
        sorted_codes = counts.index.to_list()
        sorted_codes_dict = {
            k: {"index": i, "count": int(counts[i]), "date": date_from_str(k)}
            for i, k in enumerate(sorted_codes)
        }

        # Collect histories
        vdf.set_index(self.config["voter_id"], drop=False, inplace=True)
        hdf_id_groups = hdf.groupby(self.config["voter_id"])
        vdf["all_history"] = hdf_id_groups["ELECTION_NAME"].apply(list)
        vdf["votetype_history"] = hdf_id_groups["IS_ABSENTEE_VOTER"].apply(
            list
        )
        vdf["county_history"] = hdf_id_groups["COUNTY_CODE"].apply(list)
        vdf["jurisdiction_history"] = hdf_id_groups["JURISDICTION_CODE"].apply(
            list
        )
        vdf["schooldistrict_history"] = hdf_id_groups[
            "SCHOOL_DISTRICT_CODE"
        ].apply(list)

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [
                    sorted_codes_dict[k]["index"]
                    for k in arr
                    if k in sorted_codes_dict
                ]
            else:
                return np.nan

        vdf["sparse_history"] = vdf["all_history"].map(insert_code_bin)

        if missing_history_dates:
            vdf["all_history"] = None
            vdf["sparse_history"] = None

        vdf = self.config.coerce_dates(vdf)
        vdf = self.config.coerce_numeric(
            vdf,
            extra_cols=[
                "PRECINCT",
                "WARD",
                "VILLAGE_PRECINCT",
                "SCHOOL_PRECINCT",
            ],
        )
        vdf = self.config.coerce_strings(vdf)

        self.meta = {
            "message": "michigan_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
            "elec_code_dict": elec_code_dict,
        }
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(vdf.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
