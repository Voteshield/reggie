from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from dateutil import parser
from reggie.ingestion.utils import MissingNumColumnsError, format_column_name
import logging
import pandas as pd
import datetime
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
import numpy as np
from datetime import datetime
import gc
import json


class PreprocessWisconsin(Preprocessor):
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

        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )

        if not self.ignore_checks:
            self.file_check(len(new_files))

        config = config_file
        preferred_files = [x for x in new_files if (".txt" in x["name"])]
        if len(preferred_files) > 0:
            main_file = preferred_files[0]
        else:
            main_file = new_files[0]

        main_df = self.read_csv_count_error_lines(
            main_file["obj"], sep="\t", error_bad_lines=False
        )
        logging.info(
            "dataframe memory usage: {}".format(
                main_df.memory_usage(deep=True).sum()
            )
        )
        # convert "Voter Status" to "voter_status" for backward compatibility
        main_df.rename(
            columns={"Voter Status": self.config["voter_status"]}, inplace=True
        )
        # drop rows with nan values for voterid and county
        main_df.dropna(subset=["Voter Reg Number", "County"], inplace=True)
        gc.collect()
        # dummy columns for party and birthday
        main_df[self.config["party_identifier"]] = np.nan
        main_df[self.config["birthday_identifier"]] = 0

        def parse_histcols(col_name):
            try:
                parser.parse(col_name)
                return True
            except ValueError:
                return False

        # iterate through thethe dataframe, each column election column for wisconsin
        # has a monthname and year
        valid_elections = []
        for column in main_df:
            if parse_histcols(column):
                valid_elections.append(column)
        self.column_check(list(set(main_df.columns) - set(valid_elections)))
        # sort from oldest election available to newest
        valid_elections = sorted(
            valid_elections, key=lambda date: parser.parse(date)
        )

        # election_counts: a pd series of the valid elections the the vote counts per election
        election_counts = main_df[valid_elections].count()
        # returns the decreasing counts of people who voted per election

        # election_counts.index[i] contains the election "name"
        # k contains the count of people who voted in that elections
        sorted_codes_dict = {
            election_counts.index[i]: {
                "index": i,
                "count": k,
                "date": str(
                    datetime.strptime(election_counts.index[i], "%B%Y")
                    .date()
                    .strftime("%m/%d/%Y")
                ),
            }
            for i, k in enumerate(election_counts)
        }

        sorted_codes = list(election_counts.index)

        def get_all_history(row):
            hist = []
            for i, k in row.iteritems():
                if pd.notnull(k):
                    hist.append(i)
            return hist

        def get_type_history(row):
            type_hist = []
            for i, k in row.iteritems():
                if pd.notnull(k):
                    hist = k.replace(" ", "")
                    type_hist.append(hist)
            return type_hist

        def insert_code_bin(row):
            sparse_hist = []
            for i, k in row.iteritems():
                if pd.notnull(k):
                    sparse_hist.append(sorted_codes_dict[i]["index"])
            return sparse_hist

        main_df["sparse_history"] = main_df[valid_elections].apply(
            insert_code_bin, axis=1
        )
        main_df["all_history"] = main_df[valid_elections].apply(
            get_all_history, axis=1
        )
        main_df["votetype_history"] = main_df[valid_elections].apply(
            get_type_history, axis=1
        )

        main_df.drop(columns=valid_elections, inplace=True)
        gc.collect()

        main_df = self.config.coerce_numeric(
            main_df, extra_cols=["HouseNumber", "ZipCode", "UnitNumber"]
        )
        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_strings(main_df)

        self.meta = {
            "message": "wisconsin_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }
        # need to tell it it's not compressed for test files otherwise it creates a malformed object
        # self.is_compressed = False
        logging.info("Wisconsin: writing out")

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(main_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
