from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import collect_garbage
from dateutil import parser
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime
import gc
import chardet


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
        self.processed_file = None

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )

        if not self.ignore_checks:
            self.file_check(len(new_files))

        preferred_files = [x for x in new_files if (".txt" in x["name"])]
        if len(preferred_files) > 0:
            main_file = preferred_files[0]
        else:
            main_file = new_files[0]

        chardata = main_file["obj"].read(100000)
        result = chardet.detect(chardata)
        encoding_result = result["encoding"]
        main_file["obj"].seek(0)
        logging.info("encoding: {}".format(encoding_result))

        if encoding_result == "ascii":
            # the ascii format causes an pandas decoding error, latin-1 is the correct encoding
            encoding_result = "latin-1"
        else:
            # utf-8 is the pandas default and is the encoding for some files, usually the comma-separated files but not
            # always
            encoding_result = "utf-8"
        file_sep = "\t"
        # Some files are tab separated and some are comma separated,
        # if encoding_result != 'latin-1':
        try:
            wi_columns = pd.read_csv(
                main_file["obj"],
                sep=file_sep,
                nrows=0,
                encoding=encoding_result,
            ).columns.tolist()
        except UnicodeDecodeError:
            # Attempt Latin-1
            main_file["obj"].seek(0)
            wi_columns = pd.read_csv(
                main_file["obj"], sep=file_sep, nrows=0, encoding="latin-1"
            ).columns.tolist()
            encoding_result = "latin-1"

        # If the number of columns is only 1, the wrong was used and it is (probably) a csv file, read in the correct
        # columns with the encoding that works
        if len(wi_columns) == 1:
            main_file["obj"].seek(0)
            file_sep = ","
            wi_columns = pd.read_csv(
                main_file["obj"],
                sep=file_sep,
                nrows=0,
                encoding=encoding_result,
            ).columns.tolist()

        main_file["obj"].seek(0)
        cat_columns = self.config["categorical_columns"]

        # Helper function to determine if a column is a history column by checking it for a date
        def parse_histcols(col_name):
            try:
                parser.parse(col_name)
                return True
            except ValueError:
                return False

        # iterate through the dataframe, each column election column for wisconsin
        # has a monthname and year
        valid_elections = []
        for column in wi_columns:
            if parse_histcols(column):
                valid_elections.append(column)

        cat_columns.extend(valid_elections)

        # Specify categorical columns to save memory
        dtype_dict = {
            col: ("str" if col not in cat_columns else "category")
            for col in wi_columns
        }

        # Wisconsin comes in two slightly different formats
        main_df = self.read_csv_count_error_lines(
            main_file["obj"],
            sep=file_sep,
            encoding=encoding_result,
            dtype=dtype_dict,
            error_bad_lines=False,
        )
        collect_garbage([self.main_file, self.temp_files, new_files])

        # convert "Voter Status" to "voter_status" for backward compatibility
        main_df.rename(
            columns={"Voter Status": self.config["voter_status"]}, inplace=True
        )
        # drop rows with nan values for voterid and county
        main_df.dropna(subset=["Voter Reg Number", "County"], inplace=True)

        # remove the non digit voterid's to account for corrupted data (ie dates or names that wound up in the voter
        # id column
        main_df = main_df[
            main_df["Voter Reg Number"].astype(str).str.isdigit()
        ]

        # standardize LaCrosse County and rename it to La Crosse County
        main_df.loc[
            main_df["County"].str.lower() == "lacrosse county", "County"
        ] = "La Crosse County"
        gc.collect()
        # dummy columns for party and birthday
        main_df[self.config["party_identifier"]] = np.nan
        main_df[self.config["birthday_identifier"]] = np.datetime64("NaT")

        logging.info(
            "dataframe memory usage: {}".format(
                main_df.memory_usage(deep=True).sum() // 1024 ** 3
            )
        )

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

        def insert_codes_bin(row):
            sparse_hist = []
            votetype_hist = []
            all_hist = []
            for i, k in row.iteritems():
                if pd.notnull(k):
                    sparse_hist.append(sorted_codes_dict[i]["index"])
                    type_hist = k.replace(" ", "")
                    votetype_hist.append(type_hist)
                    all_hist.append(i)
            return sparse_hist, votetype_hist, all_hist

        main_df[
            ["sparse_history", "votetype_history", "all_history"]
        ] = main_df[valid_elections].apply(
            insert_codes_bin, axis=1, result_type="expand"
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

        logging.info("Wisconsin: writing out")
        df_csv = main_df.to_csv(encoding="utf-8", index=False)
        collect_garbage([main_df])

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_csv),
            s3_bucket=self.s3_bucket,
        )
        collect_garbage([df_csv])
