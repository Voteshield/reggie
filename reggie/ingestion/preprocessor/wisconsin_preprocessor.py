from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
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

        # logging.info("buffer size: {}".format(main_file["obj"].__sizeof__()))
        rawdata = main_file["obj"].read(100000)
        result = chardet.detect(rawdata)
        encoding_result = result['encoding']

        if encoding_result == 'ascii':
            # I have no idea why I keep needing to do this?
            encoding_result = 'latin-1'
        logging.info("encoding: {}".format(encoding_result))
        main_file["obj"].seek(0)
        # todo: add the other format here
        # todo: try and except this because there are so many encodings
        if encoding_result != 'latin-1':
            try:
                wi_columns = pd.read_csv(
                    main_file["obj"], sep="\t", nrows=0
                ).columns.tolist()
            except UnicodeDecodeError:
                logging.info('unicode error in the latin-1?')
                main_file["obj"].seek(0)
                wi_columns = pd.read_csv(
                    main_file["obj"], sep=",", nrows=0, encoding='latin-1'
                ).columns.tolist()
        else:
            main_file["obj"].seek(0)
            try:
                wi_columns = pd.read_csv(
                    main_file["obj"], sep=",", encoding='latin-1', nrows=0
                ).columns.tolist()
            except UnicodeDecodeError:
                logging.info('in the exception')
                main_file["obj"].seek(0)
                wi_columns = pd.read_csv(
                    main_file["obj"], sep="\t", nrows=0
                ).columns.tolist()
            if len(wi_columns) == 1:
                logging.info('incorrect separator, but which encoding?')
                main_file["obj"].seek(0)
                wi_columns = pd.read_csv(
                    main_file["obj"], sep="\t", nrows=0, encoding='latin-1'
                ).columns.tolist()
        # wi_columns = pd.read_csv(
        #     main_file["obj"], sep="\t", nrows=0
        # ).columns.tolist()
        main_file["obj"].seek(0)
        # Todo: add to yaml instead of here,
        cat_columns = [
            "Suffix",
            "UnitType",
            "Congressional",
            "State Senate",
            "Court of Appeals",
            "Jurisdiction",
            "High School",
            "Technical College",
            "Representational School",
            "State",
            "First Class School",
            "Incorporation",
            "voter_status",
            "Voter Status Reason",
            "Voter Type",
            "IsPermanentAbsentee",
        ]

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
        dtype_dict = {
            col: ("str" if col not in cat_columns else "category")
            for col in wi_columns
        }

        # Wisconsin comes in two slightly different formats
        try:
            main_df = self.read_csv_count_error_lines(
                main_file["obj"],
                sep="\t",
                dtype=dtype_dict,
                error_bad_lines=False,
            )
        except UnicodeDecodeError:
            main_file["obj"].seek(0)
            main_df = self.read_csv_count_error_lines(
                main_file["obj"],
                sep=",",
                encoding=encoding_result,
                dtype=dtype_dict,
                error_bad_lines=False,
            )
        del self.main_file, self.temp_files, new_files
        gc.collect()

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

        # standardize La Crosse County and rename it to La Crosse County
        # Todo: Check for other places/spellings might need regex
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

        # self.column_check(list(set(main_df.columns) - set(valid_elections)))
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
        # need to tell it it's not compressed for test files otherwise it creates a malformed object
        # self.is_compressed = False
        logging.info("Wisconsin: writing out")
        # snapshot = tracemalloc.take_snapshot()
        # for stat in snapshot.statistics("lineno"):
        #     logging.info(stat)
        df_csv = main_df.to_csv(encoding="utf-8", index=False)
        del main_df
        gc.collect()

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_csv),
            s3_bucket=self.s3_bucket,
        )
        del df_csv
        gc.collect()
