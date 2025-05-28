import datetime
import json
import logging

from datetime import datetime
from io import StringIO

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


class PreprocessMaine(Preprocessor):
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
        # self.file_check(len(new_files))
        voter_df = pd.DataFrame()
        for file in new_files:
            if "voter" in file:
                logging.info("voter file found")
                voter_df = self.read_csv_count_error_lines(
                    file["obj"], sep="|", dtype="str", on_bad_lines="warn"
                )
                # for some reason party exists in the cancelled file but not here
                voter_df[self.config["party_identifier"]] = np.nan
            elif "history" in file["name"].lower():
                logging.info("vote history found")
                hist_df = self.read_csv_count_error_lines(
                    file["obj"], sep="|", dtype="str", on_bad_lines="warn"
                )
            elif "cancelled" in file["name"].lower():
                logging.info("vote history found")
                # cancelled file does not have county...for some reason. 
                canncelled_df= self.read_csv_count_error_lines(
                    file["obj"], sep="|", dtype="str", on_bad_lines="warn"
                )
                
        if canncelled_df:
            # For Some reason there are no counties in the cancelled df file
            # Derive them from zip codes found in main file?
            zip_dict = dict(zip(voter_df['ZIP'], voter_df['CTY']))
            canncelled_df['CTY'] = canncelled_df['Zip5'].map(zip_dict)
        # there are about 5 entries in the cancelled file, that have an active 
        # status in the main file for some reason. 
        
        # todo: merge them here somehow

        unnamed_cols = voter_df.columns[voter_df.columns.str.contains("Unnamed")]
        voter_df.drop(columns=unnamed_cols, inplace=True)


        cols_to_check = [x for x in voter_df.columns if x not in hist_df.columns]
        self.column_check(cols_to_check)

        voter_df = voter_df.set_index(self.config["voter_id"])

        voter_df = self.config.coerce_strings(voter_df)
        voter_df = self.config.coerce_numeric(voter_df)
        voter_df = self.config.coerce_dates(voter_df)

        # Check the file for all the proper locales
        # self.locale_check(
        #     set(voter_df[self.config["primary_locale_identifier"]]),
        # )

        # self.meta = {
        #     "message": "vermont_{}".format(datetime.now().isoformat()),
        #     "array_encoding": json.dumps(sorted_codes_dict),
        #     "array_decoding": json.dumps(sorted_elections),
        # }
        logging.info("Processed Maine")
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(voter_df.to_csv(encoding="utf-8", index=True)),
            s3_bucket=self.s3_bucket,
        )
