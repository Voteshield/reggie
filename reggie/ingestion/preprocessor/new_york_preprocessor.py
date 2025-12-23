import datetime
import gc
import json

from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    FileItem,
    Preprocessor,
    date_from_str,
)
from reggie.ingestion.utils import (
    strcol_to_array,
)
from reggie.reggie_constants import *


class PreprocessNewYork(Preprocessor):
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
        self.config_file = config_file

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="infer"
        )
        del self.main_file, self.temp_files
        gc.collect()

        # As of December 2025, NY is now providing an additional
        # purged file that we request, so now there are either 1
        # or 2 files. We can't handle multiple sizes currently in
        # file_check.
        # TODO: Make file_check more flexible.
        #if not self.ignore_checks:
        #    self.file_check(len(new_files))

        new_files = [n for n in new_files if ".txt" in n["name"].lower()]
        main_df = pd.DataFrame()
        for new_file in new_files:
            new_df = self.read_csv_count_error_lines(
                new_file["obj"],
                header=None,
                encoding="latin-1",
                on_bad_lines="warn",
            )
            main_df = pd.concat([main_df, new_df], axis=0, ignore_index=True)
        del new_files, new_df
        gc.collect()

        # In Dec 2021, NY added 2 columns (RAPARTMENTTYPE, RADDRNONSTD),
        # and rearranged the other address columns slightly.
        # Since these columns are not very useful /
        # affect only a small set of voters (140k),
        # We are going to drop them for now to allow for
        # easy compatibility with past data.
        if len(main_df.columns) > len(self.config["ordered_columns"]):
            main_df.drop(main_df.columns[11], axis=1, inplace=True)
            main_df.drop(main_df.columns[9], axis=1, inplace=True)

            # Rearrange columns slightly to fit pre-Dec 2021 data
            ordered_cols = main_df.columns.to_list()
            ordered_cols = ordered_cols[:6] + [ordered_cols[9]] + ordered_cols[6:9] + ordered_cols[10:]
            main_df = main_df[ordered_cols]

            # They also changed some of the status codes to abbreviations
            # So this maps them back to what they used to be
            main_df[41] = main_df[41].map(
                self.config["status_codes_remap"]
            )

        # apply column names
        if len(main_df.columns) == 47:
            main_df.drop(columns=main_df.columns[-2:], inplace=True)
        main_df.columns = self.config["ordered_columns"]

        logging.info(
            "dataframe memory usage: {}".format(
                main_df.memory_usage(deep=True).sum()
            )
        )

        null_hists = main_df.voterhistory != main_df.voterhistory
        main_df.voterhistory[null_hists] = NULL_CHAR
        all_codes = (
            main_df.voterhistory.str.replace(" ", "_")
            .str.replace("[", "")
            .str.replace("]", "")
        )
        all_codes = all_codes.str.cat(sep=";")
        all_codes = np.array(all_codes.split(";"))
        logging.info("Making all_history")
        main_df["all_history"] = strcol_to_array(
            main_df.voterhistory, delim=";"
        )
        unique_codes, counts = np.unique(all_codes, return_counts=True)
        del all_codes, null_hists
        gc.collect()

        count_order = counts.argsort()
        unique_codes = unique_codes[count_order]
        counts = counts[count_order]
        sorted_codes = unique_codes.tolist()
        del unique_codes, count_order
        gc.collect()

        sorted_codes_dict = {
            k: {"index": i, "count": int(counts[i])}
            for i, k in enumerate(sorted_codes)
        }
        del counts
        gc.collect()

        def insert_code_bin(arr):
            return [sorted_codes_dict[k]["index"] for k in arr]

        # in this case we save ny as sparse array since so many elections are
        # stored
        logging.info("Mapping history codes")
        main_df.all_history = main_df.all_history.map(insert_code_bin)

        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_strings(main_df)
        main_df = self.config.coerce_numeric(
            main_df,
            extra_cols=[
                "raddnumber",
                "rhalfcode",
                "rapartment",
                "rzip5",
                "rzip4",
                "mailadd4",
                "ward",
                "countyvrnumber",
                "lastvoteddate",
                "prevyearvoted",
                "prevcounty",
            ],
        )

        # Check the file for all the proper locales
        def locale_convert_str(x):
            try:
                if np.isnan(x):
                    return x
                else:
                    return str(int(float(x)))
            except:
                return x

        main_df[self.config["primary_locale_identifier"]] = main_df[
            self.config["primary_locale_identifier"]
        ].map(locale_convert_str)
        self.locale_check(
            set(main_df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "new_york_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        del sorted_codes, sorted_codes_dict
        gc.collect()

        csv_obj = main_df.to_csv(encoding="utf-8", index=False)
        del main_df
        gc.collect()

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_obj),
            s3_bucket=self.s3_bucket,
        )
        del csv_obj
        gc.collect()
