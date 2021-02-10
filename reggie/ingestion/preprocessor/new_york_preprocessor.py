from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import strcol_to_array
import datetime
from io import StringIO
import numpy as np
from datetime import datetime
import gc
import json
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

        config = self.config_file
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="infer"
        )

        if not self.ignore_checks:
            self.file_check(len(new_files))

        # no longer include pdfs in file list anyway, can just assign main file
        self.main_file = new_files[0]
        gc.collect()
        # When given the names, the pandas read_csv will always work. If given csv has too few column names it will
        # assign the names to the columns to the end, skipping the beginning columns, if too many will add nan columnms
        main_df = self.read_csv_count_error_lines(
            self.main_file["obj"],
            header=None,
            names=config["ordered_columns"],
            encoding="latin-1",
            error_bad_lines=False,
        )
        logging.info(
            "dataframe memory usage: {}".format(
                main_df.memory_usage(deep=True).sum()
            )
        )

        gc.collect()
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
        gc.collect()

        count_order = counts.argsort()
        unique_codes = unique_codes[count_order]
        counts = counts[count_order]
        sorted_codes = unique_codes.tolist()
        sorted_codes_dict = {
            k: {"index": i, "count": int(counts[i])}
            for i, k in enumerate(sorted_codes)
        }
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
        self.meta = {
            "message": "new_york_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        gc.collect()

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(main_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
