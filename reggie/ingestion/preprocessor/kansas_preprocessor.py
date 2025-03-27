import logging

from datetime import datetime, date
from dateutil import parser
from io import StringIO

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    FileItem,
    Preprocessor,
    date_from_str,
)
from reggie.ingestion.utils import (
    MissingNumColumnsError,
)


class PreprocessKansas(Preprocessor):
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

        for f in new_files:
            if (
                (".txt" in f["name"])
                and ("._" not in f["name"])
                and ("description" not in f["name"].lower())
            ):
                logging.info("reading kansas file from {}".format(f["name"]))
                df = self.read_csv_count_error_lines(
                    f["obj"],
                    sep="\t",
                    index_col=False,
                    engine="c",
                    on_bad_lines="warn",
                    encoding="latin-1",
                )

        try:
            df.columns = self.config["ordered_columns"]
        except ValueError:
            if len(df.columns) == len(self.config["ordered_columns_new"]):

                # In Nov 2024, Kansas scrambled the column order,
                # although the columns are still the same set
                # as "ordered_columns_new".
                if df.columns[0] != self.config["ordered_columns_new"][0]:
                    # Assume they are scrambled but still the same columns;
                    # Rearrange order.
                    df = df[self.config["ordered_columns_new"]]

            else:
                logging.info("Incorrect number of columns found for Kansas")
                raise MissingNumColumnsError(
                    "{} state is missing columns".format(self.state),
                    self.state,
                    len(self.config["ordered_columns_new"]),
                    len(df.columns),
                )

            for i in set(list(self.config["ordered_columns"])) - set(
                list(self.config["ordered_columns_new"])
            ):
                df[i] = None
        df[self.config["voter_status"]] = df[
            self.config["voter_status"]
        ].str.replace(" ", "")

        def ks_hist_date(s):
            try:
                elect_year = datetime.strptime(s[2:6], "%Y").year
            except:
                elect_year = -1
                pass
            if (elect_year < 1990) or (elect_year > date.today().year + 1):
                elect_year = None
            return elect_year

        def add_history(main_df):
            count_df = pd.DataFrame()
            for idx, hist in enumerate(self.config["hist_columns"]):
                unique_codes, counts = np.unique(
                    main_df[hist].str.replace(" ", "_").dropna().values,
                    return_counts=True,
                )
                count_df_new = pd.DataFrame(
                    index=unique_codes, data=counts, columns=["counts_" + hist]
                )
                count_df = pd.concat([count_df, count_df_new], axis=1)
            count_df["total_counts"] = count_df.sum(axis=1)
            unique_codes = count_df.index.values
            counts = count_df["total_counts"].values
            count_order = counts.argsort()
            unique_codes = unique_codes[count_order]
            counts = counts[count_order]
            sorted_codes = unique_codes.tolist()
            sorted_codes_dict = {
                k: {
                    "index": i,
                    "count": int(counts[i]),
                    "date": ks_hist_date(k),
                }
                for i, k in enumerate(sorted_codes)
            }

            def insert_code_bin(arr):
                return [sorted_codes_dict[k]["index"] for k in arr]

            main_df["all_history"] = main_df[
                self.config["hist_columns"]
            ].apply(lambda x: list(x.dropna().str.replace(" ", "_")), axis=1)
            main_df.all_history = main_df.all_history.map(insert_code_bin)
            return sorted_codes, sorted_codes_dict

        sorted_codes, sorted_codes_dict = add_history(main_df=df)

        df = self.config.coerce_numeric(df)
        df = self.config.coerce_strings(df)
        df = self.config.coerce_dates(df)

        # Check the file for all the proper locales
        self.locale_check(
            set(df[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "kansas_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
