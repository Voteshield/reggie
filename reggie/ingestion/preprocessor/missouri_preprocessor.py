from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime


class PreprocessMissouri(Preprocessor):
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

        # File check doesn't work in this case, it's all over the place
        preferred_files = [
            x
            for x in new_files
            if ("VotersList" in x["name"]) and (".txt" in x["name"])
        ]
        if len(preferred_files) > 0:
            main_file = preferred_files[0]
        else:
            main_file = new_files[0]

        main_df = self.read_csv_count_error_lines(
            main_file["obj"], sep="\t", error_bad_lines=False
        )

        # convert "Voter Status" to "voter_status" for backward compatibility
        main_df.rename(
            columns={"Voter Status": self.config["voter_status"]}, inplace=True
        )

        # add empty column for party_identifier
        main_df[self.config["party_identifier"]] = np.nan

        self.column_check(
            list(set(main_df.columns) - set(self.config["hist_columns"]))
        )

        def add_history(main_df):
            # also save as sparse array since so many elections are stored
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
                    "date": date_from_str(k),
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

        sorted_codes, sorted_codes_dict = add_history(main_df)
        main_df.drop(self.config["hist_columns"], axis=1, inplace=True)

        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_numeric(
            main_df,
            extra_cols=[
                "Residential ZipCode",
                "Mailing ZipCode",
                "Precinct",
                "House Number",
                "Unit Number",
                "Split",
                "Township",
                "Ward",
                "Precinct Name",
            ],
        )

        self.meta = {
            "message": "missouri_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(main_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
