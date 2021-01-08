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


class PreprocessNewJersey(Preprocessor):
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

        new_files = self.unpack_files(file_obj=self.main_file)
        config = config_file
        voter_files = [n for n in new_files if "AlphaVoter" in n["name"]]

        hist_files = [n for n in new_files if "History" in n["name"]]
        vdf = pd.DataFrame()
        hdf = pd.DataFrame()
        for f in voter_files:
            logging.info("Reading " + f["name"])
            new_df = self.read_csv_count_error_lines(
                f["obj"],
                sep="|",
                names=config["ordered_columns"],
                low_memory=False,
                error_bad_lines=False,
            )
            new_df = self.config.coerce_dates(new_df)
            new_df = self.config.coerce_numeric(
                new_df, extra_cols=["regional_school", "fire", "apt_no"]
            )
            vdf = pd.concat([vdf, new_df], axis=0)
        for f in hist_files:
            logging.info("Reading " + f["name"])
            new_df = self.read_csv_count_error_lines(
                f["obj"],
                sep="|",
                names=config["hist_columns"],
                index_col=False,
                low_memory=False,
                error_bad_lines=False,
            )
            new_df = self.config.coerce_numeric(
                new_df, col_list="hist_columns_type"
            )
            hdf = pd.concat([hdf, new_df], axis=0)

        hdf["election_name"] = (
            hdf["election_name"] + " " + hdf["election_date"]
        )
        hdf = self.config.coerce_dates(hdf, col_list="hist_columns_type")
        hdf.sort_values("election_date", inplace=True)
        hdf = hdf.dropna(subset=["election_name"])
        hdf = hdf.reset_index()
        elections = hdf["election_name"].unique().tolist()
        counts = hdf["election_name"].value_counts()
        elec_dict = {
            k: {"index": i, "count": int(counts.loc[k]) if k in counts else 0}
            for i, k in enumerate(elections)
        }
        vdf["unabridged_status"] = vdf["status"]
        vdf.loc[
            (vdf["status"] == "Inactive Confirmation")
            | (vdf["status"] == "Inactive Confirmation-Need ID"),
            "status",
        ] = "Inactive"
        vdf["tmp_id"] = vdf["voter_id"]
        vdf = vdf.set_index("tmp_id")

        hdf_id_group = hdf.groupby("voter_id")
        logging.info("Creating all_history array")
        vdf["all_history"] = hdf_id_group["election_name"].apply(list)
        logging.info("Creating party_history array")
        vdf["party_history"] = hdf_id_group["party_code"].apply(list)

        def insert_code_bin(arr):
            if arr is np.nan:
                return []
            else:
                return [elec_dict[k]["index"] for k in arr]

        vdf["sparse_history"] = vdf["all_history"].apply(insert_code_bin)
        vdf.loc[
            vdf[self.config["birthday_identifier"]]
            < pd.to_datetime("1900-01-01"),
            self.config["birthday_identifier"],
        ] = pd.NaT

        self.meta = {
            "message": "new_jersey_{}".format(datetime.now().isoformat()),
            "array_encoding": elec_dict,
            "array_decoding": elections,
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(vdf.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
