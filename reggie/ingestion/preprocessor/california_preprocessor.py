from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
)
from reggie.ingestion.utils import format_column_name
from reggie.configs.configs import Config
import logging
import pandas as pd
import datetime
from io import StringIO
from datetime import datetime
from dateutil import parser
from collections import defaultdict
import json
import time
from collections import defaultdict

"""
The california File Comes in 3 files

Big todo:
Create hist stuff first, then del from memory to free room to
Join district info in


Use ensure int string where necessary otherwise you have fun float string problems
"""


class PreprocessCalifornia(Preprocessor):
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

        config = Config(file_name=self.config_file)
        new_files = self.unpack_files(file_obj=self.main_file)
        del self.main_file, self.temp_files
        # Have to use longer whole string not just suffix because hist will match to voter file
        voter_file = [f for f in new_files if "pvrdr-vrd" in f["name"]][0]
        district_file = [f for f in new_files if "pvrdr-pd" in f["name"]][0]
        history_file = [f for f in new_files if "pvrdr-vph" in f["name"]][0]

        # chunksize
        chunk_size = 3000000
        # chunk_size = 36 * 1024 * 1024  # for dask in mb
        # Todo: Remove, diagnostic only accurate for only one file
        rows = 213637309
        num_chunks = rows // chunk_size + 2

        # Diagnostic
        voter_size = voter_file["obj"].__sizeof__()
        history_size = history_file["obj"].__sizeof__()
        district_size = district_file["obj"].__sizeof__()
        logging.info(
            "Reading In files: voter_size {}\n history_size {} \n district_size{} \n total: {}".format(
                voter_size // 1023 ** 3,
                history_size // 1023 ** 3,
                district_size // 1023 ** 3,
                (voter_size + history_size + district_size) // 1023 ** 3,
            )
        )
        temp_voter_id_df = pd.read_csv(
            voter_file["obj"],
            sep="\t",
            encoding="latin-1",
            usecols=["RegistrantID"],
            dtype=str,
        )
        voter_ids = temp_voter_id_df["RegistrantID"].unique().tolist()
        del temp_voter_id_df
        hist_dict = {i: [] for i in voter_ids}
        del voter_ids
        elect_dict = defaultdict(int)

        def dict_cols(chunk, history_dict=None, election_dict=None):
            chunk["combined_col"] = (
                chunk["ElectionType"].replace(" ", "")
                + "_"
                + chunk["ElectionDate"]
                + "_"
                + chunk["Method"]
            )
            chunk["election"] = (
                chunk["ElectionType"].replace(" ", "")
                + "_"
                + chunk["ElectionDate"]
            )
            chunk.drop(
                columns=[
                    "ElectionType",
                    "ElectionName",
                    "ElectionDate",
                    "CountyCode",
                    "Method",
                ],
                inplace=True,
            )
            for row in chunk.itertuples():
                try:
                    current_li = hist_dict[row.RegistrantID]
                    # throws key error for entries not in voter file
                    election_dict[row.election] += 1
                    combined_row = row.combined_col
                    if isinstance(current_li, list):
                        current_li.append(combined_row)
                        history_dict[row.RegistrantID] = current_li
                    else:
                        # test_dict[row['RegistrantID']][0]
                        history_dict[row.RegistrantID] = [
                            combined_row
                        ]  # Create list of elections even if len 1
                except KeyError:
                    continue

            # return history_dict, election_dict

        history_chunks = pd.read_csv(
            history_file["obj"],
            sep="\t",
            usecols=[
                "RegistrantID",
                "CountyCode",
                "ElectionDate",
                "ElectionName",
                "ElectionType",
                "Method",
            ],
            dtype=str,
            chunksize=chunk_size,
        )
        progress_tracker = 0
        for chunk in history_chunks:
            progress_tracker += 1
            logging.info("Chunk {}/{}".format(progress_tracker, num_chunks))
            start_t = time.time()
            dict_cols(chunk, hist_dict, elect_dict)
            end_time = time.time()
            logging.info(
                "time_elapsed: {}".format(round((end_time - start_t), 2))
            )
            time_remaining = round(
                ((end_time - start_t) * (num_chunks - progress_tracker) / 60),
                2,
            )
            logging.info(
                "time more or less remaining {}".format(time_remaining)
            )
        del history_file

        # index will be voterids
        logging.info("df creation")
        hist_df = pd.DataFrame.from_dict(hist_dict, orient="index", columns=['all_history'])
        # logging.info(
        #     "dataframe memory usage: {}".format(
        #         hist_df.memory_usage(deep=True).sum() // 1024 ** 3
        #     )
        # )
        del hist_dict
        csv_hist = hist_df.to_csv(encoding="utf-8", index=False)
        del hist_df
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_hist),
            s3_bucket=self.s3_bucket,
        )
