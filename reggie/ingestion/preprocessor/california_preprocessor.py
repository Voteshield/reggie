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
import dask.dataframe as dd

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
        # chunk_size = 3000000
        chunk_size = 36 * 1024 * 1024  # for dask in mb

        # Diagnostic
        voter_size = voter_file["obj"].__sizeof__()
        history_size = history_file["obj"].__sizeof__()
        district_size = district_file["obj"].__sizeof__()
        logging.info(
            "Reading In files: voter_size {}\n history_size {} \n district_size{} \n total: {}".format(
                voter_size,
                history_size,
                district_size,
                voter_size + history_size + district_size,
            )
        )
        # temp_voter_id_df = pd.read_csv(
        #     voter_file["obj"],
        #     sep="\t",
        #     encoding="latin-1",
        #     usecols=["RegistrantID"],
        #     dtype=str,
        # )
        # voter_ids = temp_voter_id_df["RegistrantID"].unique().tolist()
        # del temp_voter_id_df
        # hist_dict = {i: [] for i in voter_ids}
        # del voter_ids
        # elect_dict = defaultdict(int)
        #
        # def dict_cols(chunk, history_dict=None, election_dict=None):
        #     chunk["combined_col"] = (
        #         chunk["ElectionType"].replace(" ", "")
        #         + "_"
        #         + chunk["ElectionDate"]
        #         + "_"
        #         + chunk["Method"]
        #     )
        #     chunk["election"] = (
        #         chunk["ElectionType"].replace(" ", "")
        #         + "_"
        #         + chunk["ElectionDate"]
        #     )
        #     chunk.drop(
        #         columns=[
        #             "ElectionType",
        #             "ElectionName",
        #             "ElectionDate",
        #             "CountyCode",
        #             "Method",
        #         ],
        #         inplace=True,
        #     )
        #     for index, row in chunk.iterrows():
        #         try:
        #             current_li = hist_dict[row["RegistrantID"]]
        #             combined_row = row["combined_col"]
        #             current_li.append(combined_row)
        #             history_dict[row["RegistrantID"]] = current_li
        #             election_dict[row["election"]] += 1
        #         except KeyError:
        #             continue
        #
        #     # return history_dict, election_dict
        #
        # history_chunks = pd.read_csv(
        #     history_file["obj"],
        #     sep="\t",
        #     usecols=[
        #         "RegistrantID",
        #         "CountyCode",
        #         "ElectionDate",
        #         "ElectionName",
        #         "ElectionType",
        #         "Method",
        #     ],
        #     dtype=str,
        #     chunksize=chunk_size,
        # )
        # for chunk in history_chunks:
        #     start_t = time.time()
        #     dict_cols(chunk, hist_dict, elect_dict)
        #     end_time = time.time()
        #     logging.info("time_elapsed: {}".format(end_time - start_t))
        #
        def dask_test(chunk):
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
            chunk = chunk.drop(
                columns=[
                    "ElectionType",
                    "ElectionName",
                    "ElectionDate",
                    "CountyCode",
                    "Method",
                ]
            )
            return chunk

        df = dd.read_csv(
            "/home/tommi/Downloads/CA 2020-12-23/quarterhist.txt",
            sep="\t",
            blocksize=chunk_size,
            usecols=[
                "RegistrantID",
                "CountyCode",
                "ElectionDate",
                "ElectionName",
                "ElectionType",
                "Method",
            ],
            dtype=str,
        )

        result = dask_test(df)
        start_t = time.time()
        logging.info("starting")
        result = result.compute(num_workers=4)
        end_time = time.time()
        print("time_elapsed: ", end_time - start_t)
        del history_file
        csv_hist = result.to_csv(encoding="utf-8", index=False)
        # logging.info("test_dict complete")
        del result
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_hist),
            s3_bucket=self.s3_bucket,
        )
