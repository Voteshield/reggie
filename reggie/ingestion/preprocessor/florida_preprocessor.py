from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from reggie.ingestion.utils import MissingNumColumnsError
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime
import gc


class PreprocessFlorida(Preprocessor):
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

        logging.info("preprocessing florida")
        # new_files is list of dicts, i.e. [{"name":.. , "obj": <fileobj>}, ..]
        new_files = self.unpack_files(
            compression="unzip", file_obj=self.main_file
        )
        del self.main_file, self.temp_files
        gc.collect()

        vote_history_files = []
        voter_files = []
        for i in new_files:
            if "_H_" in i["name"]:
                vote_history_files.append(i)
            elif ".txt" in i["name"]:
                voter_files.append(i)

        if not self.ignore_checks:
            self.file_check(len(voter_files))
        concat_voter_file = concat_and_delete(voter_files)
        concat_history_file = concat_and_delete(vote_history_files)
        del new_files, vote_history_files, voter_files
        gc.collect()

        logging.info("FLORIDA: loading voter history file")
        df_hist = pd.read_fwf(concat_history_file, header=None)
        try:
            df_hist.columns = self.config["hist_columns"]
        except ValueError:
            logging.info("Incorrect history columns found in Florida")
            raise MissingNumColumnsError(
                "{} state history is missing columns".format(self.state),
                self.state,
                len(self.config["hist_columns"]),
                len(df_hist.columns),
            )
        del concat_history_file
        gc.collect()

        df_hist = df_hist[df_hist["date"].map(lambda x: len(x)) > 5]
        df_hist["election_name"] = (
            df_hist["date"] + "_" + df_hist["election_type"]
        )
        valid_elections, counts = np.unique(
            df_hist["election_name"], return_counts=True
        )
        date_order = [
            idx
            for idx, election in sorted(
                enumerate(valid_elections),
                key=lambda x: datetime.strptime(x[1][:-4], "%m/%d/%Y"),
                reverse=True,
            )
        ]
        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {
            k: {"index": i, "count": int(counts[i]), "date": date_from_str(k)}
            for i, k in enumerate(sorted_codes)
        }

        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"])
        )
        del valid_elections, counts, date_order
        gc.collect()

        logging.info("FLORIDA: history apply")
        voter_groups = df_hist.groupby("VoterID")
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["vote_type"].apply(list)
        del voter_groups, df_hist
        gc.collect()

        logging.info("FLORIDA: loading main voter file")
        df_voters = self.read_csv_count_error_lines(
            concat_voter_file, header=None, sep="\t", error_bad_lines=False
        )
        del concat_voter_file
        gc.collect()

        try:
            df_voters.columns = self.config["ordered_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for Flordia")
            raise MissingNumColumnsError(
                "{} state is missing voters columns".format(self.state),
                self.state,
                len(self.config["ordered_columns"]),
                len(df_voters.columns),
            )
        df_voters = df_voters.set_index(self.config["voter_id"])

        df_voters["all_history"] = all_history
        df_voters["vote_type"] = vote_type
        del all_history, vote_type
        gc.collect()

        df_voters = self.config.coerce_strings(df_voters)
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(
            df_voters,
            extra_cols=[
                "Precinct",
                "Precinct_Split",
                "Daytime_Phone_Number",
                "Daytime_Area_Code",
                "Daytime_Phone_Extension",
                "Daytime_Area_Code",
                "Daytime_Phone_Extension",
                "Mailing_Zipcode",
                "Residence_Zipcode",
                "Mailing_Address_Line_1",
                "Mailing_Address_Line_2",
                "Mailing_Address_Line_3",
                "Residence_Address_Line_1",
                "Residence_Address_Line_2",
            ],
        )

        self.meta = {
            "message": "florida_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }

        csv_obj = df_voters.to_csv(encoding="utf-8")
        del df_voters
        gc.collect()

        logging.info("FLORIDA: writing out")
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(csv_obj),
            s3_bucket=self.s3_bucket,
        )
        del csv_obj
        gc.collect()
