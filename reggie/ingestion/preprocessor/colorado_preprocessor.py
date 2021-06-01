import json
from reggie.ingestion.download import Preprocessor, date_from_str, FileItem
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime
import gc
from reggie.ingestion.utils import date_from_str, MissingNumColumnsError
import chardet


class PreprocessColorado(Preprocessor):
    def __init__(self, raw_s3_file, config_file, force_date=None, **kwargs):

        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        super().__init__(
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            **kwargs,
        )
        self.raw_s3_file = raw_s3_file
        self.processed_file = None

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(
            compression="unzip", file_obj=self.main_file
        )
        df_voter = pd.DataFrame(columns=self.config.raw_file_columns())
        df_hist = pd.DataFrame(columns=self.config["hist_columns"])
        df_master_voter = pd.DataFrame(
            columns=self.config["master_voter_columns"]
        )
        master_vf_version = True

        def master_to_reg_df(df):
            df.columns = self.config["master_voter_columns"]
            df["STATUS"] = df["VOTER_STATUS"]
            df["PRECINCT"] = df["PRECINCT_CODE"]
            df["VOTER_NAME"] = (
                df["LAST_NAME"]
                + ", "
                + df["FIRST_NAME"]
                + " "
                + df["MIDDLE_NAME"]
            )
            df = pd.concat(
                [df, pd.DataFrame(columns=self.config["blacklist_columns"])]
            )
            df = df[self.config.processed_file_columns()]
            return df

        for i in new_files:
            if "Registered_Voters_List" in i["name"]:
                master_vf_version = False

        for i in new_files:
            if "Public" not in i["name"]:

                if (
                    "Registered_Voters_List" in i["name"]
                    and not master_vf_version
                ):
                    logging.info("reading in {}".format(i["name"]))
                    # Colorado has a couple different encodings they send us, the format that is detected as ascii will
                    # error out if not read in as latin-1
                    # The format that is typically detected as utf-8-sig needs to have the index col explicitly set to
                    # false, or else pandas will attempt to read the voterid column
                    # in as the index and the history won't apply
                    encoding_result = chardet.detect(i["obj"].read(10000))
                    if encoding_result["encoding"] == "ascii":
                        encoding = "latin-1"
                        index_col = None
                    else:
                        encoding = encoding_result["encoding"]
                        index_col = False
                    i["obj"].seek(0)
                    df_voter = pd.concat(
                        [
                            df_voter,
                            self.read_csv_count_error_lines(
                                i["obj"],
                                encoding=encoding,
                                error_bad_lines=False,
                                index_col=index_col,
                            ),
                        ],
                        axis=0,
                    )

                elif ("Voting_History" in i["name"]) or (
                    "Coordinated_Voter_Details" in i["name"]
                ):
                    if "Voter_Details" not in i["name"]:
                        logging.info("reading in {}".format(i["name"]))
                        new_df = self.read_csv_count_error_lines(
                            i["obj"], compression="gzip", error_bad_lines=False
                        )
                        df_hist = pd.concat([df_hist, new_df], axis=0)

                    if "Voter_Details" in i["name"] and master_vf_version:
                        logging.info("reading in {}".format(i["name"]))
                        new_df = self.read_csv_count_error_lines(
                            i["obj"], compression="gzip", error_bad_lines=False
                        )
                        if len(new_df.columns) < len(
                            self.config["master_voter_columns"]
                        ):
                            new_df.insert(10, "PHONE_NUM", np.nan)
                        try:
                            new_df.columns = self.config[
                                "master_voter_columns"
                            ]
                        except ValueError:
                            logging.info(
                                "Incorrect number of columns found for Colorado for file: {}".format(
                                    i["name"]
                                )
                            )
                            raise MissingNumColumnsError(
                                "{} state is missing columns".format(
                                    self.state
                                ),
                                self.state,
                                len(self.config["master_voter_columns"]),
                                len(new_df.columns),
                            )
                        df_master_voter = pd.concat(
                            [df_master_voter, new_df], axis=0
                        )

        if df_voter.empty:
            df_voter = master_to_reg_df(df_master_voter)
        if df_hist.empty:
            raise ValueError("must supply a file containing voter history")
        df_hist["VOTING_METHOD"] = df_hist["VOTING_METHOD"].replace(np.nan, "")
        df_hist["ELECTION_DATE"] = pd.to_datetime(
            df_hist["ELECTION_DATE"], format="%m/%d/%Y", errors="coerce"
        )
        df_hist.dropna(subset=["ELECTION_DATE"], inplace=True)
        df_hist["election_name"] = (
            df_hist["ELECTION_DATE"].astype(str)
            + "_"
            + df_hist["VOTING_METHOD"]
        )

        valid_elections, counts = np.unique(
            df_hist["election_name"], return_counts=True
        )

        date_order = [
            idx
            for idx, election in sorted(
                enumerate(valid_elections),
                key=lambda x: datetime.strptime(x[1][0:10], "%Y-%m-%d"),
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

        logging.info("Colorado: history apply")
        voter_groups = df_hist.groupby(self.config["voter_id"])
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["VOTING_METHOD"].apply(list)

        df_voter.dropna(subset=[self.config["voter_id"]], inplace=True)
        df_voter = df_voter.set_index(self.config["voter_id"])
        df_voter.sort_index(inplace=True)

        df_voter["all_history"] = all_history
        df_voter["vote_type"] = vote_type
        gc.collect()

        # at some point mailing address field names changed
        for num in ["1", "2", "3"]:
            if f"MAIL_ADDR{num}" in df_voter.columns:
                # if both are present, combine them
                if f"MAILING_ADDRESS_{num}" in df_voter.columns:
                    df_voter[f"MAILING_ADDRESS_{num}"] = np.where(
                        df_voter[f"MAILING_ADDRESS_{num}"].isnull(),
                        df_voter[f"MAIL_ADDR{num}"],
                        df_voter[f"MAILING_ADDRESS_{num}"],
                    )
                else:
                    df_voter[f"MAILING_ADDRESS_{num}"] = df_voter[
                        f"MAIL_ADDR{num}"
                    ]
                df_voter.drop(columns=[f"MAIL_ADDR{num}"], inplace=True)

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(
            df_voter,
            extra_cols=[
                "HOUSE_NUM",
                "UNIT_NUM",
                "RESIDENTIAL_ZIP_CODE",
                "RESIDENTIAL_ZIP_PLUS",
                "MAILING_ZIP_CODE",
                "MAILING_ZIP_PLUS",
                "PRECINCT_NAME",
                "PRECINCT",
                "MAILING_ADDRESS_3",
                "PHONE_NUM",
            ],
        )

        self.meta = {
            "message": "Colorado_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        gc.collect()
        logging.info("Colorado: writing out")
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voter.to_csv(encoding="utf-8")),
            s3_bucket=self.s3_bucket,
        )
