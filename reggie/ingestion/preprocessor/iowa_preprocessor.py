import json
from reggie.ingestion.download import Preprocessor, date_from_str, FileItem
import logging
import pandas as pd
import datetime
from io import StringIO
import numpy as np
from datetime import datetime


class PreprocessIowa(Preprocessor):
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

        def is_first_file(fname):
            if ("CD1" in fname) or ("CD 1" in fname):
                if ("Part1" in fname) or ("Part 1" in fname):
                    return True
            return False

        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip"
        )
        logging.info("IOWA: reading in voter file")

        first_file = [f for f in new_files if is_first_file(f["name"])][0]
        remaining_files = [
            f for f in new_files if not is_first_file(f["name"])
        ]
        if not self.ignore_checks:
            # add 1 for firs file
            valid_files = len(remaining_files) + 1
            self.file_check(valid_files)

        buffer_cols = [
            "buffer0",
            "buffer1",
            "buffer2",
            "buffer3",
            "buffer4",
            "buffer5",
            "buffer6",
            "buffer7",
            "buffer8",
            "buffer9",
        ]

        # Reads the headers in on the first file given
        headers = pd.read_csv(first_file["obj"], nrows=1).columns

        # Gather the columns for renaming in order to fit the original schema in the database and then rename
        # so that the columns in the header will fit what is expected
        column_rename_dict = self.config["rename_columns"]
        normalized_headers = [
            x if x not in column_rename_dict else column_rename_dict[x]
            for x in headers
        ]
        normalized_headers = [x.replace(" ", "_") for x in normalized_headers]

        columns_to_check = [
            x
            for x in normalized_headers
            if x not in self.config["election_columns"]
        ]
        self.column_check(columns_to_check)

        # Add the buffer columns back in for lines that contain extra commas
        headers_with_buffers = normalized_headers + buffer_cols

        # Begin reading the file with the correct headers
        df_voters = self.read_csv_count_error_lines(
            first_file["obj"],
            skiprows=1,
            header=None,
            names=headers_with_buffers,
            error_bad_lines=False,
        )

        for i in remaining_files:
            skiprows = 1 if "Part1" in i["name"] else 0
            new_df = self.read_csv_count_error_lines(
                i["obj"],
                header=None,
                skiprows=skiprows,
                names=headers_with_buffers,
                error_bad_lines=False,
            )
            df_voters = pd.concat([df_voters, new_df], axis=0)

        key_delim = "_"
        df_voters["all_history"] = ""
        df_voters = df_voters[df_voters.COUNTY != "COUNTY"]

        # instead of iterating over all of the columns for each row, we should
        # handle all this beforehand.
        # also we should not compute the unique values until after, not before
        df_voters.drop(columns=buffer_cols, inplace=True)

        for c in self.config["election_dates"]:
            null_rows = df_voters[c].isnull()
            df_voters[c][null_rows] = ""

            # each key contains info from the columns
            prefix = c.split("_")[0] + key_delim

            # and the corresponding votervotemethod column
            vote_type_col = c.replace("ELECTION_DATE", "VOTERVOTEMETHOD")
            null_rows = df_voters[vote_type_col].isnull()
            df_voters[vote_type_col].loc[null_rows] = ""
            # add election type and date
            df_voters[c] = prefix + df_voters[c].str.strip()
            # add voting method
            df_voters[c] += key_delim + df_voters[vote_type_col].str.strip()

            # the code below will format each key as
            # <election_type>_<date>_<voting_method>_<political_party>_
            # <political_org>
            if "PRIMARY" in prefix:

                # so far so good but we need more columns in the event of a
                # primary
                org_col = c.replace(
                    "PRIMARY_ELECTION_DATE", "POLITICAL_ORGANIZATION"
                )
                party_col = c.replace(
                    "PRIMARY_ELECTION_DATE", "POLITICAL_PARTY"
                )
                df_voters[org_col].loc[df_voters[org_col].isnull()] = ""
                df_voters[party_col].loc[df_voters[party_col].isnull()] = ""
                party_info = (
                    df_voters[party_col].str.strip()
                    + key_delim
                    + df_voters[org_col].str.replace(" ", "")
                )
                df_voters[c] += key_delim + party_info
            else:
                # add 'blank' values for the primary slots
                df_voters[c] += key_delim + key_delim

            df_voters[c] = df_voters[c].str.replace(prefix + key_delim * 3, "")
            df_voters[c] = df_voters[c].str.replace('"', "")
            df_voters[c] = df_voters[c].str.replace("'", "")
            df_voters.all_history += " " + df_voters[c]

        # make into an array (null values are '' so they are ignored)
        df_voters.all_history = df_voters.all_history.str.split()
        elections, counts = np.unique(
            df_voters[self.config["election_dates"]], return_counts=True
        )
        # we want reverse order (lower indices are higher frequency)
        count_order = counts.argsort()[::-1]
        elections = elections[count_order]
        counts = counts[count_order]

        # create meta
        sorted_codes_dict = {
            j: {"index": i, "count": int(counts[i]), "date": date_from_str(j)}
            for i, j in enumerate(elections)
        }

        default_item = {"index": len(elections)}

        def ins_code_bin(a):
            return [sorted_codes_dict.get(k, default_item)["index"] for k in a]

        # In an instance like this, where we've created our own systematized
        # labels for each election I think it makes sense to also keep them
        # in addition to the sparse history
        df_voters["sparse_history"] = df_voters.all_history.apply(ins_code_bin)

        self.meta = {
            "message": "iowa_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(elections.tolist()),
        }
        for c in df_voters.columns:
            df_voters[c].loc[df_voters[c].isnull()] = ""

        for c in df_voters.columns:
            df_voters[c] = (
                df_voters[c]
                .astype(str)
                .str.encode("utf-8", errors="ignore")
                .str.decode("utf-8")
            )

        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(
            df_voters,
            extra_cols=[
                "COMMUNITY_COLLEGE",
                "COMMUNITY_COLLEGE_DIRECTOR",
                "LOSST_CONTIGUOUS_CITIES",
                "PRECINCT",
                "SANITARY",
                "SCHOOL_DIRECTOR",
                "UNIT_NUM",
            ],
        )
        # force reg num to be integer
        df_voters["REGN_NUM"] = pd.to_numeric(
            df_voters["REGN_NUM"], errors="coerce"
        ).fillna(0)
        df_voters["REGN_NUM"] = df_voters["REGN_NUM"].astype(int)

        # Drop the election columns because they are no longer needed
        df_voters.drop(columns=self.config["election_columns"], inplace=True)

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(df_voters.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
