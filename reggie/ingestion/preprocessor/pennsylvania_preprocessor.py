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
import time
from dateutil import parser
from collections import defaultdict
import json
'''







Notes:

The Pennsylvania files come in sets, consisting of 4 files per county. The first is the voter file which contains
the history columns.
These history columns consist of two columns per election (for votetype and party) for 40 elections (80 columns total)
 these are positional in the dataframe,
There is an additional election map file, which contains the information about each election indexed on their position

Pennsylvania also divides the districts into 40 positional columns which correspond to the two zone files given. One
containing the information on the zone types and the other which maps to the zone codes found in the fields in the 
district columns in the dataframe

There is the STRANGEST pandas error in Bedford county when creating the temporary dataframe
"ValueError: unknown type str1056" investigate more, but this error doesn't show up anywhere else and the solutions
for similar errors are to wait for pandas releases. It's something in pandas expressions evaluate I think types are
wrong?
'''


class PreprocessPennsylvania(Preprocessor):
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
        voter_files = [f for f in new_files if "FVE" in f["name"]]
        election_maps = [f for f in new_files if "Election Map" in f["name"]]
        zone_codes = [f for f in new_files if "Codes" in f["name"]]
        zone_types = [f for f in new_files if "Types" in f["name"]]

        if not self.ignore_checks:
            # election maps need to line up to voter files?
            self.file_check(len(voter_files), len(election_maps))
        counties = config["county_names"]
        main_df = None
        dfcols = config["ordered_columns"]

        # add dtypes to the columns here, so far it's the only thing that fixes the strange error

        # help, python maps are so much faster
        # create a mapping that returns a series based on the values across rows of cells.
        # consolidates the non nan values into one string that can be appended as a column later for the all_history and
        # the districts columns
        def list_map(df_sub, columns, zone_dict=None):
            def mapping(li, zone_dict=zone_dict):
                if zone_dict is None:
                    li = [x for x in li if x != "nan"]
                    return li
                else:
                    li = [
                        zone_dict[x]
                        for x in li
                        if x != "nan" and x in zone_dict
                    ]
                    return li

            return pd.Series(
                map(mapping, df_sub[columns].values.astype(str).tolist())
            )

        # for c in counties:
        sorted_codes = []
        sorted_code_dict = defaultdict(defaultdict)
        for idx, c in enumerate(counties):
            logging.info("Processing {} {}/{}".format(c, idx, len(counties)))
            c = format_column_name(c)
            try:
                voter_file = next(
                    f for f in voter_files if c in f["name"].lower()
                )
                election_map = next(
                    f for f in election_maps if c in f["name"].lower()
                )
                zones = next(f for f in zone_codes if c in f["name"].lower())
                types = next(f for f in zone_types if c in f["name"].lower())
            # Potentially consider catching the error if they forget to include a file here?
            except StopIteration:
                continue
            df = self.read_csv_count_error_lines(
                voter_file["obj"],
                sep="\t",
                names=dfcols,
                error_bad_lines=False, dtype=config['dtypes']
            )
            edf = self.read_csv_count_error_lines(
                election_map["obj"],
                sep="\t",
                names=["county", "number", "title", "date"],
                error_bad_lines=False, dtype={'county': str, 'number': str, 'title': str, 'date': str}
            )
            zdf = self.read_csv_count_error_lines(
                zones["obj"],
                sep="\t",
                names=[
                    "county_name",
                    "zone_number",
                    "zone_code",
                    "zone_description",
                ],
                error_bad_lines=False,
            )
            tdf = self.read_csv_count_error_lines(
                types["obj"],
                sep="\t",
                names=[
                    "county_name",
                    "zone_number",
                    "zone_short_name",
                    "zone_long_name",
                ],
                error_bad_lines=False,
            )
            # format the election data into the format expected in the original all_history column
            edf["election_list"] = edf["title"] + " " + edf["date"]

            # Gather the positional vote and distict columns
            district_columns = df.columns[30:70].to_list()
            vote_columns = df.columns[70:150].to_list()

            # create a dict of the formatted string election data with the index number in the given file, this
            # corresponds to the column index beginning at the start of the vote columns in the dataframe (starting at 1)
            # Todo: This upper is causing a pretty big diff but I am not sure if I care?
            election_map = pd.Series(
                edf.election_list.str.upper().values, index=edf.number
            ).to_dict()

            # merge the zone files together to consolidate the information in one dataframe
            # todo: just add the info to the one dataframe rather than create a new one
            merged_zones = zdf.merge(tdf, how="left", on="zone_number")
            # format a column field that contains the zone description and the name so
            # that it matches the current district field
            merged_zones["combined"] = (
                merged_zones["zone_description"]
                + " Type: "
                + merged_zones["zone_long_name"]
            )

            # create a dict that utilizes the zone code as the key and the long name string as the value
            zone_dict = dict(
                zip(merged_zones.zone_code.astype(str), merged_zones.combined)
            )

            # Gather the pairs of election columns to iterate over both at the same time to collect the information
            # contained in both of the columns per election
            vote_column_list = list(zip(df.columns[70:150:2], df.columns[71:150:2]))

            # todo: probably don't need this dataframe either
            district_df = df[district_columns]

            # get the value from the eleciton map key, then combine it with the value in the party and vote type cells
            # Create a history dataframe containing, as cells the election name as gathered in the election file, the
            # vote type (AP, A etc), and the party all separated by spaces
            # The columns are all named election_#_vote_type but the cells contain the relevant information
            vote_hist_df = pd.DataFrame(
                {
                    i: election_map[i.split("_")[1]]
                    + " "
                    + df[i]
                    + " "
                    + df[j]
                    for i, j in vote_column_list
                    if i.split("_")[1] in election_map
                }
            )

            # counts for the metadata
            counts = vote_hist_df.count()
            for i in counts.index:
                current_key = election_map[i.split("_")[1]]
                
                # Metadata needs to be _ separated not space
                current_key = "_".join(current_key.split())
                if current_key in sorted_code_dict:
                    sorted_code_dict[current_key]['count'] += int(counts[i])
                else:
                    # todo: consolidate this into one dict call if possible
                    current_date = edf.loc[edf['number'] == i.split("_")[1]]['date'].values[0]
                    new_dict_entry = defaultdict(str)
                    new_dict_entry['date'] = current_date
                    new_dict_entry['count'] = int(counts[i])
                    sorted_code_dict[current_key] = new_dict_entry
            # converts the dataframe to a series that contains the list of elections participate in indexed on position
            vote_hist_df = list_map(vote_hist_df, vote_hist_df.columns)
            districts = list_map(district_df, district_columns, zone_dict)

            df["all_history"] = vote_hist_df
            df["districts"] = districts
            df.drop(vote_columns, axis=1, inplace=True)
            df.drop(district_columns, axis=1, inplace=True)

            cols_to_check = [col for col in list(df.columns) if col not in vote_columns and col not in district_columns]

            self.column_check(list(df.columns), cols_to_check)
            if main_df is None:
                main_df = df
            else:
                main_df = pd.concat([main_df, df], ignore_index=True)
        sorted_keys = sorted(sorted_code_dict.items(), key=lambda x: parser.parse(x[1]['date']))
        for index, key in enumerate(sorted_keys):
            sorted_code_dict[key[0]]['index'] = index
            sorted_codes.append(key[0])

        logging.info("coercing")
        main_df = config.coerce_dates(main_df)
        main_df = config.coerce_numeric(
            main_df,
            extra_cols=[
                "house_number",
                "apartment_number",
                "address_line_2",
                "zip",
                "mail_address_1",
                "mail_address_2",
                "mail_zip",
                "precinct_code",
                "precinct_split_id",
                "legacy_id",
                "home_phone",
            ],
        )
        logging.info("Writing CSV")
        self.meta = {
            "message": "pennsylvania_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_code_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        # to verify more easily
        # return main_df
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(main_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
