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
        elections = 40
        dfcols = config["ordered_columns"][:-3]
        # could be consolidated into one loop but elections is only 40 long and the order here matters
        for i in range(elections):
            dfcols.extend(["district_{}".format(i + 1)])
        for i in range(elections):
            dfcols.extend(["election_{}_vote_method".format(i + 1)])
            dfcols.extend(["election_{}_party".format(i + 1)])
        dfcols.extend(config["ordered_columns"][-3:])

        # help, python maps are so much faster
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
            except StopIteration:
                continue
            df = self.read_csv_count_error_lines(
                voter_file["obj"],
                sep="\t",
                names=dfcols,
                error_bad_lines=False,
            )
            edf = self.read_csv_count_error_lines(
                election_map["obj"],
                sep="\t",
                names=["county", "number", "title", "date"],
                error_bad_lines=False,
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
            # format the election data to merge
            edf["election_list"] = edf["title"] + " " + edf["date"]

            vote_columns = df.columns[70:150].to_list()
            district_columns = df.columns[30:70].to_list()

            # create a dict of the election data and the number in the given file, this corresponds to the column
            # location in the file
            election_map = pd.Series(
                edf.election_list.values, index=edf.number
            ).to_dict()
            # merge the zone files together
            unholy_union = zdf.merge(tdf, how="left", on="zone_number")
            # format a column that contains the zone description and the name so that it matches the current district
            # field
            unholy_union["combined"] = (
                unholy_union["zone_description"]
                + " Type: "
                + unholy_union["zone_long_name"]
            )

            # create a dict that contains the zone code as the key and the long name string as the value
            zone_dict = dict(
                zip(unholy_union.zone_code.astype(str), unholy_union.combined)
            )
            # gathers the pairs of election columns to iterate over both at the same time
            vote_li = list(zip(df.columns[70:110:2], df.columns[71:110:2]))
            district_df = df[district_columns]
            # get the value from the eleciton map key, then combine it with the value in the party and vote type cells
            vote_hist_df = pd.DataFrame(
                {
                    i: election_map[int(i.split("_")[1])]
                    + " "
                    + df[i]
                    + " "
                    + df[j]
                    for i, j in vote_li
                    if int(i.split("_")[1]) in zone_dict
                }
            )

            # Unnecessary
            cols_to_drop = vote_hist_df.columns
            # converts the dataframe to a series that contains the list of elections participate in indexed on position
            vote_hist_df = list_map(vote_hist_df, cols_to_drop)
            district_ser = list_map(district_df, district_columns, zone_dict)

            df["all_history"] = vote_hist_df
            df["districts"] = district_ser
            df.drop(vote_columns, axis=1, inplace=True)
            df.drop(district_columns, axis=1, inplace=True)

            #     df["all_history"] = df[vote_columns].apply(return_election_string, args=(election_map,), axis=1)
            #     df["districts"] = df[district_columns].apply(return_district_string, args=(zone_dict,), axis=1)
            # return df
            # can check columns for each PA county?
            self.column_check(list(df.columns))
            if main_df is None:
                main_df = df
            else:
                main_df = pd.concat([main_df, df], ignore_index=True)

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
        }
        return main_df
        # self.processed_file = FileItem(
        #     name="{}.processed".format(self.config["state"]),
        #     io_obj=StringIO(main_df.to_csv(encoding="utf-8", index=False)),
        #     s3_bucket=self.s3_bucket,
        #)
