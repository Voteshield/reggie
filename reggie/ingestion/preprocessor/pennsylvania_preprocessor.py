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

        # helper
        def return_election_string(x, election_dict_map):
            elect_map = election_dict_map.copy()
            no_nan_cols = x.dropna()
            hist_list = []
            for i, value in enumerate(no_nan_cols):
                hist_string = ""
                election_key = no_nan_cols.index[i].split('_')[1]
                election = "_".join(no_nan_cols.index[i].split('_')[:2])
                if int(election_key) in elect_map.keys():
                    hist_string += elect_map[int(election_key)]
                    hist_string += " "
                    elect_map.pop(int(election_key))
                    hist_string += f'{no_nan_cols[election + "_vote_method"]} '
                    hist_string += f'{no_nan_cols[election + "_party"]}'
                if hist_string != "":
                    hist_list.append(hist_string)
            return hist_list

        # more help
        def return_district_string(x, zone_dict):
            no_nan = x.dropna()
            zone_list = []
            zone_string = ""
            for idx, value in enumerate(no_nan):
                zone_string = ""
                try:
                    zone_string += f'{zone_dict[str(no_nan[idx])]}'
                except KeyError:
                    zone_string = ""
                if zone_list != "":
                    zone_list.append(zone_string)
            return zone_list
        # for c in counties:
        for idx, c in enumerate(counties):
            logging.info("Processing {}".format(c))
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
                names=["county", "number", "code", "title"],
                error_bad_lines=False,
            )
            tdf = self.read_csv_count_error_lines(
                types["obj"],
                sep="\t",
                names=["county", "number", "abbr", "title"],
                error_bad_lines=False,
            )
            # vote_history = df.iloc[:, 70:110]
            # vote_columns = vote_history.columns.to_list()
            vote_columns = df.columns[70:110].to_list()
            district_columns = df.columns[30:70].to_list()
            edf["election_list"] = edf["title"] + " " + edf["date"]
            election_map = pd.Series(edf.election_list.values, index=edf.number).to_dict()
            # vectorize if time or possible?
            df["all_history"] = df[vote_columns].apply(return_election_string, args=(election_map,), axis=1)
            unholy_union = zdf.merge(tdf, how='left', on='zone_number')
            unholy_union["combined"] = unholy_union["zone_description"] + " Type: " + unholy_union["zone_long_name"]
            zone_dict = dict(zip(unholy_union.zone_code, unholy_union.combined))
            df["all_history"] = df[vote_columns].apply(return_election_string, args=(election_map,), axis=1)
            df["districts"] = df[district_columns].apply(return_district_string, args=(zone_dict,), axis=1)
            # for i in range(elections):
            #     df = df.drop("election_{}".format(i), axis=1)
            #     df = df.drop("district_{}".format(i + 1), axis=1)
            # can check columns for each PA county?
            self.column_check(list(df.columns))
            if main_df is None:
                main_df = df
            else:
                main_df = pd.concat([main_df, df], ignore_index=True)
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

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(main_df.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
