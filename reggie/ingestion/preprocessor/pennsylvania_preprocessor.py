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
        for i in range(elections):
            dfcols.extend(["district_{}".format(i + 1)])
        for i in range(elections):
            dfcols.extend(["election_{}_vote_method".format(i + 1)])
            dfcols.extend(["election_{}_party".format(i + 1)])
        dfcols.extend(config["ordered_columns"][-3:])

        for c in counties:
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
            df = df.replace('"')
            edf = edf.replace('"')
            zdf = zdf.replace('"')
            edf.index = edf["number"]

            for i in range(elections):
                s = pd.Series(index=df.index)
                # Blair isn't sending all their election codes
                try:
                    s[:] = (
                        edf.iloc[i]["title"] + " " + edf.iloc[i]["date"] + " "
                    )
                except IndexError:
                    s[:] = "UNSPECIFIED"
                df["election_{}".format(i)] = (
                    s
                    + df["election_{}_vote_method".format(i + 1)].apply(str)
                    + " "
                    + df["election_{}_party".format(i + 1)]
                )
                df.loc[
                    df["election_{}_vote_method".format(i + 1)].isna(),
                    "election_{}".format(i),
                ] = pd.np.nan
                df = df.drop("election_{}_vote_method".format(i + 1), axis=1)
                df = df.drop("election_{}_party".format(i + 1), axis=1)

                df["district_{}".format(i + 1)] = df[
                    "district_{}".format(i + 1)
                ].map(
                    zdf.drop_duplicates("code")
                    .reset_index()
                    .set_index("code")["title"]
                )
                df["district_{}".format(i + 1)] += ", Type: " + df[
                    "district_{}".format(i + 1)
                ].map(
                    zdf.drop_duplicates("title")
                    .reset_index()
                    .set_index("title")["number"]
                ).map(
                    tdf.set_index("number")["title"]
                )

            df["all_history"] = df[
                ["election_{}".format(i) for i in range(elections)]
            ].values.tolist()
            df["all_history"] = df["all_history"].map(
                lambda L: list(filter(pd.notna, L))
            )
            df["districts"] = df[
                ["district_{}".format(i + 1) for i in range(elections)]
            ].values.tolist()
            df["districts"] = df["districts"].map(
                lambda L: list(filter(pd.notna, L))
            )

            for i in range(elections):
                df = df.drop("election_{}".format(i), axis=1)
                df = df.drop("district_{}".format(i + 1), axis=1)

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
