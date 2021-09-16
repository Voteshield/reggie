from reggie.reggie_constants import (
    CONFIG_DIR,
    PRIMARY_LOCALE_ALIAS,
    PRIMARY_LOCALE_TYPE,
    PRIMARY_LOCALE_NAMES,
    LOCALE_DIR,
)
import yaml
import pandas as pd
import json
import glob
from datetime import datetime

config_cache = {}


class Config(object):
    def __init__(self, state=None, file_name=None):
        if state is None and file_name is None:
            raise ValueError("either state or config file must be passed")
        if state is not None:
            config_file = self.config_file_from_state(state)
        else:
            config_file = file_name

        self.data = self.load_data(
            config_file, self.infer_locale_files(config_file)
        )

        self.primary_locale_type = self.data.get(PRIMARY_LOCALE_TYPE, "county")
        self.primary_locale_names = self.data[PRIMARY_LOCALE_NAMES]
        self.primary_locale_column = self.data[PRIMARY_LOCALE_ALIAS]

    @classmethod
    def config_file_from_state(cls, state):
        return "{}{}.yaml".format(CONFIG_DIR, state)

    @classmethod
    def infer_locale_files(cls, config_file):
        """
        If one or more human-readable locale name files exists in the
        expected locations for this state, return list of all files.
        :param config_file: state's yaml file
        :return: list of locale name files
        """
        state = config_file.split("/")[-1].split(".")[0]
        return glob.glob(f"{LOCALE_DIR}*/{state}.json")

    @classmethod
    def load_data(cls, config_file, locale_files):

        global config_cache
        if config_file in config_cache:
            return config_cache[config_file]
        else:
            with open(config_file) as f:
                # quiet the warnings and also be safe but ensure compatibility
                config = yaml.load(f, Loader=yaml.FullLoader)

                # add primary locale dicts to config object
                config[PRIMARY_LOCALE_NAMES] = {}
                for f in locale_files:
                    locale_type = f.split("/")[-2]
                    try:
                        with open(f) as fp:
                            locale_data = json.load(fp)
                        locale_dict = {}
                        for locale in locale_data:
                            locale_dict[locale["id"]] = locale["name"]
                    except:
                        locale_dict = None
                    config[PRIMARY_LOCALE_NAMES][locale_type] = locale_dict

                config_cache[config_file] = config
        return config

    # """
    # In the following 4 methods we recreate the functionality of a dictionary,
    # as needed in the rest of the application.
    # """

    def __getitem__(self, key):
        return self.data[key]

    def __contains__(self, key):
        return key in self.data

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    def database_columns(self):
        return [
            c
            for c in self.data["ordered_columns"]
            if c not in self.data["blacklist_columns"]
        ]

    def raw_file_columns(self):
        """
        raw file columns is used to set the column names in the
        beginning of the file formatting, when the naming of the
        columns is different from the columns in the semi-universal format.
        :return: list of raw columns if columns are different
        from database columns
        """

        if "raw_ordered_columns" in self.data:
            return self.data["raw_ordered_columns"]
        else:
            return self.data["ordered_columns"]

    def processed_file_columns(self):
        return self.data["ordered_columns"]

    def coerce_dates(self, df, col_list="columns"):
        """
        takes all columns with timestamp or date labels in the config file and
        forces the corresponding entries in the raw file into datetime objects
        :param df: dataframe to modify
        :param col_list: name of field in yaml to pull column types from
        :return: modified dataframe
        """

        def catch_floats(x):
            if ".0" == x[-2:]:
                return str(int(float(x)))
            else:
                return x

        def disallow_future_dates(x, max_year):
            if (type(x) != str) and (x.year > max_year):
                return pd.Timestamp(x.year - 100, x.month, x.day)
            return x

        def disallow_past_dates(x, min_year=1910):
            if (type(x) != str) and (x.year < min_year):
                return pd.NaT
            return x

        min_voter_age = 17

        date_fields = [
            c
            for c, v in self.data[col_list].items()
            if v == "date" or v == "timestamp"
        ]
        date_fields = [x for x in date_fields if x in df.columns]
        for field in date_fields:
            df[field] = df[field].apply(str)
            df[field] = df[field].str.strip()
            df[field] = df[field].map(catch_floats)
            if not isinstance(self.data["date_format"], list):
                df[field] = pd.to_datetime(
                    df[field], format=self.data["date_format"], errors="coerce"
                ).fillna(pd.NaT)
            else:
                for format_str in self.data["date_format"]:
                    formatted = pd.to_datetime(
                        df[field], format=format_str, errors="coerce"
                    ).fillna(pd.NaT)
                    if len(formatted.unique()) > 1:
                        df[field] = formatted
                        break

            if field == self.data["birthday_identifier"]:
                df[field] = df[field].map(
                    lambda x: disallow_future_dates(
                        x, datetime.now().year - min_voter_age
                    )
                )
            else:
                df[field] = df[field].map(
                    lambda x: disallow_future_dates(x, datetime.now().year)
                )

            df[field] = df[field].map(disallow_past_dates)
        return df

    def coerce_numeric(self, df, extra_cols=None, col_list="columns"):
        """
        takes all columns with int labels in the config file as well as any
        requested extra columns, and forces the corresponding entries in the
        raw file into numerics
        :param df: dataframe to modify
        :param extra_cols: other columns to convert
        :param col_list: name of field in yaml to pull column types from
        :return: modified dataframe
        """
        extra_cols = [] if extra_cols is None else extra_cols
        numeric_fields = [
            c
            for c, v in self.data[col_list].items()
            if "int" in v or v == "float" or v == "double"
        ]
        int_fields = [c for c, v in self.data[col_list].items() if "int" in v]
        numeric_fields = [x for x in numeric_fields if x in df.columns]
        int_fields = [x for x in int_fields if x in df.columns]
        if extra_cols is not None:
            extra_cols = [x for x in extra_cols if x in df.columns]
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors="coerce")
        for field in int_fields:
            df[field] = df[field].astype(int, errors="ignore")
        for field in extra_cols:
            df[field] = pd.to_numeric(df[field], errors="coerce").fillna(
                df[field]
            )
        return df

    def coerce_strings(
        self, df, extra_cols=None, exclude=[""], col_list="columns"
    ):
        """
        takes all columns with text or varchar labels in the config,
        strips out whitespace and converts text to all lowercase
        NOTE: does not convert voter_status or party_identifier,
              since those are typically defined as capitalized
        :param df: dataframe to modify
        :param extra_cols: extra columns to add
        :param exclude: columns to exclude
        :param col_list: name of field in yaml to pull column types from
        :return: modified dataframe
        """
        text_fields = [
            c
            for c, v in self.data[col_list].items()
            if v == "text" or "char" in v
        ]
        if extra_cols is not None:
            text_fields = text_fields + extra_cols
        print(text_fields)
        for field in text_fields:
            if (
                (field in df)
                and (field != self.data["voter_status"])
                and (field != self.data["party_identifier"])
                and (field not in exclude)
            ):
                if "use_pyarrow" not in self.data.keys():
                    string_copy = df[field].astype(str)
                else:
                    string_copy = df[field]
                stripped_copy = string_copy.str.strip()
                stripped_copy = stripped_copy.str.split().str.join(" ")
                lower_copy = stripped_copy.str.lower()
                utf_decoded = lower_copy.str.encode("utf-8", errors="ignore")
                df[field] = utf_decoded.str.decode("utf-8")
        return df

    def to_json(self):
        return json.dumps(self.data)
