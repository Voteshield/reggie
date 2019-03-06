from constants import CONFIG_DIR
import yaml
import pandas as pd

config_cache = {}


class Config(object):
    def __init__(self, state=None, file_name=None):
        if state is None and file_name is None:
            raise ValueError("either state or config file must be passed")
        if state is not None:
            config_file = self.config_file_from_state(state)
        else:
            config_file = file_name

        self.data = self.load_data(config_file)

    @classmethod
    def config_file_from_state(cls, state):
        return "{}{}.yaml".format(CONFIG_DIR, state)

    @classmethod
    def load_data(cls, config_file):

        global config_cache
        if config_file in config_cache:
            return config_cache[config_file]
        else:
            with open(config_file) as f:
                config = yaml.load(f)
                config_cache[config_file] = config
        return config

    """
    In the following 4 methods we recreate the functionality of a dictionary, 
    as needed in the rest of the application.
    """
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
        return [c for c in self.data["ordered_columns"] if c not in
                self.data["blacklist_columns"]]

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
        date_fields = [c for c, v in self.data[col_list].items() if
                       v == "date" or v == "timestamp"]
        for field in date_fields:
            df[field] = df[field].apply(str)
            if not isinstance(self.data["date_format"], list):
                df[field] = pd.to_datetime(df[field],
                                           format=self.data["date_format"],
                                           errors='coerce')
            else:
                for format_str in self.data["date_format"]:
                    formatted = pd.to_datetime(df[field], format=format_str,
                                               errors='coerce')
                    if len(formatted.unique()) > 1:
                        df[field] = formatted
                        break
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
        numeric_fields = [c for c, v in self.data[col_list].items()
                          if "int" in v or v == "float" or v == "double"]
        int_fields = [c for c, v in self.data[col_list].items()
                      if "int" in v]
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce')
        for field in int_fields:
            df[field] = df[field].astype(int, errors='ignore')
        for field in extra_cols:
            df[field] = pd.to_numeric(df[field],
                                      errors='coerce').fillna(df[field])
        return df

    def coerce_strings(self, df):
        """
        takes all columns with text or varchar labels in the config,
        strips out whitespace and converts text to all lowercase
        NOTE: does not convert voter_status or party_identifier,
              since those are typically defined as capitalized
        :param df: dataframe to modify
        :return: modified dataframe
        """
        text_fields = [c for c, v in self.data["columns"].items()
                       if v == "text" or "char" in v]
        for field in text_fields:
            if (field in df) and (field != self.data["voter_status"]) \
               and (field != self.data["party_identifier"]):
                df[field] = df[field].astype(str).str.strip().str.lower()
        return df

    def admissible_change_types(self):
        change_types = [col for col in self.data["ordered_columns"]
                        if (col not in self.history_change_types() and
                            col != self.data["voter_id"])]
        return change_types

    def history_change_types(self):
        history_cols = [
            "all_history",
            "primary_history",
            "special_history",
            "general_history",
            "sparse_history",
            "vote_type",
            "all_voting_methods",
            "party_history",
            "coded_history",
            "verbose_history",
            "voterhistory",
            "lastvoteddate",
            "Date_last_voted",
            "Date_changed",
            "Last_contact_date"]
        return history_cols
