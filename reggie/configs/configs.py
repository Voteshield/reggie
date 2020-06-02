from reggie.constants import CONFIG_DIR, COUNTY_ALIAS, \
    PRIMARY_LOCALE_TYPE
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
        self.county_column = self.data[COUNTY_ALIAS]
        self.primary_locale_type = self.data.get(PRIMARY_LOCALE_TYPE, "county")


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
        return [c for c in self.data["ordered_columns"] if c not in
                self.data["blacklist_columns"]]

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
        date_fields = [c for c, v in self.data[col_list].items() if
                       v == "date" or v == "timestamp"]
        for field in date_fields:
            df[field] = df[field].apply(str)
            if not isinstance(self.data["date_format"], list):
                df[field] = pd.to_datetime(df[field],
                    errors='coerce').dt.strftime(self.data['date_format'])
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

    def coerce_strings(self, df, extra_cols=None, exclude=None, col_list="columns"):
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
        text_fields = [c for c, v in self.data[col_list].items()
                       if v == "text" or "char" in v]
        if extra_cols is not None:
            text_fields = text_fields + extra_cols

        for field in text_fields:
            if (field in df) \
            and (field != self.data["voter_status"]) \
            and (field != self.data["party_identifier"]) \
            and (field not in exclude):
                string_copy = df[field].astype(str)
                stripped_copy = string_copy.str.strip()
                lower_copy = stripped_copy.str.lower()
                utf_decoded = lower_copy.str.encode('utf-8', errors='ignore')
                df[field] = utf_decoded.str.decode('utf-8')
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
            "votetype_history",
            "county_history",
            "jurisdiction_history",
            "schooldistrict_history",
            "all_voting_methods",
            "party_history",
            "coded_history",
            "verbose_history",
            "voterhistory",
            "lastvoteddate",
            "Date_last_voted",
            "Date_changed",
            "Last_contact_date",
            "text_election_code_1",
            "text_election_code_2",
            "text_election_code_3",
            "text_election_code_4",
            "text_election_code_5",
            "text_election_code_6",
            "text_election_code_7",
            "text_election_code_8",
            "text_election_code_9",
            "text_election_code_10",
            "Election_Date",
            "Election_Type",
            "Election_Party",
            "Election_Voting_Method"]
        return history_cols

    def get_locale_field(self, locale_type):
        """
        Return field name (e.g. "county_code") for a standardized locale type.
        :param locale_type: one of standardized set {"county", "jurisdiction", etc.}
        :return: actual field name for this locale_type in yaml / db
        """
        if locale_type is None:
            locale_type = self.primary_locale_type
        locale_field = self.data['{}_identifier'.format(locale_type)]
        return locale_field

    def locale_type_is_numeric(self, locale_type):
        """
        Return whether locale type is numeric.
        :param locale_type: one of standardized set {"county", "jurisdiction", etc.}
        :return: True if the DB column for this locale type is numeric, False otherwise
        """
        # This case fixes Ohio, because county is numeric but db field is text
        if (locale_type == 'county') and self.data['numeric_county']:
            return True

        locale_field = self.get_locale_field(locale_type)
        field_type = self.data['columns'][locale_field]
        if ("int" in field_type) or (field_type == "float") or \
          (field_type == "double"):
           return True
        else:
            return False

    def is_primary_locale_type(self, locale_type):
        """
        Return whether a locale type is the primary one for this state.
        :param locale_type: one of standardized set {"county", "jurisdiction", etc.}
        :return: True if locale_type is the state's primary one, False otherwise
        """
        if locale_type is not None:
            if self.primary_locale_type == locale_type:
                return True
        return False
