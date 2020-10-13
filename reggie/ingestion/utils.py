import boto3
import json
import re
import logging

from dateutil import parser
from botocore.exceptions import ClientError

from reggie.configs.configs import Config
from reggie.reggie_constants import META_FILE_PREFIX, NULL_CHAR, \
    PROCESSED_FILE_PREFIX, RAW_FILE_PREFIX


s3 = boto3.resource("s3")


class MissingElectionCodesError(Exception):
    pass


class MissingColumnsError(Exception):
    def __init__(self, message, state, expected_columns, missing_columns, unexpected_columns, current_columns):
        self.message = message
        self.state = state
        self.expected_columns = expected_columns
        self.missing_columns = missing_columns
        self.unexpected_columns = unexpected_columns
        self.current_columns = current_columns

    def __str__(self):
        return self.message


class TooManyMalformedLines(Exception):
    pass


def generate_s3_key(file_class, state, source, download_date, file_type,
                    compression=None, testing=False):
    return "{}/{}/{}/{}.{}{}".format(file_class, state,
                                     source, download_date,
                                     file_type, "." + compression if
                                     compression is not None else "")


def date_from_str(s):
    """
    return the <date> component from a filename or s3 key following the
    standard format:

        voter_file/<state>/<source>/<date>.<file_extension>.gz

    :return date string
    """
    match = re.search(r'(\d+[-_/]\d+[-_/]\d+)', s) if s is not None else None
    if match is not None:
        date = str(match.group(1))
    else:
        date = None
    return date


def df_to_postgres_array_string(df, cols, delim=","):
    """
    convert a matrix of values (defined by df + cols) into list of strings
    which can be inserted as array objects
    into postgres
    :param df: pandas DataFrame object
    :param cols: list of strings
    :param delim: delimiter for string arrays (default = ",")
    :return: list of strings, each defining an array to be inserted into
    postgres
    """
    concat_matrix = df[cols].values
    concat_matrix[concat_matrix != concat_matrix] = NULL_CHAR
    concat_array = "{" + concat_matrix[:, 0].copy()
    for i, _ in enumerate(cols[1:]):
        concat_array += delim + concat_matrix[:, i + 1]
    concat_array += "}"
    return concat_array


def strcol_to_array(str_col, delim=","):
    """

    :param str_col:
    :param delim:
    :return:
    """
    return str_col.str.replace(" ", "_").str.replace("[", "").str\
        .replace("]", "").str.split(delim)


def get_s3_uploads(state, file_class, source, s3_bucket, testing=False):
    """
    returns any files uploaded to s3 for a state, fileclass, source, and
    whether or not you are testing
    :param state:
    :param file_class:
    :param source:
    :param testing:
    :return:
    """
    assert(file_class in [PROCESSED_FILE_PREFIX, RAW_FILE_PREFIX,
                          META_FILE_PREFIX])
    if not testing:
        prefix = "{}/{}/{}".format(file_class, state, source)
    else:
        prefix = "testing/{}/{}/".format(file_class, state)
    keys = [a for a in s3.Bucket(s3_bucket).objects.filter(Prefix=prefix)
            if a.key[-1] != "/"]
    return keys


def get_processed_s3_uploads(state, s3_bucket, testing=False):
    configs = Config(state=state)
    keys = get_s3_uploads(configs["state"], configs["file_class"],
                          configs["source"], s3_bucket, testing)
    return keys


def pull_sorted_upload_keys(state, s3_bucket, testing=False):
    keys = get_processed_s3_uploads(state, s3_bucket, testing)
    return sorted([k.key for k in keys],
                  key=lambda x: parser.parse(date_from_str(x)))


def get_surrounding_dates(date,
                          state,
                          s3_bucket,
                          testing=False):
    this_date = date
    pre_date = None
    post_date = None
    keys = pull_sorted_upload_keys(state, s3_bucket, testing=testing)

    pre_key = None
    post_key = None

    key_list = list(
        filter(
            lambda x: parser.parse(date_from_str(x)).date() != this_date,
            keys))
    dates = list(
        filter(
            lambda x: x != this_date, [parser.parse(date_from_str(k)).date()
                                       for k in keys]))

    # iterate thru all processed file dates
    for i, d in enumerate(dates):

        # this_date is earlier than any snapshot file
        if i == 0 and this_date < d:
            post_date = d
            post_key = key_list[i]

        # this_date is later than any snapshot file
        elif i == len(dates) - 1 and this_date > d:
            pre_date = d
            pre_key = key_list[i]

        # this_date is between dates[i -1] and d
        elif i > 0 and dates[i - 1] < this_date < d:
            pre_date = dates[i - 1]
            post_date = d
            pre_key = key_list[i - 1]
            post_key = key_list[i]

    # in the special case where we are trying insert the first file for a
    # state, this method returns (None, None)
    return pre_date, post_date, pre_key, post_key


def get_metadata_for_key(k, s3_bucket):
    """
    Get complimentary metadata for an s3 object.
    :param k: a processed file
    :return:
    """
    if k[-4:] == 'json':
        meta_key = k
        meta = {}
    else:
        obj = s3.Object(s3_bucket, k).get()
        dir_array = k.split("/")
        if dir_array[0] == "testing":
            k_0 = "/".join(k.split("/")[2:])
            meta_key = "testing/{}/{}.json".format(META_FILE_PREFIX, k_0)
        else:
            k_0 = "/".join(k.split("/")[1:])
            meta_key = "{}/{}.json".format(META_FILE_PREFIX, k_0)
        meta = obj["Metadata"]

    try:
        meta_obj = s3.Object(s3_bucket, meta_key).get()
        meta_temp = json.loads(meta_obj["Body"].read().decode("utf-8") )
        for k in meta_temp:
            if type(meta_temp[k]) == str\
                    and k not in ["message", "last_updated"]:
                meta_temp[k] = json.loads(meta_temp[k])
        meta.update(meta_temp)
    except (ClientError, ValueError) as e:
        print(e)
        if isinstance(e, ValueError) or e.response['Error']['Code'] == \
                'NoSuchKey':
            logging.info("could not load metadata from {}, attempted to "
                         "load from built-in metadata store on  "
                         "object".format(meta_key))
        else:
            raise e

    return meta


def format_column_name(c):
    """
    Switch a column name into a postgres compatible format, apply any
    additional stylistic preferences
    :param c: non-formatted column
    :return: formatted column
    """
    return c.replace("-", "_").replace("(", "").replace(")", "")\
        .replace(" ", "_").lower()


def normalize_columns(df, cols, types=None):
    missing_cols = [c for c in cols if c not in df.columns.values.tolist()]
    wanted_cols = [c for c in df.columns.values.tolist() if c in cols]
    df = df[wanted_cols]
    for c in missing_cols:
        df[c] = None
    common_cols = [c for c in cols if c in df.columns.values.tolist()]
    df = df[common_cols]
    return df, common_cols

