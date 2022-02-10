import os
import logging


LOG_LEVEL = int(os.environ.get("LOG_LEVEL", logging.INFO))


def setup_reggie():
    logging.basicConfig(level=LOG_LEVEL)

setup_reggie()


PRIMARY_LOCALE_ALIAS = "primary_locale_identifier"
PRIMARY_LOCALE_TYPE = "primary_locale_type"
PRIMARY_LOCALE_NAMES = "primary_locale_names"

REGGIE_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = REGGIE_PROJECT_DIR + "/configs/data/"
LOCALE_DIR = REGGIE_PROJECT_DIR + "/configs/{}/".format(PRIMARY_LOCALE_NAMES)
CONFIG_OHIO_FILE = CONFIG_DIR + "ohio.yaml"
CONFIG_CHUNK_URLS = "data_chunk_links"

RAW_FILE_PREFIX = "raw_voter_file"
PROCESSED_FILE_PREFIX = "voter_file"
MODIFICATION_FILE_PREFIX = "modification_file"

META_FILE_PREFIX = "meta"
NULL_CHAR = "n"
NO_PARTY_PLACEHOLDER = "npa"

MAX_MALFORMED_LINES_ALLOWED = 200
