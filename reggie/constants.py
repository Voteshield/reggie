import os
import logging

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = PROJECT_DIR + "/configs/data/"
LOG_LEVEL = int(os.environ.get("LOG_LEVEL", logging.INFO))
PRIMARY_LOCALE_TYPE = "primary_locale_type"
COUNTY_ALIAS = "county_identifier"


def setup():
	logging.basicConfig(level=LOG_LEVEL)
	
setup()
