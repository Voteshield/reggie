import os
import logging


LOG_LEVEL = int(os.environ.get("LOG_LEVEL", logging.INFO))


def setup_reggie():
    logging.basicConfig(level=LOG_LEVEL)

setup_reggie()


PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = PROJECT_DIR + "/configs/data/"
LOCALE_DIR = PROJECT_DIR + "/configs/{}/".format(PRIMARY_LOCALE_NAMES)

PRIMARY_LOCALE_ALIAS = "primary_locale_identifier"
LOCALE_TYPE = "locale_type"
PRIMARY_LOCALE_TYPE = "primary_locale_type"
PRIMARY_LOCALE_NAMES = "primary_locale_names"

ADDRESS_CHANGE_TYPE = "address"
NAME_CHANGE_TYPE = "name"
REMOVAL = "removal"
REGISTRATION = "registration"
ACTIVATION_CHANGE_TYPE = "activated"
DEACTIVATION_CHANGE_TYPE = "deactivated"
PARTY_CHANGE_TYPE = "party"
BIRTHDAY_CHANGE_TYPE = "birthday"
EARLY_VOTER_CHANGE_TYPE = "early_voter"

CORE_CHANGE_TYPES = [ADDRESS_CHANGE_TYPE, NAME_CHANGE_TYPE, REMOVAL,
                     REGISTRATION, ACTIVATION_CHANGE_TYPE,
                     DEACTIVATION_CHANGE_TYPE, PARTY_CHANGE_TYPE,
                     BIRTHDAY_CHANGE_TYPE]
