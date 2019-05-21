import os
import logging

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = PROJECT_DIR + "/configs/data/"
LOG_LEVEL = int(os.environ.get("LOG_LEVEL", logging.INFO))


def setup():
	logging.basicConfig(level=LOG_LEVEL)
	
setup()
