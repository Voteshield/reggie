from reggie.configs.configs import Config
import requests
import zipfile
from reggie.ingestion.download import FileItem, ohio_get_last_updated, nc_date_grab
from reggie.reggie_constants import RAW_FILE_PREFIX, PROCESSED_FILE_PREFIX, META_FILE_PREFIX
import logging
from reggie.ingestion.utils import s3
import json
from dateutil import parser
import bs4
from urllib.request import urlopen
from reggie.ingestion.download import Preprocessor


def state_download(state, s3_bucket):
    config_file = Config.config_file_from_state(state=state)
    configs = Config(file_name=config_file)

    if state == "north_carolina":
        today = nc_date_grab()
        list_files = configs['data_chunk_links']
        zipped_files = []
        for i, url in enumerate(list_files):
            target_path = "/tmp/" + state + str(i) + ".zip"
            zipped_files.append(target_path)
            response = requests.get(url, stream=True)
            handle = open(target_path, "wb")
            for chunk in response.iter_content(chunk_size=512):
                if chunk:
                    handle.write(chunk)
            handle.close()
        file_to_zip = today + ".zip"
        with zipfile.ZipFile(file_to_zip, 'w') as myzip:
            for f in zipped_files:
                myzip.write(f)
        file_to_zip = FileItem(
            "NC file auto download",
            filename=file_to_zip,
            s3_bucket=s3_bucket)
        loader = Preprocessor(config_file=config_file, raw_s3_file=None, force_date=today,
                        s3_bucket=s3_bucket)
        loader.s3_dump(file_to_zip, file_class=RAW_FILE_PREFIX)

    elif state == "ohio":
        today = str(ohio_get_last_updated().isoformat())[0:10]
        list_files = configs['data_chunk_links']
        file_names = configs['data_file_names']
        zipped_files = []
        for i, url in enumerate(list_files):
            logging.info("downloading {} file".format(url))
            target_path = "/tmp/" + state + "_" + file_names[i] + ".txt.gz"
            zipped_files.append(target_path)
            response = requests.get(url, stream=True, verify=False)
            handle = open(target_path, "wb")
            for chunk in response.iter_content(chunk_size=512):
                if chunk:
                    handle.write(chunk)
            handle.close()
            logging.info("downloaded {} file".format(url))
        file_to_zip = today + ".zip"
        logging.info("Zipping files")
        with zipfile.ZipFile(file_to_zip, 'w') as myzip:
            for f in zipped_files:
                myzip.write(f)
        logging.info("Uploading")
        file_to_zip = FileItem(
            "OH file auto download",
            filename=file_to_zip,
            s3_bucket=s3_bucket)
        loader = Preprocessor(config_file=config_file, raw_s3_file=None, force_date=today,
                        s3_bucket=s3_bucket)
        loader.s3_dump(file_to_zip, file_class=RAW_FILE_PREFIX)
