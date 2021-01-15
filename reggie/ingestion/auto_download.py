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

# def nc_date_grab():
#     nc_file = urlopen(
#         'https://s3.amazonaws.com/dl.ncsbe.gov?delimiter=/&prefix=data/')
#     data = nc_file.read()
#     nc_file.close()
#     root = xml.etree.ElementTree.fromstring(data.decode('utf-8'))
#
#     def nc_parse_xml(file_name):
#         z = 0
#         for child in root.itertext():
#             # TT Is there a reason the if below is overindented?
#                 if z == 1:
#                     return child
#                 if file_name in child:
#                     z += 1
#
#     file_date_vf = nc_parse_xml(file_name="data/ncvoter_Statewide.zip")
#     file_date_his = nc_parse_xml(file_name="data/ncvhis_Statewide.zip")
#     if file_date_his[0:10] != file_date_vf[0:10]:
#         logging.info(
#             "Different dates between files, reverting to voter file date")
#     file_date_vf = parser.parse(file_date_vf).isoformat()[0:10]
#     return file_date_vf
#
#
# def ohio_get_last_updated():
#     html = requests.get("https://www6.ohiosos.gov/ords/f?p=VOTERFTP:STWD",
#                         verify=False).text
#     soup = bs4.BeautifulSoup(html, "html.parser")
#     results = soup.find_all("td", {"headers": "DATE_MODIFIED"})
#     return max(parser.parse(a.text) for a in results)
#
# def generate_key(self, file_class=PROCESSED_FILE_PREFIX):
#         if "native_file_extension" in self.config and \
#                 file_class != "voter_file":
#             k = generate_s3_key(file_class, self.state,
#                                 self.source, self.download_date,
#                                 self.config["native_file_extension"])
#         else:
#             k = generate_s3_key(file_class, self.state, self.source,
#                                 self.download_date, "csv", "gz")
#         return "testing/" + k if self.testing else k


# adding the two helper functions
# def s3_dump(state, file_item, meta=None, s3_bucket=None, file_class=PROCESSED_FILE_PREFIX):
#     if not isinstance(file_item, FileItem):
#         raise ValueError("'file_item' must be of type 'FileItem'")
#     if file_class != PROCESSED_FILE_PREFIX:
#         if state == 'ohio':
#             download_date = str(
#                 ohio_get_last_updated().isoformat())[0:10]
#         elif state == "north_carolina":
#             download_date = str(nc_date_grab())
#     meta = meta if meta is not None else {}
#     meta["last_updated"] = download_date
#     s3.Object(s3_bucket, generate_key(file_class=file_class)).put(
#         Body=file_item.obj, ServerSideEncryption='AES256')
#     if file_class != RAW_FILE_PREFIX:
#         s3.Object(self.s3_bucket, self.generate_key(
#             file_class=META_FILE_PREFIX) + ".json").put(
#             Body=json.dumps(meta), ServerSideEncryption='AES256')


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
        loader = Preprocessor(config_file=config_file, force_date=today,
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
        print("file_to_zip", file_to_zip)
        logging.info("Zipping files")
        with zipfile.ZipFile(file_to_zip, 'w') as myzip:
            for f in zipped_files:
                myzip.write(f)
        logging.info("Uploading")
        file_to_zip = FileItem(
            "OH file auto download",
            filename=file_to_zip,
            s3_bucket=s3_bucket)
        print("testing?")
        loader = Preprocessor(config_file=config_file, force_date=today,
                        s3_bucket=s3_bucket)
        raise ValueError("stopping?")
        # loader.s3_dump(file_to_zip, file_class=RAW_FILE_PREFIX)
