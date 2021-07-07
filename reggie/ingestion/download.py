import json
import os
import shutil
import sys
import uuid
import xml.etree.ElementTree

from bz2 import BZ2File
from datetime import datetime
from dateutil import parser
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
from subprocess import Popen, PIPE
from urllib.request import urlopen
from xlrd.book import XLRDError
from zipfile import ZipFile, BadZipfile
from gzip import GzipFile

import bs4
import numpy as np
import pandas as pd
import requests

from pandas.errors import ParserError

from reggie.configs.configs import Config
from reggie.ingestion.utils import (
    date_from_str,
    generate_s3_key,
    normalize_columns,
    s3,
    TooManyMalformedLines,
    MissingColumnsError,
    MissingFilesError,
)
from reggie.reggie_constants import (
    CONFIG_CHUNK_URLS,
    MAX_MALFORMED_LINES_ALLOWED,
    META_FILE_PREFIX,
    PROCESSED_FILE_PREFIX,
    RAW_FILE_PREFIX,
)


def ohio_get_last_updated():
    html = requests.get(
        "https://www6.ohiosos.gov/ords/f?p=VOTERFTP:STWD", verify=False
    ).text
    soup = bs4.BeautifulSoup(html, "html.parser")
    results = soup.find_all("td", {"headers": "DATE_MODIFIED"})
    return max(parser.parse(a.text) for a in results)


def nc_date_grab():
    nc_file = urlopen("https://s3.amazonaws.com/dl.ncsbe.gov?delimiter=/&prefix=data/")
    data = nc_file.read()
    nc_file.close()
    root = xml.etree.ElementTree.fromstring(data.decode("utf-8"))

    def nc_parse_xml(file_name):
        z = 0
        for child in root.itertext():
            if z == 1:
                return child
            if file_name in child:
                z += 1

    file_date_vf = nc_parse_xml(file_name="data/ncvoter_Statewide.zip")
    file_date_his = nc_parse_xml(file_name="data/ncvhis_Statewide.zip")
    if file_date_his[0:10] != file_date_vf[0:10]:
        logging.info("Different dates between files, reverting to voter file date")
    file_date_vf = parser.parse(file_date_vf).isoformat()[0:10]
    return file_date_vf


def get_object(key, fn, s3_bucket):
    with open(fn, "w+") as obj:
        s3.Bucket(s3_bucket).download_fileobj(Key=key, Fileobj=obj)


def get_object_mem(key, s3_bucket):
    file_obj = BytesIO()
    s3.Bucket(s3_bucket).download_fileobj(Key=key, Fileobj=file_obj)
    return file_obj


def concat_and_delete(in_list):
    outfile = StringIO()

    for f_obj in in_list:
        s = f_obj["obj"].read()
        outfile.write(s.decode())
    outfile.seek(0)
    return outfile


class ErrorLog(object):
    """
    Allow us to catch and count number of error lines skipped during read_csv,
    by redirecting stderr to this error log object.
    """

    def __init__(self):
        self.error_string = ""

    def write(self, data):
        self.error_string += data

    def count_skipped_lines(self):
        return self.error_string.count("Skipping")

    def print_log_string(self):
        logging.info(self.error_string)


class FileItem(object):
    """
    in this case, name is always a string and obj is a StringIO/BytesIO object
    """

    def __init__(self, name, key=None, filename=None, io_obj=None, s3_bucket=""):
        if not any([key, filename, io_obj]):
            raise ValueError(
                "must supply at least one key,"
                " filename, or io_obj but "
                "all are none"
            )
        if key is not None:
            self.obj = get_object_mem(key, s3_bucket)
        elif filename is not None:
            try:
                with open(filename) as f:
                    s = f.read()
                    self.obj = StringIO(s)
            except UnicodeDecodeError:
                with open(filename, "rb") as f:
                    s = f.read()
                    self.obj = BytesIO(s)
        else:
            self.obj = io_obj
        self.name = name

    def __str__(self):
        if isinstance(self.obj, StringIO) or isinstance(self.obj, BytesIO):
            s = len(self.obj.getvalue())
        else:
            s = "unknown"
        return "FileItem: name={}, obj={}, size={}".format(self.name, self.obj, s)


class Preprocessor:
    """
    A class used to contain the functions necessary for the state-specific preprocessors

    Attributes
    ----------
    raw_s3_file: str
        the name of the uploaded raw_s3_file
    config_file_path : str
        the file path for the config file for a specific state
    config : Config object
        the config object of the config_file_path
    chunk_urls : str
        the urls for data chunks
    file_type : str
        the expected filetype for a voter file
    source : str
        the source of the voter file
    is_compressed : bool
        whether or not the raw file is compressed
    checksum : str
    state : str
        the state having a this preprocessor object being created for
    meta : dict
        dictionary containing the meta data for a file
    testing : bool
        whether or not this is a test object
    ignore_checks : bool
        a flag to set if the number of files for a state is expected to be different than defined in the state yaml
    s3_bucket : str
        the s3 bucket to write to
    force_date : date
        sets the download_date to the given date or now if none
    force_file : str
    temp_files : list
        a list containing the temp files inside a voter file, will eventually be combined

    """

    def __init__(
        self,
        raw_s3_file,
        config_file,
        force_date=None,
        force_file=None,
        testing=False,
        ignore_checks=False,
        s3_bucket="",
        **kwargs
    ):

        # Init change begin (adding loader object)
        self.config_file_path = config_file
        self.config = Config(file_name=config_file)
        self.chunk_urls = (
            self.config[CONFIG_CHUNK_URLS] if CONFIG_CHUNK_URLS in self.config else []
        )
        if "tmp" not in os.listdir("/"):
            os.system("mkdir /tmp")
        self.file_type = self.config["file_type"]
        self.source = self.config["source"]
        self.is_compressed = False
        self.checksum = None
        self.state = self.config["state"]
        self.meta = None
        self.testing = testing
        self.ignore_checks = ignore_checks
        self.s3_bucket = s3_bucket
        if force_date is not None:
            self.download_date = parser.parse(force_date).isoformat()
        else:
            self.download_date = datetime.now().isoformat()
        if force_file is not None:
            working_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
            logging.info("copying {} to {}".format(force_file, working_file))
            shutil.copy2(force_file, working_file)
            self.main_file = FileItem(
                "loader_force_file", filename=working_file, s3_bucket=self.s3_bucket
            )
        else:
            self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())

        self.temp_files = [self.main_file]

        # Init change end
        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        self.raw_s3_file = raw_s3_file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def compress(self):
        """
        intended to be called after the consolidated (processed) file has been
        created and saved in self.main_file
        :param compression_type: gzip is default
        :return: None
        """
        if not self.is_compressed:
            logging.info("compressing")
            p = Popen(["gzip", "-c"], stdout=PIPE, stderr=PIPE, stdin=PIPE)
            op, err = p.communicate(self.main_file.obj.read().encode())
            self.main_file.obj.seek(0)
            self.is_compressed = True
            self.main_file.obj = BytesIO(op)

    @staticmethod
    def unzip_decompress(file_name):
        """
        handles decompression for .zip files
        :param file_name: .zip file
        :return: dictionary of file-like objects with their names as keys
        """
        zip_file = ZipFile(file_name)
        file_names = zip_file.namelist()
        logging.info("decompressing unzip {} into {}".format(file_name, file_names))
        file_objs = []
        for name in file_names:
            file_objs.append({"name": name, "obj": BytesIO(zip_file.read(name))})

        return file_objs

    @staticmethod
    def gunzip_decompress(file_obj, file_name):
        """
        handles decompression for .gz files
        :param file_name: .gz file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        gzip_file = GzipFile(fileobj=file_obj)
        try:
            return [
                {"name": file_name + "decompressed", "obj": BytesIO(gzip_file.read())}
            ]
        except OSError:
            return None

    @staticmethod
    def bunzip2_decompress(file_name):
        """
        handles decompression for .bz2 files
        :param file_name: .bz2 file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        logging.info(
            "decompressing {} {} to {}".format(
                "bunzip2", file_name, os.path.dirname(file_name)
            )
        )
        bz2_file = BZ2File(file_name)
        return [{"name": "decompressed_file", "obj": bz2_file}]

    @staticmethod
    def infer_compression(file_name):
        """
        infer file type and map to compression type
        :param file_name: file in question
        :return: string (de)compression type or None
        """
        if file_name[-3:] == "bz2":
            compression_type = "bunzip2"
        elif file_name[-2:] == "gz":
            compression_type = "gunzip"
        elif file_name[-3:] == "zip":
            compression_type = "unzip"
        else:
            compression_type = None
        if compression_type is None:
            logging.info("could not infer the file type of {}".format(file_name))
        logging.info("compression type of {} is {}".format(file_name, compression_type))
        return compression_type

    def decompress(self, s3_file_obj, compression_type="gunzip"):
        """
        decompress a file using either unzip or gunzip, unless the file is an
        .xlsx file, in which case it is returned as is (these files are
        compressed by default, and are unreadable in their unpacked form by
        pandas)
        :param s3_file_obj: the path of the file to be decompressed
        :param compression_type: available options - ["unzip", "gunzip"]
        :return: a (str, bool) tuple containing the location of the processed
        file and whether or not it was actually
        decompressed
        """

        new_files = None
        inferred_compression = self.infer_compression(s3_file_obj["name"])
        if compression_type == "infer":
            if inferred_compression is not None:
                compression_type = inferred_compression
            else:
                compression_type = "unzip"
        logging.info(
            "decompressing {} using {}".format(s3_file_obj["name"], compression_type)
        )

        if (
            (s3_file_obj["name"].split(".")[-1].lower() == "xlsx")
            or (s3_file_obj["name"].split(".")[-1].lower() == "txt")
            or (s3_file_obj["name"].split(".")[-1].lower() == "pdf")
            or (s3_file_obj["name"].split(".")[-1].lower() == "png")
            or ("MACOS" in s3_file_obj["name"])
        ):
            logging.info("did not decompress {}".format(s3_file_obj["name"]))
            raise BadZipfile
        else:
            # convert to
            if isinstance(s3_file_obj["obj"], StringIO):
                bytes_obj = BytesIO(s3_file_obj["obj"].read().encode())
            else:
                bytes_obj = s3_file_obj["obj"]
            if compression_type == "unzip":
                new_files = self.unzip_decompress(bytes_obj)
            elif compression_type == "bunzip2":
                new_files = self.bunzip2_decompress(bytes_obj)
            else:
                new_files = self.gunzip_decompress(bytes_obj, s3_file_obj["name"])

            if compression_type is not None and new_files is not None:
                logging.info("decompression done: {}".format(s3_file_obj))
            else:
                logging.info("did not decompress {}".format(s3_file_obj))

        self.is_compressed = False
        return new_files

    def generate_key(self, file_class=PROCESSED_FILE_PREFIX):
        if "native_file_extension" in self.config and file_class != "voter_file":
            k = generate_s3_key(
                file_class,
                self.state,
                self.source,
                self.download_date,
                self.config["native_file_extension"],
            )
        else:
            k = generate_s3_key(
                file_class, self.state, self.source, self.download_date, "csv", "gz"
            )
        return "testing/" + k if self.testing else k

    def s3_dump(self, file_item, file_class=PROCESSED_FILE_PREFIX):
        if not isinstance(file_item, FileItem):
            raise ValueError("'file_item' must be of type 'FileItem'")
        if file_class != PROCESSED_FILE_PREFIX:
            if self.config["state"] == "ohio":
                self.download_date = str(ohio_get_last_updated().isoformat())[0:10]
            elif self.config["state"] == "north_carolina":
                self.download_date = str(nc_date_grab())
        meta = self.meta if self.meta is not None else {}
        meta["last_updated"] = self.download_date
        s3.Object(self.s3_bucket, self.generate_key(file_class=file_class)).put(
            Body=file_item.obj, ServerSideEncryption="AES256"
        )
        if file_class != RAW_FILE_PREFIX:
            s3.Object(
                self.s3_bucket, self.generate_key(file_class=META_FILE_PREFIX) + ".json"
            ).put(Body=json.dumps(meta), ServerSideEncryption="AES256")

    def generate_local_key(self, meta=False):
        if meta:
            name = "meta_" + self.state + "_" + self.download_date + ".json"
        else:
            name = self.state + "_" + self.download_date + ".csv.gz"
        return name

    @staticmethod
    def output_dataframe(file_item):
        return pd.read_csv(file_item.obj)

    def local_dump(self, file_item):
        df = self.output_dataframe(file_item)
        df.to_csv(self.generate_local_key(), compression="gzip")
        with open(self.generate_local_key(meta=True), "w") as fp:
            json.dump(self.meta, fp)

    # End old loader functions

    def s3_download(self):
        name = "/tmp/voteshield_{}".format(self.raw_s3_file.split("/")[-1])
        return FileItem(key=self.raw_s3_file, name=name, s3_bucket=self.s3_bucket)

    def unpack_files(self, file_obj, compression="unzip"):
        all_files = []

        def filter_unnecessary_files(files):
            unnecessary = [".png", "MACOS", "DS_Store", ".pdf", ".mdb", ".xml", ".rels"]
            for item in unnecessary:
                files = [n for n in files if item not in n["name"]]
            return files

        def expand_recurse(s3_file_objs):
            for f in s3_file_objs:
                if f["name"][-1] != "/":
                    try:
                        decompressed_result = self.decompress(
                            f, compression_type=compression
                        )
                        if decompressed_result is not None:
                            print("decompression ok for {}".format(f))
                            expand_recurse(decompressed_result)
                        else:
                            print("decomp returned none for {}".format(f))
                    except BadZipfile as e:
                        print("decompression failed for {}".format(f))
                        all_files.append(f)

        if type(self.main_file) == str:
            expand_recurse([{"name": self.main_file, "obj": open(self.main_file)}])
        else:
            expand_recurse([{"name": self.main_file.name, "obj": self.main_file.obj}])
        if "format" in self.config and "ignore_files" in self.config["format"]:
            all_files = [
                n
                for n in all_files
                if list(n.keys())[0] not in self.config["format"]["ignore_files"]
                and os.path.basename(list(n.keys())[0])
                not in self.config["format"]["ignore_files"]
            ]

        all_files = filter_unnecessary_files(all_files)

        for n in all_files:
            if type(n["obj"]) != str:
                n["obj"].seek(0)
        self.temp_files.extend(all_files)
        logging.info("unpacked: - {}".format(all_files))
        return all_files

    def concat_file_segments(self, file_names):
        """
        Serially concatenates the "file segments" into a single csv file.
        Should use this method when config["segmented_files"] is true. Should
        NOT be used to deal with files separated by column. Concatenates the
        files into self.main_file
        :param file_names: files to concatenate
        """

        first_success = False
        last_headers = None

        def list_compare(a, b):
            i = 0
            for j, k in zip(a, b):
                if j != k:
                    return j, k, i
                i += 1
            return False

        lengths = {}

        for f in file_names:
            lengths[f["name"]] = f["obj"].seek(SEEK_END)
            f["obj"].seek(SEEK_SET)

        file_names = sorted(file_names, key=lambda x: lengths[x["name"]], reverse=True)
        outfile = StringIO()
        for f in file_names:
            try:
                if self.config["file_type"] == "xlsx":
                    df = pd.read_excel(f["obj"])
                else:
                    df = pd.read_csv(f["obj"])
            except (XLRDError, ParserError):
                logging.info(
                    "Skipping {} ... Unsupported format, or corrupt "
                    "file".format(f["name"])
                )
                continue
            if not first_success:
                last_headers = sorted(df.columns)
            df, _ = normalize_columns(df, last_headers)
            if list_compare(last_headers, sorted(df.columns)):
                mismatched_headers = list_compare(last_headers, df.columns)
                raise ValueError(
                    "file chunks contained different or "
                    "misaligned headers: {} != {} at index {}".format(
                        *mismatched_headers
                    )
                )
            s = df.to_csv(header=not first_success, encoding="utf-8")
            first_success = True
            outfile.write(s)

        outfile.seek(0)
        return outfile

    @staticmethod
    def read_csv_count_error_lines(file_obj, **kwargs):
        """
        Run pandas read_csv while redirecting stderr so we can keep a
        count of how many lines are malformed without erroring out.
        :param file_obj: file object to be read
        :param **kwargs: kwargs for read_csv()
        :return: dataframe read from file
        """
        sys.stderr.flush()
        original_stderr = sys.stderr

        num_skipped = 0
        df = None
        try:
            sys.stderr = ErrorLog()
            df = pd.read_csv(file_obj, **kwargs)
            num_skipped = sys.stderr.count_skipped_lines()
            sys.stderr.print_log_string()  # still print original warning output
            sys.stderr = original_stderr
        except UnicodeDecodeError:
            sys.stderr = original_stderr
            raise
        except Exception as e:
            logging.error("{}: {}".format(type(e), e))
            sys.stderr = original_stderr

        if num_skipped > 0:
            logging.info(
                "WARNING: pandas.read_csv() skipped a total of {} lines, "
                "which had an unexpected number of fields."
                "".format(num_skipped)
            )

        if num_skipped > MAX_MALFORMED_LINES_ALLOWED:

            raise TooManyMalformedLines(
                "ERROR: Because pandas.read_csv() skipped more than {} lines, "
                "aborting file preprocess. Please manually examine the file "
                "to see if the formatting is as expected.".format(
                    MAX_MALFORMED_LINES_ALLOWED
                )
            )

        return df

    @staticmethod
    def reconcile_columns(df, expected_cols):
        for c in expected_cols:
            if c not in df.columns:
                df[c] = np.nan
        for c in df.columns:
            if c not in expected_cols:
                df.drop(columns=[c], inplace=True)
        return df

    def file_check(self, voter_files, hist_files=None):
        expected_voter = self.config["expected_number_of_files"]
        if hist_files:
            expected_hist = self.config["expected_number_of_hist_files"]
            if expected_hist != hist_files:
                raise MissingFilesError(
                    "{} state is missing history files".format(self.state),
                    self.state,
                    expected_hist,
                    hist_files,
                )

        if expected_voter != voter_files:
            logging.info(
                "Incorrect number of voter files found, expected {}, found {}".format(
                    expected_voter, voter_files
                )
            )
            raise MissingFilesError(
                "{} state is missing voter files".format(self.state),
                self.state,
                expected_voter,
                voter_files,
            )

    def column_check(self, current_columns, expected_columns=None):

        if expected_columns is None:
            expected_columns = self.config["ordered_columns"]

        extra_cols = []
        unexpected_columns = list(set(current_columns) - set(expected_columns))
        missing_columns = list(set(expected_columns) - set(current_columns))

        if set(current_columns) > set(expected_columns):
            # This is the case if there are more columns than expected, this won't cause the system to break but
            # might be worth looking in to
            logging.info(
                "more columns than expected detected, the current columns contain the expected "
                "columns along with these extra columns {}".format(unexpected_columns)
            )
            return unexpected_columns
        elif set(current_columns) != set(expected_columns):
            logging.info(
                "columns expected not found in current columns: {}".format(
                    missing_columns
                )
            )
            raise MissingColumnsError(
                "{} state is missing columns".format(self.state),
                self.state,
                expected_columns,
                missing_columns,
                unexpected_columns,
                current_columns,
            )

        return extra_cols

    # Preprocessors begin here


if __name__ == "__main__":
    print(ohio_get_last_updated())
