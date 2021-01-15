import uuid
from datetime import datetime
from subprocess import Popen, PIPE
from datetime import date as dt
import pandas as pd
from dateutil import parser
import json
from reggie.reggie_constants import *
from reggie.configs.configs import Config
from reggie.ingestion.utils import date_from_str, df_to_postgres_array_string, \
    format_column_name, generate_s3_key, get_metadata_for_key, \
    get_surrounding_dates, MissingElectionCodesError, normalize_columns, \
    s3, strcol_to_array, TooManyMalformedLines, MissingColumnsError, MissingFilesError, MissingNumColumnsError
from xlrd.book import XLRDError
from pandas.io.parsers import ParserError
import shutil
import numpy as np
import subprocess
import gc
from zipfile import ZipFile, BadZipfile
from gzip import GzipFile
from bz2 import BZ2File
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
import bs4
import requests
from urllib.request import urlopen
import xml.etree.ElementTree
import os
import sys


def ohio_get_last_updated():
    html = requests.get("https://www6.ohiosos.gov/ords/f?p=VOTERFTP:STWD",
                        verify=False).text
    soup = bs4.BeautifulSoup(html, "html.parser")
    results = soup.find_all("td", {"headers": "DATE_MODIFIED"})
    return max(parser.parse(a.text) for a in results)


def nc_date_grab():
    nc_file = urlopen(
        'https://s3.amazonaws.com/dl.ncsbe.gov?delimiter=/&prefix=data/')
    data = nc_file.read()
    nc_file.close()
    root = xml.etree.ElementTree.fromstring(data.decode('utf-8'))

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
        logging.info(
            "Different dates between files, reverting to voter file date")
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
        self.error_string = ''

    def write(self, data):
        self.error_string += data

    def count_skipped_lines(self):
        return self.error_string.count('Skipping')

    def print_log_string(self):
        logging.info(self.error_string)


class FileItem(object):
    """
    in this case, name is always a string and obj is a StringIO/BytesIO object
    """

    def __init__(self, name, key=None, filename=None, io_obj=None, s3_bucket=""):
        if not any([key, filename, io_obj]):
            raise ValueError("must supply at least one key,"
                             " filename, or io_obj but "
                             "all are none")
        if key is not None:
            self.obj = get_object_mem(key, s3_bucket)
        elif filename is not None:
            try:
                with open(filename) as f:
                    s = f.read()
                    self.obj = StringIO(s)
            except UnicodeDecodeError:
                with open(filename, 'rb') as f:
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
        return "FileItem: name={}, obj={}, size={}" \
            .format(self.name, self.obj, s)


class Loader(object):
    """
    this object should be used to perform downloads directly from
    online resources which are specified by yaml files
    in the config directory.

    A Loader uses filesystem resources for temporarily storing the files
    on disk during the chunk concatenation process,
    therefore __enter__ and __exit__ are defined to allow safe usage
    of Loader in a 'with' statement:
    for example:
    ```
        with Loader() as l:
            l.download_chunks()
            l.s3_dump()
    ```
    """

    def __init__(self, config_file=CONFIG_OHIO_FILE, force_date=None,
                 force_file=None, testing=False, ignore_checks=False, s3_bucket=""):
        self.config_file_path = config_file
        config = Config(file_name=config_file)
        self.config = config
        self.chunk_urls = config[
            CONFIG_CHUNK_URLS] if CONFIG_CHUNK_URLS in config else []
        if "tmp" not in os.listdir("/"):
            os.system("mkdir /tmp")
        self.file_type = config["file_type"]
        self.source = config["source"]
        self.is_compressed = False
        self.checksum = None
        self.state = config["state"]
        self.obj_will_download = False
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
                "loader_force_file",
                filename=working_file,
                s3_bucket=self.s3_bucket)
        else:
            self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())

        self.temp_files = [self.main_file]

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
            p = Popen(["gzip", "-c"], stdout=PIPE,
                      stderr=PIPE, stdin=PIPE)
            op, err = p.communicate(self.main_file.obj.read().encode())
            self.main_file.obj.seek(0)
            self.is_compressed = True
            self.main_file.obj = BytesIO(op)

    def unzip_decompress(self, file_name):
        """
        handles decompression for .zip files
        :param file_name: .zip file
        :return: dictionary of file-like objects with their names as keys
        """
        zip_file = ZipFile(file_name)
        file_names = zip_file.namelist()
        logging.info("decompressing unzip {} into {}".format(file_name,
                                                             file_names))
        file_objs = []
        for name in file_names:
            file_objs.append({"name": name,
                              "obj": BytesIO(zip_file.read(name))})

        return file_objs

    def gunzip_decompress(self, file_obj, file_name):
        """
        handles decompression for .gz files
        :param file_name: .gz file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        gzip_file = GzipFile(fileobj=file_obj)
        try:
            return [{"name": file_name + "decompressed",
                     "obj": BytesIO(gzip_file.read())}]
        except OSError:
            return None

    def bunzip2_decompress(self, file_name):
        """
        handles decompression for .bz2 files
        :param file_name: .bz2 file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        logging.info("decompressing {} {} to {}"
                     .format("bunzip2",
                             file_name,
                             os.path.dirname(file_name)))
        bz2_file = BZ2File(file_name)
        return [{"name": "decompressed_file", "obj": bz2_file}]

    def infer_compression(self, file_name):
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
            logging.info(
                "could not infer the file type of {}".format(file_name))
        logging.info("compression type of {} is {}".format(
            file_name, compression_type))
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
                compression_type = 'unzip'
        logging.info("decompressing {} using {}".format(s3_file_obj["name"],
                                                        compression_type))

        if (s3_file_obj["name"].split(".")[-1].lower() == "xlsx") or \
                (s3_file_obj["name"].split(".")[-1].lower() == "txt") or \
                (s3_file_obj["name"].split(".")[-1].lower() == "pdf") or \
                (s3_file_obj["name"].split(".")[-1].lower() == "png") or \
                ("MACOS" in s3_file_obj["name"]):
                # why was csv removed?
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
                new_files = self.gunzip_decompress(bytes_obj,
                                                   s3_file_obj["name"])

            if compression_type is not None and new_files is not None:
                logging.info("decompression done: {}".format(s3_file_obj))
            else:
                logging.info("did not decompress {}".format(s3_file_obj))

        self.is_compressed = False
        return new_files

    def generate_key(self, file_class=PROCESSED_FILE_PREFIX):
        if "native_file_extension" in self.config and \
                file_class != "voter_file":
            k = generate_s3_key(file_class, self.state,
                                self.source, self.download_date,
                                self.config["native_file_extension"])
        else:
            k = generate_s3_key(file_class, self.state, self.source,
                                self.download_date, "csv", "gz")
        return "testing/" + k if self.testing else k

    def s3_dump(self, file_item, file_class=PROCESSED_FILE_PREFIX):
        if not isinstance(file_item, FileItem):
            raise ValueError("'file_item' must be of type 'FileItem'")
        if file_class != PROCESSED_FILE_PREFIX:
            if self.config["state"] == 'ohio':
                self.download_date = str(
                    ohio_get_last_updated().isoformat())[0:10]
            elif self.config["state"] == "north_carolina":
                self.download_date = str(nc_date_grab())
        meta = self.meta if self.meta is not None else {}
        meta["last_updated"] = self.download_date
        s3.Object(self.s3_bucket, self.generate_key(file_class=file_class)).put(
            Body=file_item.obj, ServerSideEncryption='AES256')
        if file_class != RAW_FILE_PREFIX:
            s3.Object(self.s3_bucket, self.generate_key(
                file_class=META_FILE_PREFIX) + ".json").put(
                Body=json.dumps(meta), ServerSideEncryption='AES256')

    def generate_local_key(self, meta=False):
        if meta:
            name = "meta_" + self.state + "_" + self.download_date + ".json"
        else:
            name = self.state + "_" + self.download_date + ".csv.gz"
        return name

    def output_dataframe(self, file_item):
        return pd.read_csv(file_item.obj)

    def local_dump(self, file_item):
        df = self.output_dataframe(file_item)
        df.to_csv(self.generate_local_key(), compression='gzip')
        with open(self.generate_local_key(meta=True), 'w') as fp:
            json.dump(self.meta, fp)


class Preprocessor(Loader):
    def __init__(self, raw_s3_file, config_file, force_date=None, **kwargs):

        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        super(Preprocessor, self).__init__(
            config_file=config_file, force_date=force_date,
            **kwargs)
        self.raw_s3_file = raw_s3_file

        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

    def s3_download(self):
        name = "/tmp/voteshield_{}" \
            .format(self.raw_s3_file.split("/")[-1])

        return FileItem(key=self.raw_s3_file,
                        name=name,
                        s3_bucket=self.s3_bucket)

    def unpack_files(self, file_obj, compression="unzip"):
        all_files = []

        def filter_unnecessary_files(files):
            unnecessary = [".png", "MACOS", "DS_Store", ".pdf", ".mdb"]
            for item in unnecessary:
                files = [n for n in files if item not in n["name"]]
            return files

        def expand_recurse(s3_file_objs):
            for f in s3_file_objs:
                if f["name"][-1] != "/":
                    try:
                        decompressed_result = self.decompress(
                            f, compression_type=compression)
                        if decompressed_result is not None:
                            print('decompression ok for {}'.format(f))
                            expand_recurse(decompressed_result)
                        else:
                            print('decomp returned none for {}'.format(f))
                    except BadZipfile as e:
                        print('decompression failed for {}'.format(f))
                        all_files.append(f)

        if type(self.main_file) == str:
            expand_recurse([{"name": self.main_file,
                             "obj": open(self.main_file)}])
        else:
            expand_recurse([{"name": self.main_file.name,
                             "obj": self.main_file.obj}])
        if "format" in self.config and "ignore_files" in self.config["format"]:
            all_files = [n for n in all_files if list(n.keys())[0] not in
                         self.config["format"]["ignore_files"] and
                         os.path.basename(list(n.keys())[0]) not in
                         self.config["format"]["ignore_files"]]

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

        file_names = sorted(file_names, key=lambda x: lengths[x["name"]],
                            reverse=True)
        outfile = StringIO()
        for f in file_names:
            try:
                if self.config["file_type"] == 'xlsx':
                    df = pd.read_excel(f["obj"])
                else:
                    df = pd.read_csv(f["obj"])
            except (XLRDError, ParserError):
                logging.info("Skipping {} ... Unsupported format, or corrupt "
                             "file".format(f["name"]))
                continue
            if not first_success:
                last_headers = sorted(df.columns)
            df, _ = normalize_columns(df, last_headers)
            if list_compare(last_headers, sorted(df.columns)):
                mismatched_headers = list_compare(last_headers, df.columns)
                raise ValueError("file chunks contained different or "
                                 "misaligned headers: {} != {} at index {}"
                                 .format(*mismatched_headers))
            s = df.to_csv(header=not first_success, encoding='utf-8')
            first_success = True
            outfile.write(s)

        outfile.seek(0)
        return outfile

    def read_csv_count_error_lines(self, file_obj, **kwargs):
        """
        Run pandas read_csv while redirecting stderr so we can keep a
        count of how many lines are malformed without erroring out.
        :param file_obj: file object to be read
        :param **kwargs: kwargs for read_csv()
        :return: dataframe read from file
        """
        sys.stderr.flush()
        original_stderr = sys.stderr

        try:
            sys.stderr = ErrorLog()
            df = pd.read_csv(file_obj, **kwargs)
            num_skipped = sys.stderr.count_skipped_lines()
            sys.stderr.print_log_string()   # still print original warning output
            sys.stderr = original_stderr
        except Exception as e:
            logging.error(e)

        if num_skipped > 0:
            logging.info(
                "WARNING: pandas.read_csv() skipped a total of {} lines, " \
                "which had an unexpected number of fields.""".format(
                    num_skipped))

        if num_skipped > MAX_MALFORMED_LINES_ALLOWED:

            raise TooManyMalformedLines(
                "ERROR: Because pandas.read_csv() skipped more than {} lines, " \
                "aborting file preprocess. Please manually examine the file " \
                "to see if the formatting is as expected.".format(
                    MAX_MALFORMED_LINES_ALLOWED))

        return df

    def reconcile_columns(self, df, expected_cols):
        for c in expected_cols:
            if c not in df.columns:
                df[c] = np.nan
        for c in df.columns:
            if c not in expected_cols:
                df.drop(columns=[c], inplace=True)
        return df

    def file_check(self,  voter_files, hist_files=None):
        expected_voter = self.config["expected_number_of_files"]
        if hist_files:
            expected_hist = self.config["expected_number_of_hist_files"]
            if expected_hist != hist_files:
                raise MissingFilesError("{} state is missing history files".format(self.state), self.state,
                                          expected_hist, hist_files)

        if expected_voter != voter_files:
            logging.info("Incorrect number of voter files found, expected {}, found {}".format(expected_voter,
                                                                                               voter_files))
            raise MissingFilesError("{} state is missing voter files".format(self.state), self.state,
                                      expected_voter, voter_files)

    def column_check(self, current_columns, expected_columns=None):

        if expected_columns is None:
            expected_columns = self.config["ordered_columns"]

        extra_cols = []
        unexpected_columns = list(set(current_columns) - set(expected_columns))
        missing_columns = list(set(expected_columns) - set(current_columns))


        if set(current_columns) > set(expected_columns):
            # This is the case if there are more columns than expected, this won't cause the system to break but
            # might be worth looking in to
            logging.info("more columns than expected detected, the current columns contain the expected "
                         "columns along with these extra columns {}".format(unexpected_columns))
            return unexpected_columns
        elif set(current_columns) != set(expected_columns):
            logging.info("columns expected not found in current columns: {}".format(missing_columns))
            raise MissingColumnsError("{} state is missing columns".format(self.state), self.state,
                                      expected_columns, missing_columns, unexpected_columns,
                                      current_columns)

        return extra_cols

    def preprocess_texas(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression='unzip')

        if not self.ignore_checks:
            self.file_check(len(new_files))
        widths_one = [3, 10, 10, 50, 50, 50, 50,
                      4, 1, 8, 9, 12, 2, 50, 12,
                      2, 12, 12, 50, 9, 110, 50,
                      50, 20, 20, 8, 1, 1, 8, 2, 3, 6]
        widths_two = [3, 4, 10, 50, 50, 50, 50,
                      4, 1, 8, 9, 12, 2, 50, 12,
                      2, 12, 12, 50, 9, 110, 50,
                      50, 20, 20, 8, 1, 1, 8, 2, 3, 6]
        df_voter = pd.DataFrame(columns=self.config.raw_file_columns())
        df_hist = pd.DataFrame(columns=self.config.raw_file_columns())
        have_length = False
        for i in new_files:
            file_len = i['obj'].seek(SEEK_END)
            i['obj'].seek(SEEK_SET)
            if ("count" not in i['name'] and file_len != 0):

                if not have_length:
                    line_length = len(i['obj'].readline())
                    i['obj'].seek(SEEK_END)
                    have_length = True
                    if line_length == 686:
                        widths = widths_one
                    elif line_length == 680:
                        widths = widths_two
                    else:
                        raise ValueError(
                            "Width possibilities have changed,"
                            "new width found: {}".format(line_length))
                    have_length = True
                logging.info("Loading file {}".format(i))
                new_df = pd.read_fwf(
                    i['obj'], widths=widths, header=None)
                try:
                    new_df.columns = self.config.raw_file_columns()
                except ValueError:
                    logging.info("Incorrect number of columns found for texas")
                    raise MissingNumColumnsError("{} state is missing columns".format(self.state), self.state,
                                                 len(self.config.raw_file_columns()), len(new_df.columns))
                if new_df['Election_Date'].head(n=100).isnull().sum() > 75:
                    df_voter = pd.concat(
                        [df_voter, new_df], axis=0, ignore_index=True)
                else:
                    df_hist = pd.concat([df_hist, new_df],
                                        axis=0, ignore_index=True)
            del i['obj']
        if df_hist.empty:
            logging.info("This file contains no voter history")
        df_voter['Effective_Date_of_Registration'] = df_voter[
            'Effective_Date_of_Registration'].fillna(-1).astype(
            int, errors='ignore').astype(str).replace('-1', np.nan)
        df_voter[self.config["party_identifier"]] = 'npa'
        df_hist[self.config['hist_columns']] = df_hist[
            self.config['hist_columns']].replace(np.nan, '', regex=True)
        df_hist["election_name"] = df_hist["Election_Date"].astype(str) + \
                                   "_" + \
                                   df_hist['Election_Type'].astype(
                                       str) + "_" + df_hist['Election_Party'].astype(str)

        valid_elections, counts = np.unique(df_hist["election_name"],
                                            return_counts=True)

        def texas_datetime(x):
            try:
                return datetime.strptime(x[0:8], "%Y%m%d")
            except (ValueError):
                return datetime(1970, 1, 1)

        date_order = [idx for idx, election in
                      sorted(enumerate(valid_elections),
                             key=lambda x: texas_datetime(x[1]),
                             reverse=True)]
        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                 "date": str(texas_datetime(k).date())}
                             for i, k in enumerate(sorted_codes)}

        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))
        logging.info("Texas: history apply")
        voter_groups = df_hist.groupby(self.config['voter_id'])
        sparse_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["Election_Voting_Method"].apply(list)

        df_voter = df_voter.set_index(self.config["voter_id"])
        df_voter["sparse_history"] = sparse_history
        df_voter["all_history"] = voter_groups["election_name"].apply(list)
        df_voter["vote_type"] = vote_type
        gc.collect()
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(df_voter, extra_cols=[
            'Permanent_Zipcode', 'Permanent_House_Number', 'Mailing_Zipcode'])
        df_voter.drop(self.config['hist_columns'],
                      axis=1, inplace=True)
        self.meta = {
            "message": "texas_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        gc.collect()
        logging.info("Texas: writing out")
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voter.to_csv()),
                        s3_bucket=self.s3_bucket)

    def preprocess_ohio(self):
        new_files = self.unpack_files(file_obj=self.main_file)

        if not self.ignore_checks:
            self.file_check(len(new_files))

        for i in new_files:
            logging.info("Loading file {}".format(i))
            if "_22" in i['name']:
                df = self.read_csv_count_error_lines(i['obj'], encoding='latin-1',
                    compression='gzip', error_bad_lines=False)
            elif ".txt" in i['name']:
                temp_df = self.read_csv_count_error_lines(
                    i['obj'], encoding='latin-1', compression='gzip',
                    error_bad_lines=False)
                df = pd.concat([df, temp_df], axis=0)

        # create history meta data
        voting_history_cols = list(filter(
            lambda x: any([pre in x for pre in (
                "GENERAL-", "SPECIAL-", "PRIMARY-")]), df.columns.values))
        self.column_check(list(set(df.columns) - set(voting_history_cols)))
        total_records = df.shape[0]
        sorted_codes = voting_history_cols
        sorted_codes_dict = {k: {"index": i,
                                 "count": int(
                                     total_records - df[k].isna().sum()),
                                 "date": date_from_str(k)}
                             for i, k in enumerate(voting_history_cols)}
        self.meta = {
            "message": "ohio_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df.to_csv(encoding='utf-8')),
                        s3_bucket=self.s3_bucket)

    def preprocess_minnesota(self):
        logging.info("Minnesota: loading voter file")
        new_files = self.unpack_files(
            compression='unzip', file_obj=self.main_file)

        if not self.ignore_checks:
            self.file_check(len(new_files))
        voter_reg_df = pd.DataFrame(columns=self.config['ordered_columns'])
        voter_hist_df = pd.DataFrame(columns=self.config['hist_columns'])
        for i in new_files:
            if "election" in i['name'].lower():
                voter_hist_df = pd.concat(
                    [voter_hist_df, self.read_csv_count_error_lines(
                        i['obj'], error_bad_lines=False)],
                    axis=0)
            elif "voter" in i['name'].lower():
                voter_reg_df = pd.concat(
                    [voter_reg_df, self.read_csv_count_error_lines(
                        i['obj'], encoding='latin-1', error_bad_lines=False)],
                    axis=0)
        voter_reg_df[self.config["voter_status"]] = np.nan
        voter_reg_df[self.config["party_identifier"]] = np.nan

        # if the dataframes are assigned columns to begin with, there will be nans due to concat if the columns are off
        self.column_check(list(voter_reg_df.columns))

        voter_reg_df['DOBYear'] = voter_reg_df['DOBYear'].astype(str).str[0:4]

        voter_hist_df["election_name"] = voter_hist_df["ElectionDate"] + \
                                         "_" + voter_hist_df["VotingMethod"]
        valid_elections, counts = np.unique(voter_hist_df["election_name"],
                                            return_counts=True)
        date_order = [idx for idx, election in
                      sorted(enumerate(valid_elections),
                             key=lambda x: datetime.strptime(x[1][:-2],
                                                             "%m/%d/%Y"),
                             reverse=True)]
        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                 "date": date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}

        voter_hist_df["array_position"] = voter_hist_df["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))

        logging.info("Minnesota: history apply")
        voter_groups = voter_hist_df.groupby("VoterId")
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["VotingMethod"].apply(list)

        voter_reg_df = voter_reg_df.set_index(self.config["voter_id"])

        voter_reg_df["all_history"] = all_history
        voter_reg_df["vote_type"] = vote_type
        gc.collect()

        voter_reg_df = self.config.coerce_strings(voter_reg_df)
        voter_reg_df = self.config.coerce_dates(voter_reg_df)
        voter_reg_df = self.config.coerce_numeric(voter_reg_df)

        self.meta = {
            "message": "minnesota_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        gc.collect()
        logging.info("Minnesota: writing out")
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(voter_reg_df.to_csv()),
                        s3_bucket=self.s3_bucket)

    def preprocess_colorado(self):
        config = Config("colorado")
        new_files = self.unpack_files(compression='unzip',
                                      file_obj=self.main_file)
        df_voter = pd.DataFrame(columns=self.config.raw_file_columns())
        df_hist = pd.DataFrame(columns=self.config['hist_columns'])
        df_master_voter = pd.DataFrame(
            columns=self.config['master_voter_columns'])
        master_vf_version = True

        def master_to_reg_df(df):
            df.columns = self.config['master_voter_columns']
            df['STATUS'] = df['VOTER_STATUS']
            df['PRECINCT'] = df['PRECINCT_CODE']
            df['VOTER_NAME'] = df['LAST_NAME'] + ", " + df['FIRST_NAME'] + \
                               " " + df['MIDDLE_NAME']
            df = pd.concat(
                [df, pd.DataFrame(columns=self.config['blacklist_columns'])])
            df = df[self.config.processed_file_columns()]
            return df

        for i in new_files:
            if "Registered_Voters_List" in i['name']:
                master_vf_version = False

        for i in new_files:
            if "Public" not in i['name']:

                if "Registered_Voters_List" in i['name'] and not master_vf_version:
                    logging.info("reading in {}".format(i['name']))
                    df_voter = pd.concat(
                        [df_voter, self.read_csv_count_error_lines(i['obj'],
                            encoding='latin-1', error_bad_lines=False)], axis=0)

                elif ( (("Voting_History" in i['name']) or \
                        ("Coordinated_Voter_Details" in i['name'])) ):
                    if "Voter_Details" not in i['name']:
                        logging.info("reading in {}".format(i['name']))
                        new_df = self.read_csv_count_error_lines(i['obj'],
                            compression='gzip', error_bad_lines=False)
                        df_hist = pd.concat([df_hist, new_df], axis=0)

                    if "Voter_Details" in i['name'] and master_vf_version:
                        logging.info("reading in {}".format(i['name']))
                        new_df = self.read_csv_count_error_lines(
                            i['obj'], compression='gzip', error_bad_lines=False)
                        if len(new_df.columns) < len(self.config['master_voter_columns']):
                            new_df.insert(10, 'PHONE_NUM', np.nan)
                        try:
                            new_df.columns = self.config['master_voter_columns']
                        except ValueError:
                            logging.info("Incorrect number of columns found for Colorado for file: {}".format(
                                i['name']))
                            raise MissingNumColumnsError("{} state is missing columns".format(self.state), self.state,
                                                         len(self.config["master_voter_columns"]), len(new_df.columns))
                        df_master_voter = pd.concat(
                            [df_master_voter, new_df], axis=0)

        if df_voter.empty:
            df_voter = master_to_reg_df(df_master_voter)
        if df_hist.empty:
            raise ValueError("must supply a file containing voter history")
        df_hist['VOTING_METHOD'] = df_hist[
            'VOTING_METHOD'].replace(np.nan, '')
        df_hist["ELECTION_DATE"] = pd.to_datetime(df_hist["ELECTION_DATE"],
                                                  format="%m/%d/%Y",
                                                  errors='coerce')
        df_hist.dropna(subset=["ELECTION_DATE"], inplace=True)
        df_hist["election_name"] = df_hist["ELECTION_DATE"].astype(
            str) + "_" + df_hist["VOTING_METHOD"]

        valid_elections, counts = np.unique(df_hist["election_name"],
                                            return_counts=True)

        date_order = [idx for idx, election in
                      sorted(enumerate(valid_elections),
                             key=lambda x: datetime.strptime(x[1][0:10],
                                                             "%Y-%m-%d"),
                             reverse=True)]
        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                 "date": date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}

        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))

        logging.info("Colorado: history apply")
        voter_groups = df_hist.groupby(self.config["voter_id"])
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["VOTING_METHOD"].apply(list)

        df_voter.dropna(subset=[self.config["voter_id"]], inplace=True)
        df_voter = df_voter.set_index(self.config["voter_id"])
        df_voter.sort_index(inplace=True)

        df_voter["all_history"] = all_history
        df_voter["vote_type"] = vote_type
        gc.collect()

        # at some point mailing address field names changed
        for num in ["1", "2", "3"]:
            if f"MAIL_ADDR{num}" in df_voter.columns:
                df_voter[f"MAILING_ADDRESS_{num}"] = \
                    df_voter[f"MAIL_ADDR{num}"]
                df_voter.drop(columns=[f"MAIL_ADDR{num}"], inplace=True)

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(df_voter, extra_cols=[
            "HOUSE_NUM", "UNIT_NUM", "RESIDENTIAL_ZIP_CODE",
            "RESIDENTIAL_ZIP_PLUS", "MAILING_ZIP_CODE", "MAILING_ZIP_PLUS",
            "PRECINCT_NAME", "PRECINCT", "MAILING_ADDRESS_3", "PHONE_NUM"])

        self.meta = {
            "message": "Colorado_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        gc.collect()
        logging.info("Colorado: writing out")
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voter.to_csv(encoding='utf-8')),
                        s3_bucket=self.s3_bucket)

    def preprocess_georgia(self):
        config = Config("georgia")
        logging.info("GEORGIA: loading voter and voter history file")
        new_files = self.unpack_files(
            compression='unzip', file_obj=self.main_file)

        voter_files = []
        vh_files = []
        for i in new_files:
            if "Georgia_Daily_VoterBase.txt".lower() in i["name"].lower():
                logging.info("Detected voter file: " + i["name"])
                voter_files.append(i)
            elif "txt" in i["name"].lower():
                vh_files.append(i)
        logging.info("Detected {} history files".format(len(vh_files)))

        if not self.ignore_checks:
            self.file_check(len(voter_files))

        df_voters = self.read_csv_count_error_lines(
            voter_files[0]["obj"], sep="|", quotechar='"', quoting=3,
            error_bad_lines=False)
        try:
            df_voters.columns = self.config["ordered_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for Georgia")
            raise MissingNumColumnsError("{} state is missing columns".format(
                self.state), self.state, len(self.config["ordered_columns"]),
                len(df_voters.columns))
        df_voters['Registration_Number'] = df_voters[
            'Registration_Number'].astype(str).str.zfill(8)

        concat_history_file = concat_and_delete(vh_files)

        logging.info("Performing GA history manipulation")

        history = self.read_csv_count_error_lines(concat_history_file, sep="  ",
                              names=['Concat_str', 'Other'], error_bad_lines=False)

        history['County_Number'] = history['Concat_str'].str[0:3]
        history['Registration_Number'] = history['Concat_str'].str[3:11]
        history['Election_Date'] = history['Concat_str'].str[11:19]
        history['Election_Type'] = history['Concat_str'].str[19:22]
        history['Party'] = history['Concat_str'].str[22:24]
        history['Absentee'] = history['Other'].str[0]
        history['Provisional'] = history['Other'].str[1]
        history['Supplimental'] = history['Other'].str[2]
        type_dict = {"001": "GEN_PRIMARY", "002": "GEN_PRIMARY_RUNOFF",
                     "003": "GEN", "004": "GEN_ELECT_RUNOFF",
                     "005": "SPECIAL_ELECT",
                     "006": "SPECIAL_RUNOFF", "007": "NON-PARTISAN",
                     "008": "SPECIAL_NON-PARTISAN", "009": "RECALL",
                     "010": "PPP"}
        history = history.replace({"Election_Type": type_dict})
        history['Combo_history'] = history['Election_Date'].str.cat(
            others=history[['Election_Type', 'Party', 'Absentee',
                            'Provisional', 'Supplimental']], sep='_')
        history = history.filter(items=['County_Number', 'Registration_Number',
                                        'Election_Date', 'Election_Type',
                                        'Party', 'Absentee', 'Provisional',
                                        'Supplimental', 'Combo_history'])
        history = history.dropna()

        logging.info("Creating GA sparse history")

        valid_elections, counts = np.unique(history["Combo_history"],
                                            return_counts=True)

        date_order = [idx for idx, election in
                      sorted(enumerate(valid_elections),
                             key=lambda x: datetime.strptime(
                                 x[1][0:8], "%Y%m%d"), reverse=True)]

        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                 "date": datetime.strptime(k[0:8], "%Y%m%d")}
                             for i, k in enumerate(sorted_codes)}
        history["array_position"] = history["Combo_history"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))

        voter_groups = history.groupby('Registration_Number')
        all_history = voter_groups['Combo_history'].apply(list)
        all_history_indices = voter_groups['array_position'].apply(list)
        df_voters = df_voters.set_index('Registration_Number')
        df_voters["party_identifier"] = "npa"
        df_voters["all_history"] = all_history
        df_voters["sparse_history"] = all_history_indices
        df_voters = config.coerce_dates(df_voters)
        df_voters = config.coerce_numeric(df_voters, extra_cols=[
            "Land_district", "Mail_house_nbr", "Land_lot",
            "Commission_district", "School_district",
            "Ward city council_code", "County_precinct_id",
            "Judicial_district", "County_district_a_value",
            "County_district_b_value", "City_precinct_id", "Mail_address_2",
            "Mail_address_3", "Mail_apt_unit_nbr", "Mail_country",
            "Residence_apt_unit_nbr"])

        self.meta = {
            "message": "georgia_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict, indent=4,
                                         sort_keys=True, default=str),
            "array_decoding": json.dumps(sorted_codes),
            "election_type": json.dumps(type_dict)
        }

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voters.to_csv()),
                        s3_bucket=self.s3_bucket)

    def preprocess_nevada(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')

        if not self.ignore_checks:
            self.file_check(len(new_files))
        voter_file = new_files[0] if "ElgbVtr" in new_files[0]["name"] \
            else new_files[1]
        hist_file = new_files[0] if "VtHst" in new_files[0]["name"] else \
            new_files[1]

        df_hist = self.read_csv_count_error_lines(hist_file["obj"], header=None,
            error_bad_lines=False)
        df_hist.columns = self.config["hist_columns"]
        df_voters = self.read_csv_count_error_lines(voter_file["obj"], header=None,
            error_bad_lines=False)

        try:
            df_voters.columns = self.config["ordered_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for Nevada")
            raise MissingNumColumnsError("{} state is missing columns".format(self.state), self.state,
                                         len(self.config["ordered_columns"]), len(df_voters.columns))

        sorted_codes = df_hist.date.unique().tolist()
        sorted_codes.sort(key=lambda x: datetime.strptime(x, "%m/%d/%Y"))
        counts = df_hist.date.value_counts()
        sorted_codes_dict = {k: {"index": i,
                                 "count": int(counts.loc[k]),
                                 "date": date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]["index"] for k in arr]
            else:
                return np.nan

        df_voters = df_voters.set_index('VoterID', drop=False)
        voter_id_groups = df_hist.groupby('VoterID')
        df_voters['all_history'] = voter_id_groups['date'].apply(list)
        df_voters['votetype_history'] = voter_id_groups['vote_code'].apply(list)
        df_voters['sparse_history'] = df_voters['all_history'].map(insert_code_bin)

        # create compound string for unique voter ID from county ID
        df_voters['County_Voter_ID'] = df_voters['County'].str.replace(
            ' ', '').str.lower() + '_' + df_voters['County_Voter_ID'].astype(
            int).astype(str)
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(df_voters, extra_cols=['Zip',
            'Phone', 'Congressional_District', 'Senate_District',
            'Assembly_District', 'Education_District', 'Regent_District',
            'Registered_Precinct'])
        df_voters = self.config.coerce_strings(df_voters)

        self.meta = {
            "message": "nevada_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voters.to_csv(index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_florida(self):
        logging.info("preprocessing florida")
        # new_files is list of dicts, i.e. [{"name":.. , "obj": <fileobj>}, ..]
        new_files = self.unpack_files(
            compression='unzip', file_obj=self.main_file)

        vote_history_files = []
        voter_files = []
        for i in new_files:
            if "_H_" in i["name"]:
                vote_history_files.append(i)
            elif ".txt" in i["name"]:
                voter_files.append(i)

        if not self.ignore_checks:
            self.file_check(len(voter_files))
        concat_voter_file = concat_and_delete(voter_files)
        concat_history_file = concat_and_delete(vote_history_files)
        gc.collect()

        logging.info("FLORIDA: loading voter history file")
        df_hist = pd.read_fwf(concat_history_file, header=None)
        try:
            df_hist.columns = self.config["hist_columns"]
        except ValueError:
            logging.info("Incorrect history columns found in Florida")
            raise MissingNumColumnsError("{} state history is missing columns".format(self.state), self.state,
                                         len(self.config["hist_columns"]), len(df_hist.columns))
        gc.collect()

        df_hist = df_hist[df_hist["date"].map(lambda x: len(x)) > 5]
        df_hist["election_name"] = df_hist["date"] + "_" + \
                                   df_hist["election_type"]
        valid_elections, counts = np.unique(df_hist["election_name"],
                                            return_counts=True)
        date_order = [idx for idx, election in
                      sorted(enumerate(valid_elections),
                             key=lambda x: datetime.strptime(x[1][:-4],
                                                             "%m/%d/%Y"),
                             reverse=True)]
        valid_elections = valid_elections[date_order]
        counts = counts[date_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                 "date": date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}

        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))

        logging.info("FLORIDA: history apply")
        voter_groups = df_hist.groupby("VoterID")
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["vote_type"].apply(list)
        gc.collect()

        logging.info("FLORIDA: loading main voter file")
        df_voters = self.read_csv_count_error_lines(concat_voter_file, header=None,
            sep="\t", error_bad_lines=False)

        try:
            df_voters.columns = self.config["ordered_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for Flordia")
            raise MissingNumColumnsError("{} state is missing voters columns".format(self.state), self.state,
                                         len(self.config["ordered_columns"]), len(df_voters.columns))
        df_voters = df_voters.set_index(self.config["voter_id"])

        df_voters["all_history"] = all_history
        df_voters["vote_type"] = vote_type
        gc.collect()

        df_voters = self.config.coerce_strings(df_voters)
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(df_voters, extra_cols=[
            "Precinct", "Precinct_Split", "Daytime_Phone_Number",
            "Daytime_Area_Code", "Daytime_Phone_Extension",
            "Daytime_Area_Code", "Daytime_Phone_Extension",
            "Mailing_Zipcode", "Residence_Zipcode",
            "Mailing_Address_Line_1", "Mailing_Address_Line_2",
            "Mailing_Address_Line_3", "Residence_Address_Line_1",
            "Residence_Address_Line_2"])

        self.meta = {
            "message": "florida_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }

        gc.collect()
        logging.info("FLORIDA: writing out")
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voters.to_csv()),
                        s3_bucket=self.s3_bucket)

    def preprocess_kansas(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression='unzip')

        if not self.ignore_checks:
            self.file_check(len(new_files))

        for f in new_files:
            if (".txt" in f['name']) and ("._" not in f['name']) and \
                    ("description" not in f['name'].lower()):
                logging.info("reading kansas file from {}".format(f['name']))
                df = self.read_csv_count_error_lines(f['obj'], sep="\t",
                    index_col=False, engine='c', error_bad_lines=False,
                    encoding='latin-1')
        try:
            df.columns = self.config["ordered_columns"]
        except ValueError:
            try:
                df.columns = self.config["ordered_columns_new"]
            except ValueError:
                logging.info("Incorrect number of columns found for Kansas")
                raise MissingNumColumnsError("{} state is missing columns".format(self.state), self.state,
                                             len(self.config["ordered_columns_new"]), len(df.columns))

            for i in set(list(self.config["ordered_columns"])) - \
                     set(list(self.config["ordered_columns_new"])):
                df[i] = None
        df[self.config["voter_status"]] = df[
            self.config["voter_status"]].str.replace(" ", "")

        def ks_hist_date(s):
            try:
                elect_year = parser.parse(s[2:6]).year
            except:
                elect_year = -1
                pass
            if (elect_year < 1850) or (elect_year > dt.today().year + 1):
                elect_year = None
            return (elect_year)

        def add_history(main_df):
            count_df = pd.DataFrame()
            for idx, hist in enumerate(self.config['hist_columns']):
                unique_codes, counts = np.unique(main_df[hist].str.replace(
                    " ", "_").dropna().values, return_counts=True)
                count_df_new = pd.DataFrame(index=unique_codes, data=counts,
                                            columns=['counts_' + hist])
                count_df = pd.concat([count_df, count_df_new], axis=1)
            count_df['total_counts'] = count_df.sum(axis=1)
            unique_codes = count_df.index.values
            counts = count_df['total_counts'].values
            count_order = counts.argsort()
            unique_codes = unique_codes[count_order]
            counts = counts[count_order]
            sorted_codes = unique_codes.tolist()
            sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                     "date": ks_hist_date(k)}
                                 for i, k in enumerate(sorted_codes)}

            def insert_code_bin(arr):
                return [sorted_codes_dict[k]["index"] for k in arr]

            main_df['all_history'] = main_df[
                self.config['hist_columns']].apply(
                lambda x: list(x.dropna().str.replace(" ", "_")), axis=1)
            main_df.all_history = main_df.all_history.map(insert_code_bin)
            return sorted_codes, sorted_codes_dict

        sorted_codes, sorted_codes_dict = add_history(main_df=df)

        df = self.config.coerce_numeric(df)
        df = self.config.coerce_strings(df)
        df = self.config.coerce_dates(df)
        self.meta = {
            "message": "kansas_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df.to_csv(encoding='utf-8',index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_iowa(self):
        def is_first_file(fname):
            if "CD1" in fname:
                if ("Part1" in fname) or ("Part 1" in fname):
                        return True
            return False

        new_files = self.unpack_files(
            file_obj=self.main_file, compression='unzip')
        logging.info("IOWA: reading in voter file")

        first_file = [f for f in new_files if is_first_file(f["name"])][0]
        remaining_files = [f for f in new_files if not is_first_file(f["name"])]
        if not self.ignore_checks:
            # add 1 for firs file
            valid_files = len(remaining_files) + 1
            self.file_check(valid_files)

        buffer_cols = ["buffer0", "buffer1", "buffer2", "buffer3", "buffer4",
                       "buffer5", "buffer6", "buffer7", "buffer8", "buffer9"]

        # Reads the headers in on the first file given
        headers = pd.read_csv(first_file["obj"], nrows=1).columns

        # Gather the columns for renaming in order to fit the original schema in the database and then rename
        # so that the columns in the header will fit what is expected
        column_rename_dict = self.config["rename_columns"]
        normalized_headers = [x if x not in column_rename_dict else column_rename_dict[x] for x in headers]
        normalized_headers = [x.replace(" ", "_") for x in normalized_headers]

        columns_to_check = [x for x in normalized_headers if x not in self.config["election_columns"]]
        self.column_check(columns_to_check)

        # Add the buffer columns back in for lines that contain extra commas
        headers_with_buffers = normalized_headers + buffer_cols

        # Begin reading the file with the correct headers
        df_voters = self.read_csv_count_error_lines(first_file["obj"], skiprows=1,
            header=None, names=headers_with_buffers, error_bad_lines=False)

        for i in remaining_files:
            skiprows = 1 if "Part1" in i["name"] else 0
            new_df = self.read_csv_count_error_lines(i["obj"], header=None,
                skiprows=skiprows, names=headers_with_buffers, error_bad_lines=False)
            df_voters = pd.concat([df_voters, new_df], axis=0)

        key_delim = "_"
        df_voters["all_history"] = ''
        df_voters = df_voters[df_voters.COUNTY != "COUNTY"]

        # instead of iterating over all of the columns for each row, we should
        # handle all this beforehand.
        # also we should not compute the unique values until after, not before
        df_voters.drop(columns=buffer_cols, inplace=True)

        for c in self.config["election_dates"]:
            null_rows = df_voters[c].isnull()
            df_voters[c][null_rows] = ""

            # each key contains info from the columns
            prefix = c.split("_")[0] + key_delim

            # and the corresponding votervotemethod column
            vote_type_col = c.replace("ELECTION_DATE", "VOTERVOTEMETHOD")
            null_rows = df_voters[vote_type_col].isnull()
            df_voters[vote_type_col].loc[null_rows] = ""
            # add election type and date
            df_voters[c] = prefix + df_voters[c].str.strip()
            # add voting method
            df_voters[c] += key_delim + df_voters[vote_type_col].str.strip()

            # the code below will format each key as
            # <election_type>_<date>_<voting_method>_<political_party>_
            # <political_org>
            if "PRIMARY" in prefix:

                # so far so good but we need more columns in the event of a
                # primary
                org_col = c.replace("PRIMARY_ELECTION_DATE",
                                    "POLITICAL_ORGANIZATION")
                party_col = c.replace("PRIMARY_ELECTION_DATE",
                                      "POLITICAL_PARTY")
                df_voters[org_col].loc[df_voters[org_col].isnull()] = ""
                df_voters[party_col].loc[df_voters[party_col].isnull()] = ""
                party_info = df_voters[party_col].str.strip() + key_delim + \
                             df_voters[org_col].str.replace(" ", "")
                df_voters[c] += key_delim + party_info
            else:
                # add 'blank' values for the primary slots
                df_voters[c] += key_delim + key_delim

            df_voters[c] = df_voters[c].str.replace(prefix + key_delim * 3, '')
            df_voters[c] = df_voters[c].str.replace('"', '')
            df_voters[c] = df_voters[c].str.replace("'", '')
            df_voters.all_history += " " + df_voters[c]

        # make into an array (null values are '' so they are ignored)
        df_voters.all_history = df_voters.all_history.str.split()
        elections, counts = np.unique(df_voters[self.config["election_dates"]],
                                      return_counts=True)
        # we want reverse order (lower indices are higher frequency)
        count_order = counts.argsort()[::-1]
        elections = elections[count_order]
        counts = counts[count_order]

        # create meta
        sorted_codes_dict = {j: {"index": i, "count": int(counts[i]),
                                 "date": date_from_str(j)}
                             for i, j in enumerate(elections)}

        default_item = {"index": len(elections)}

        def ins_code_bin(a):
            return [sorted_codes_dict.get(k, default_item)["index"] for k in a]

        # In an instance like this, where we've created our own systematized
        # labels for each election I think it makes sense to also keep them
        # in addition to the sparse history
        df_voters["sparse_history"] = df_voters.all_history.apply(ins_code_bin)

        self.meta = {
            "message": "iowa_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(elections.tolist()),
        }
        for c in df_voters.columns:
            df_voters[c].loc[df_voters[c].isnull()] = ""

        for c in df_voters.columns:
            df_voters[c] = df_voters[c].astype(str).str.encode(
                'utf-8', errors='ignore').str.decode('utf-8')

        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(df_voters, extra_cols=[
            "COMMUNITY_COLLEGE", "COMMUNITY_COLLEGE_DIRECTOR",
            "LOSST_CONTIGUOUS_CITIES", "PRECINCT", "SANITARY",
            "SCHOOL_DIRECTOR", "UNIT_NUM"])
        # force reg num to be integer
        df_voters['REGN_NUM'] = pd.to_numeric(df_voters['REGN_NUM'],
                                              errors='coerce').fillna(0)
        df_voters['REGN_NUM'] = df_voters['REGN_NUM'].astype(int)

        # Drop the election columns because they are no longer needed
        df_voters.drop(columns=self.config["election_columns"], inplace=True)

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voters.to_csv(encoding='utf-8',
                                                         index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_arizona2(self):

        def file_is_active(filename):
            for word in ['Canceled', 'Suspense', 'Inactive']:
                if word in filename:
                    return False
            return True

        def add_files_to_main_df(main_df, file_list):
            alias_dict = self.config['column_aliases']
            for f in file_list:
                if f['name'].split('.')[-1] == 'csv':
                    new_df = self.read_csv_count_error_lines(
                        f['obj'], error_bad_lines=False)
                else:
                    new_df = pd.read_excel(f['obj'])

                for c in new_df.columns:
                    # files vary in consistent use of spaces in headers,
                    # and some headers have different aliases for headers
                    if c.replace(' ', '') in alias_dict.keys():
                        new_df.rename(
                            columns={c: alias_dict[c.replace(' ', '')]},
                            inplace=True)
                    else:
                        new_df.rename(columns={c: c.replace(' ', '')},
                                      inplace=True)
                new_df.rename(columns={"YearofBirth": "DOB"}, inplace=True)
                main_df = pd.concat([main_df, new_df], sort=False)
            return main_df

        def insert_code_bin(arr):
            return [sorted_codes_dict[k]['index'] for k in arr]

        new_files = self.unpack_files(file_obj=self.main_file,
                                      compression="unzip")

        active_files = [f for f in new_files if file_is_active(f['name'])]
        other_files = [f for f in new_files if not file_is_active(f['name'])]

        main_df = pd.DataFrame()
        main_df = add_files_to_main_df(main_df, active_files)
        main_df = add_files_to_main_df(main_df, other_files)
        main_df.reset_index(drop=True, inplace=True)

        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_strings(main_df)
        main_df = self.config.coerce_numeric(main_df, extra_cols=[
            'HouseNumber', 'UnitNumber', 'ResidenceZip', 'MailingZip',
            'Phone', 'PrecinctPart', 'VRAZVoterID'])

        voter_columns = [c for c in main_df.columns if not c[0].isdigit()]
        history_columns = [c for c in main_df.columns if c[0].isdigit()]

        self.column_check(voter_columns)
        to_normalize = history_columns + \
                       [self.config['party_identifier'], self.config['voter_status']]
        for c in to_normalize:
            s = main_df[c].astype(str).str.strip().str.lower()
            s = s.str.encode('utf-8', errors='ignore').str.decode('utf-8')
            main_df.loc[(~main_df[c].isna()), c] = s.loc[(~main_df[c].isna())]
        for c in history_columns:
            main_df[c] = main_df[c].str.replace(' - ', '_')

        main_df[self.config['party_identifier']] = \
            main_df[self.config['party_identifier']].map(
                lambda x: self.config['party_aliases'][x] \
                          if x in self.config['party_aliases'] else x)

        # handle history:
        sorted_codes = history_columns[::-1]
        hist_df = main_df[sorted_codes]
        voter_df = main_df[voter_columns]
        counts = (~hist_df.isna()).sum()
        sorted_codes_dict = {k: {'index': int(i),
                                 'count': int(counts[i]),
                                 'date': date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}

        hist_df.loc[:, 'vote_codes'] = pd.Series(hist_df.values.tolist())
        hist_df.loc[:, 'vote_codes'] = hist_df.loc[:, 'vote_codes'].map(
            lambda x: [c for c in x if not pd.isna(c)])
        voter_df.loc[:, 'votetype_history'] = hist_df.loc[:, 'vote_codes'].map(
            lambda x: [c.split('_')[0] for c in x])
        voter_df.loc[:, 'party_history'] = hist_df.loc[:, 'vote_codes'].map(
            lambda x: [c.split('_')[1]
                       if len(c.split('_')) > 1
                       else self.config['no_party_affiliation']
                       for c in x])

        hist_df.drop(columns=['vote_codes'], inplace=True)
        for c in hist_df.columns:
            hist_df.loc[:, c] = hist_df.loc[:, c].map(
                lambda x: c if not pd.isna(x) else np.nan)
        voter_df.loc[:, 'all_history'] = pd.Series(hist_df.values.tolist())
        voter_df.loc[:, 'all_history'] = voter_df.loc[:, 'all_history'].map(
            lambda x: [c for c in x if not pd.isna(c)])
        voter_df.loc[:, 'sparse_history'] =  voter_df.loc[:, 'all_history'].map(
            insert_code_bin)

        expected_cols = self.config['ordered_columns'] + \
                        self.config['ordered_generated_columns']
        voter_df = self.reconcile_columns(voter_df, expected_cols)
        voter_df = voter_df[expected_cols]

        self.meta = {
            "message": "arizona2_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(voter_df.to_csv(encoding='utf-8',
                                                        index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_arizona(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip")
        new_files = [f for f in new_files if "LEGEND.xlsx" not in f["name"]]

        combined_file = self.concat_file_segments(new_files)

        main_df = self.read_csv_count_error_lines(combined_file, error_bad_lines=False)

        voting_action_cols = list(filter(lambda x: "party_voted" in x,
                                         main_df.columns.values))
        voting_method_cols = list(filter(lambda x: "voting_method" in x,
                                         main_df.columns.values))
        all_voting_history_cols = voting_action_cols + voting_method_cols

        main_df["all_history"] = df_to_postgres_array_string(
            main_df, voting_action_cols)
        main_df["all_voting_methods"] = df_to_postgres_array_string(
            main_df, voting_method_cols)
        main_df[self.config["birthday_identifier"]] = pd.to_datetime(
            main_df[self.config["birthday_identifier"]].fillna(
                -1).astype(int).astype(str),
            format=self.config["date_format"],
            errors='coerce')
        elections_key = [c.split("_")[-1] for c in voting_action_cols]

        main_df.drop(all_voting_history_cols, axis=1, inplace=True)

        main_df.columns = main_df.columns.str.strip(' ')
        main_df = self.config.coerce_numeric(main_df, extra_cols=[
            "text_mail_zip5", "text_mail_zip4", "text_phone_last_four",
            "text_phone_exchange", "text_phone_area_code",
            "precinct_part_text_name", "precinct_part",
            "occupation", "text_mail_carrier_rte",
            "text_res_address_nbr", "text_res_address_nbr_suffix",
            "text_res_unit_nbr", "text_res_carrier_rte",
            "text_mail_address1", "text_mail_address2", "text_mail_address3",
            "text_mail_address4"])
        self.meta = {
            "message": "arizona_{}".format(datetime.now().isoformat()),
            "array_dates": json.dumps(elections_key)
        }

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(main_df.to_csv(encoding='utf-8',
                                                       index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_new_york(self):
        config = Config("new_york")
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="infer")

        if not self.ignore_checks:
            self.file_check(len(new_files))

        # no longer include pdfs in file list anyway, can just assign main file
        self.main_file = new_files[0]
        gc.collect()
        # When given the names, the pandas read_csv will always work. If given csv has too few column names it will
        # assign the names to the columns to the end, skipping the beginning columns, if too many will add nan columnms
        main_df = self.read_csv_count_error_lines(
            self.main_file["obj"], header=None, names=config["ordered_columns"],
            encoding='latin-1', error_bad_lines=False)
        logging.info("dataframe memory usage: {}".format(main_df.memory_usage(deep=True).sum()))

        gc.collect()
        null_hists = main_df.voterhistory != main_df.voterhistory
        main_df.voterhistory[null_hists] = NULL_CHAR
        all_codes = main_df.voterhistory.str.replace(" ", "_") \
            .str.replace("[", "") \
            .str.replace("]", "")
        all_codes = all_codes.str.cat(sep=";")
        all_codes = np.array(all_codes.split(";"))
        logging.info("Making all_history")
        main_df["all_history"] = strcol_to_array(main_df.voterhistory,
                                                 delim=";")
        unique_codes, counts = np.unique(all_codes, return_counts=True)
        gc.collect()

        count_order = counts.argsort()
        unique_codes = unique_codes[count_order]
        counts = counts[count_order]
        sorted_codes = unique_codes.tolist()
        sorted_codes_dict = {k: {"index": i, "count": int(counts[i])} for i, k in
                             enumerate(sorted_codes)}
        gc.collect()

        def insert_code_bin(arr):
            return [sorted_codes_dict[k]["index"] for k in arr]

        # in this case we save ny as sparse array since so many elections are
        # stored
        logging.info("Mapping history codes")
        main_df.all_history = main_df.all_history.map(insert_code_bin)
        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_strings(main_df)
        main_df = self.config.coerce_numeric(main_df, extra_cols=[
            "raddnumber", "rhalfcode", "rapartment", "rzip5", "rzip4",
            "mailadd4", "ward", "countyvrnumber", "lastvoteddate",
            "prevyearvoted", "prevcounty"])
        self.meta = {
            "message": "new_york_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        gc.collect()

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(main_df.to_csv(index=False,
                                                       encoding='utf-8')),
                        s3_bucket=self.s3_bucket)

    def preprocess_north_carolina(self):
        new_files = self.unpack_files(
            file_obj=self.main_file)  # array of dicts

        if not self.ignore_checks:
            self.file_check(len(new_files))

        self.config = Config("north_carolina")
        for i in new_files:
            if ("ncvhis" in i['name']) and (".txt" in i['name']):
                vote_hist_file = i
            elif ("ncvoter" in i['name']) and (".txt" in i['name']):
                voter_file = i
        voter_df = self.read_csv_count_error_lines(voter_file['obj'], sep="\t",
            quotechar='"', encoding='latin-1', error_bad_lines=False)

        vote_hist = self.read_csv_count_error_lines(vote_hist_file['obj'], sep="\t",
            quotechar='"', error_bad_lines=False)

        try:
            voter_df.columns = self.config["ordered_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for the voter file in North Carolina")
            raise MissingNumColumnsError("{} state is missing columns".format(self.state), self.state,
                                         len(self.config["ordered_columns"]), len(voter_df.columns))
        try:
            vote_hist.columns = self.config["hist_columns"]
        except ValueError:
            logging.info("Incorrect number of columns found for the history file in North Carolina")
            raise

        valid_elections, counts = np.unique(vote_hist["election_desc"],
                                            return_counts=True)
        count_order = counts.argsort()[::-1]
        valid_elections = valid_elections[count_order]
        counts = counts[count_order]

        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                 "date": date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}
        vote_hist["array_position"] = vote_hist["election_desc"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))

        voter_groups = vote_hist.groupby(self.config["voter_id"])
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["voting_method"].apply(list)

        voter_df = voter_df.set_index(self.config["voter_id"])

        voter_df["all_history"] = all_history
        voter_df["vote_type"] = vote_type

        voter_df = self.config.coerce_strings(voter_df)
        voter_df = self.config.coerce_dates(voter_df)
        voter_df = self.config.coerce_numeric(voter_df, extra_cols=[
            "county_commiss_abbrv", "fire_dist_abbrv", "full_phone_number",
            "judic_dist_abbrv", "munic_dist_abbrv", "municipality_abbrv",
            "precinct_abbrv", "precinct_desc", "school_dist_abbrv",
            "super_court_abbrv", "township_abbrv", "township_desc",
            "vtd_abbrv", "vtd_desc", "ward_abbrv"])

        self.meta = {
            "message": "north_carolina_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        self.is_compressed = False
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(voter_df.to_csv(
                            index=True, encoding='utf-8')),
                        s3_bucket=self.s3_bucket)

    def preprocess_missouri(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip")

        # File check doesn't work in this case, it's all over the place
        preferred_files = [x for x in new_files
                           if ("VotersList" in x["name"]) and
                           (".txt" in x["name"])]
        if len(preferred_files) > 0:
            main_file = preferred_files[0]
        else:
            main_file = new_files[0]

        main_df = self.read_csv_count_error_lines(
            main_file["obj"], sep='\t', error_bad_lines=False)

        # convert "Voter Status" to "voter_status" for backward compatibility
        main_df.rename(columns={"Voter Status": self.config["voter_status"]},
                       inplace=True)

        # add empty column for party_identifier
        main_df[self.config["party_identifier"]] = np.nan

        self.column_check(list(set(main_df.columns) - set(self.config['hist_columns'])))

        def add_history(main_df):
            # also save as sparse array since so many elections are stored
            count_df = pd.DataFrame()
            for idx, hist in enumerate(self.config['hist_columns']):
                unique_codes, counts = np.unique(main_df[hist].str.replace(
                    " ", "_").dropna().values, return_counts=True)
                count_df_new = pd.DataFrame(index=unique_codes, data=counts,
                                            columns=['counts_' + hist])
                count_df = pd.concat([count_df, count_df_new], axis=1)
            count_df['total_counts'] = count_df.sum(axis=1)
            unique_codes = count_df.index.values
            counts = count_df['total_counts'].values
            count_order = counts.argsort()
            unique_codes = unique_codes[count_order]
            counts = counts[count_order]
            sorted_codes = unique_codes.tolist()
            sorted_codes_dict = {k: {"index": i, "count": int(counts[i]),
                                     "date": date_from_str(k)}
                                 for i, k in enumerate(sorted_codes)}

            def insert_code_bin(arr):
                return [sorted_codes_dict[k]["index"] for k in arr]

            main_df['all_history'] = main_df[
                self.config['hist_columns']].apply(
                lambda x: list(x.dropna().str.replace(" ", "_")), axis=1)
            main_df.all_history = main_df.all_history.map(insert_code_bin)
            return sorted_codes, sorted_codes_dict

        sorted_codes, sorted_codes_dict = add_history(main_df)
        main_df.drop(self.config['hist_columns'], axis=1, inplace=True)

        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_numeric(main_df, extra_cols=[
            "Residential ZipCode", "Mailing ZipCode", "Precinct",
            "House Number", "Unit Number", "Split", "Township",
            "Ward", "Precinct Name"])

        self.meta = {
            "message": "missouri_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(main_df.to_csv(encoding='utf-8',
                                                       index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_michigan(self):
        config = Config('michigan')
        new_files = self.unpack_files(file_obj=self.main_file)
        if not self.ignore_checks:
            self.file_check(len(new_files))
        voter_file = ([n for n in new_files if 'entire_state_v' in n['name'] or
                       'EntireStateVoters' in n['name']] + [None])[0]
        hist_file = ([n for n in new_files if 'entire_state_h' in n['name'] or
                      'EntireStateVoterHistory' in n['name']] + [None])[0]
        elec_codes = ([n for n in new_files if 'electionscd' in n['name']] +
                      [None])[0]

        logging.info('Loading voter file: ' + voter_file['name'])
        if voter_file['name'][-3:] == 'lst':
            vcolspecs = [[0, 35], [35, 55], [55, 75],
                         [75, 78], [78, 82], [82, 83],
                         [83, 91], [91, 92], [92, 99], [99, 103], [103, 105],
                         [105, 135], [135, 141], [141, 143], [143, 156],
                         [156, 191], [191, 193], [193, 198], [198, 248],
                         [248, 298], [298, 348], [348, 398], [398, 448],
                         [448, 461], [461, 463], [463, 468], [468, 474],
                         [474, 479], [479, 484], [484, 489], [489, 494],
                         [494, 499], [499, 504], [504, 510], [510, 516],
                         [516, 517], [517, 519]]
            vdf = pd.read_fwf(voter_file['obj'],
                              colspecs=vcolspecs,
                              names=config['fwf_voter_columns'],
                              na_filter=False)
        elif voter_file['name'][-3:] == 'csv':
            vdf = self.read_csv_count_error_lines(voter_file['obj'],
                encoding='latin-1', na_filter=False, error_bad_lines=False)
            # rename 'STATE' field to not conflict with our 'state' field
            vdf.rename(columns={'STATE': 'STATE_ADDR'}, inplace=True)
        else:
            raise NotImplementedError('File format not implemented')

        def column_is_empty(col):
            total_size = col.shape[0]
            if (sum(col.isna()) == total_size) or (sum(col == '')):
                return True
            return False

        def fill_empty_columns(df):
            # Dummy values for newly added data fields
            if column_is_empty(df['STATUS_USER_CODE']):
                df['STATUS_USER_CODE'] = '_'
            if column_is_empty(df['VOTER_ID']):
                df['VOTER_ID'] = 0
            if column_is_empty(df['STATUS_DATE']):
                df['STATUS_DATE'] = '1970-01-01 00:00:00'
            return df

        vdf = self.reconcile_columns(vdf, config['columns'])
        vdf = fill_empty_columns(vdf)
        vdf = vdf.reindex(columns=config['ordered_columns'])
        vdf[config['party_identifier']] = 'npa'

        logging.info('Loading history file: ' + hist_file['name'])
        if hist_file['name'][-3:] == 'lst':
            hcolspecs = [[0, 13], [13, 15], [15, 20],
                         [20, 25], [25, 38], [38, 39]]
            hdf = pd.read_fwf(hist_file['obj'],
                              colspecs=hcolspecs,
                              names=config['fwf_hist_columns'],
                              na_filter=False)
        elif hist_file['name'][-3:] == 'csv':
            hdf = self.read_csv_count_error_lines(
                hist_file['obj'], na_filter=False, error_bad_lines=False)
            if ('IS_ABSENTEE_VOTER' not in hdf.columns) and \
                    ('IS_PERMANENT_ABSENTEE_VOTER' in hdf.columns):
                hdf.rename(columns={
                    'IS_PERMANENT_ABSENTEE_VOTER': 'IS_ABSENTEE_VOTER'},
                    inplace=True)
        else:
            raise NotImplementedError('File format not implemented')

        # If hdf has ELECTION_DATE (new style) instead of ELECTION_CODE,
        # then we don't need to do election code lookups
        elec_code_dict = dict()
        missing_history_dates = False
        if 'ELECTION_DATE' in hdf.columns:
            try:
                hdf['ELECTION_NAME'] = pd.to_datetime(hdf['ELECTION_DATE']).map(
                    lambda x: x.strftime('%Y-%m-%d'))
            except ValueError:
                missing_history_dates = True
                hdf['ELECTION_NAME'] = hdf['ELECTION_DATE']
        else:
            if elec_codes:
                # If we have election codes in this file
                logging.info('Loading election codes file: ' + elec_codes['name'])
                if elec_codes['name'][-3:] == 'lst':
                    ecolspecs = [[0, 13], [13, 21], [21, 46]]
                    edf = pd.read_fwf(elec_codes['obj'],
                                      colspecs=ecolspecs,
                                      names=config['elec_code_columns'],
                                      na_filter=False)
                    edf['Date'] = pd.to_datetime(edf['Date'], format='%m%d%Y')
                elif elec_codes['name'][-3:] == 'csv':
                    # I'm not sure if this would actually ever happen
                    edf = self.read_csv_count_error_lines(
                        elec_codes['obj'], names=config['elec_code_columns'],
                        na_filter=False, error_bad_lines=False)
                else:
                    raise NotImplementedError('File format not implemented')

                # make a code dictionary that will be stored with meta data
                for idx, row in edf.iterrows():
                    d = row['Date'].strftime('%Y-%m-%d')
                    elec_code_dict[row['Election_Code']] = {
                        'Date': d,
                        'Slug': d + '_' + str(row['Election_Code']) + '_' + \
                                row['Title'].replace(' ', '-').replace('_', '-')}
            else:
                # Get election codes from most recent meta data
                this_date = parser.parse(date_from_str(self.raw_s3_file)).date()
                pre_date, post_date, pre_key, post_key = get_surrounding_dates(
                    this_date, self.state, self.s3_bucket, testing=self.testing)
                if pre_key is not None:
                    nearest_meta = get_metadata_for_key(pre_key, self.s3_bucket)
                    elec_code_dict = nearest_meta['elec_code_dict']
                    if len(elec_code_dict) == 0:
                        raise MissingElectionCodesError(
                            'No election codes in nearby meta data.')
                else:
                    raise MissingElectionCodesError(
                        'No election code file or nearby meta data found.')

            # Election code lookup
            hdf['ELECTION_NAME'] = hdf['ELECTION_CODE'].map(
                lambda x: elec_code_dict[str(x)]['Slug']
                if str(x) in elec_code_dict else str(x))

        # Create meta data
        counts = hdf['ELECTION_NAME'].value_counts()
        counts.sort_index(inplace=True)
        sorted_codes = counts.index.to_list()
        sorted_codes_dict = {k: {'index': i,
                                 'count': int(counts[i]),
                                 'date': date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}

        # Collect histories
        vdf.set_index(config['voter_id'], drop=False, inplace=True)
        hdf_id_groups = hdf.groupby(config['voter_id'])
        vdf['all_history'] = hdf_id_groups['ELECTION_NAME'].apply(list)
        vdf['votetype_history'] = hdf_id_groups['IS_ABSENTEE_VOTER'].apply(list)
        vdf['county_history'] = hdf_id_groups['COUNTY_CODE'].apply(list)
        vdf['jurisdiction_history'] = hdf_id_groups['JURISDICTION_CODE'].apply(list)
        vdf['schooldistrict_history'] = hdf_id_groups['SCHOOL_DISTRICT_CODE'].apply(list)

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]['index'] for k in arr
                        if k in sorted_codes_dict]
            else:
                return np.nan

        vdf['sparse_history'] = vdf['all_history'].map(insert_code_bin)

        if missing_history_dates:
            vdf['all_history'] = None
            vdf['sparse_history'] = None

        vdf = self.config.coerce_dates(vdf)
        vdf = self.config.coerce_numeric(
            vdf, extra_cols=['PRECINCT', 'WARD', 'VILLAGE_PRECINCT',
                             'SCHOOL_PRECINCT'])
        vdf = self.config.coerce_strings(vdf)

        self.meta = {
            "message": "michigan_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
            "elec_code_dict": elec_code_dict
        }
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(vdf.to_csv(encoding='utf-8',
                                                   index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_pennsylvania(self):
        config = Config('pennsylvania')
        new_files = self.unpack_files(file_obj=self.main_file)
        voter_files = [f for f in new_files if "FVE" in f["name"]]
        election_maps = [f for f in new_files if "Election Map" in f["name"]]
        zone_codes = [f for f in new_files if "Codes" in f["name"]]
        zone_types = [f for f in new_files if "Types" in f["name"]]

        if not self.ignore_checks:
            # election maps need to line up to voter files?
            self.file_check(len(voter_files), len(election_maps))
        counties = config["county_names"]
        main_df = None
        elections = 40
        dfcols = config["ordered_columns"][:-3]
        for i in range(elections):
            dfcols.extend(["district_{}".format(i + 1)])
        for i in range(elections):
            dfcols.extend(["election_{}_vote_method".format(i + 1)])
            dfcols.extend(["election_{}_party".format(i + 1)])
        dfcols.extend(config["ordered_columns"][-3:])

        for c in counties:
            logging.info("Processing {}".format(c))
            c = format_column_name(c)
            try:
                voter_file = next(
                    f for f in voter_files if c in f["name"].lower())
                election_map = next(
                    f for f in election_maps if c in f["name"].lower())
                zones = next(f for f in zone_codes if c in f["name"].lower())
                types = next(f for f in zone_types if c in f["name"].lower())
            except StopIteration:
                continue
            df = self.read_csv_count_error_lines(
                voter_file["obj"], sep='\t', names=dfcols, error_bad_lines=False)
            edf = self.read_csv_count_error_lines(
                election_map["obj"], sep='\t',
                names=['county', 'number', 'title', 'date'], error_bad_lines=False)
            zdf = self.read_csv_count_error_lines(
                zones['obj'], sep='\t', names=['county', 'number', 'code', 'title'],
                error_bad_lines=False)
            tdf = self.read_csv_count_error_lines(
                types['obj'], sep='\t', names=['county', 'number', 'abbr', 'title'],
                error_bad_lines=False)
            df = df.replace('"')
            edf = edf.replace('"')
            zdf = zdf.replace('"')
            edf.index = edf["number"]

            for i in range(elections):
                s = pd.Series(index=df.index)
                # Blair isn't sending all their election codes
                try:
                    s[:] = edf.iloc[i]["title"] + ' ' + \
                           edf.iloc[i]["date"] + ' '
                except IndexError:
                    s[:] = "UNSPECIFIED"
                df["election_{}".format(i)] = s + \
                                              df["election_{}_vote_method".format(i + 1)].apply(
                                                  str) + ' ' + df["election_{}_party".format(i + 1)]
                df.loc[df["election_{}_vote_method".format(i + 1)].isna(),
                       "election_{}".format(i)] = pd.np.nan
                df = df.drop("election_{}_vote_method".format(i + 1), axis=1)
                df = df.drop("election_{}_party".format(i + 1), axis=1)

                df["district_{}".format(i + 1)] = df["district_{}".format(
                    i + 1)].map(zdf.drop_duplicates('code').reset_index()
                                .set_index('code')['title'])
                df["district_{}".format(i + 1)] += \
                    ', Type: ' + df["district_{}".format(i + 1)] \
                        .map(zdf.drop_duplicates('title').reset_index()
                             .set_index('title')['number']) \
                        .map(tdf.set_index('number')['title'])

            df["all_history"] = df[["election_{}".format(i)
                                    for i in range(elections)]].values.tolist()
            df["all_history"] = df["all_history"].map(
                lambda L: list(filter(pd.notna, L)))
            df["districts"] = df[["district_{}".format(i + 1)
                                  for i in range(elections)]].values.tolist()
            df["districts"] = df["districts"].map(
                lambda L: list(filter(pd.notna, L)))

            for i in range(elections):
                df = df.drop("election_{}".format(i), axis=1)
                df = df.drop("district_{}".format(i + 1), axis=1)

            # can check columns for each PA county?
            self.column_check(list(df.columns))
            if main_df is None:
                main_df = df
            else:
                main_df = pd.concat([main_df, df], ignore_index=True)

        main_df = config.coerce_dates(main_df)
        main_df = config.coerce_numeric(main_df, extra_cols=[
            "house_number", "apartment_number", "address_line_2", "zip",
            "mail_address_1", "mail_address_2", "mail_zip", "precinct_code",
            "precinct_split_id", "legacy_id", "home_phone"])
        logging.info("Writing CSV")
        self.meta = {
            "message": "pennsylvania_{}".format(datetime.now().isoformat()),
        }

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(main_df.to_csv(encoding='utf-8',
                                                       index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_new_jersey(self):
        new_files = self.unpack_files(file_obj=self.main_file)
        config = Config("new_jersey")
        voter_files = [n for n in new_files if 'AlphaVoter' in n["name"]]

        hist_files = [n for n in new_files if 'History' in n["name"]]
        vdf = pd.DataFrame()
        hdf = pd.DataFrame()
        for f in voter_files:
            logging.info("Reading " + f["name"])
            new_df = self.read_csv_count_error_lines(
                f["obj"], sep='|', names=config['ordered_columns'],
                low_memory=False, error_bad_lines=False)
            new_df = self.config.coerce_dates(new_df)
            new_df = self.config.coerce_numeric(new_df, extra_cols=[
                "regional_school", "fire", "apt_no"])
            vdf = pd.concat([vdf, new_df], axis=0)
        for f in hist_files:
            logging.info("Reading " + f["name"])
            new_df = self.read_csv_count_error_lines(
                f["obj"], sep='|', names=config['hist_columns'], index_col=False,
                low_memory=False, error_bad_lines=False)
            new_df = self.config.coerce_numeric(
                new_df, col_list='hist_columns_type')
            hdf = pd.concat([hdf, new_df], axis=0)

        hdf['election_name'] = hdf['election_name'] + ' ' + \
                               hdf['election_date']
        hdf = self.config.coerce_dates(hdf, col_list='hist_columns_type')
        hdf.sort_values('election_date', inplace=True)
        hdf = hdf.dropna(subset=['election_name'])
        hdf = hdf.reset_index()
        elections = hdf["election_name"].unique().tolist()
        counts = hdf["election_name"].value_counts()
        elec_dict = {
            k: {'index': i, 'count': int(counts.loc[k]) if k in counts else 0}
            for i, k in enumerate(elections)
        }
        vdf['unabridged_status'] = vdf['status']
        vdf.loc[(vdf['status'] == 'Inactive Confirmation') |
                (vdf['status'] == 'Inactive Confirmation-Need ID'),
                'status'] = 'Inactive'
        vdf['tmp_id'] = vdf['voter_id']
        vdf = vdf.set_index('tmp_id')

        hdf_id_group = hdf.groupby('voter_id')
        logging.info("Creating all_history array")
        vdf['all_history'] = hdf_id_group['election_name'].apply(list)
        logging.info("Creating party_history array")
        vdf['party_history'] = hdf_id_group['party_code'].apply(list)

        def insert_code_bin(arr):
            if arr is np.nan:
                return []
            else:
                return [elec_dict[k]['index'] for k in arr]

        vdf['sparse_history'] = vdf['all_history'].apply(insert_code_bin)
        vdf.loc[
            vdf[self.config['birthday_identifier']] <
            pd.to_datetime('1900-01-01'),
            self.config['birthday_identifier']] = pd.NaT

        self.meta = {
            "message": "new_jersey_{}".format(datetime.now().isoformat()),
            "array_encoding": elec_dict,
            "array_decoding": elections
        }

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(vdf.to_csv(encoding='utf-8',
                                                   index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_new_jersey2(self):

        def format_birthdays_differently_per_county(df):
            field = self.config['birthday_identifier']
            df[field] = df[field].apply(str)
            for format_str in self.config['date_format']:
                formatted = pd.to_datetime(df[field], format=format_str,
                                           errors='coerce')
                if len(formatted[~formatted.isna()]) > (0.5 * len(formatted)):
                    df[field] = formatted
                    break
            return df

        def combine_dfs(filelist):
            df = pd.DataFrame()
            for f in filelist:
                logging.info('Reading file: {}'.format(f['name']))
                new_df = self.read_csv_count_error_lines(
                    f['obj'], error_bad_lines=False)
                if 'vlist' in f['name']:
                    new_df = format_birthdays_differently_per_county(new_df)
                df = pd.concat([df, new_df], axis=0)
            return df

        def simplify_status(status):
            basic_status = ['Active', 'Inactive', 'Pending']
            if type(status) is str:
                for s in basic_status:
                    if s in status:
                        return s
            return np.nan

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]['index'] for k in arr]
            else:
                return np.nan

        def handle_date(d):
            possible_date = date_from_str(d)
            if possible_date is None:
                return ''
            return pd.to_datetime(possible_date).strftime('%m/%d/%Y')

        new_files = self.unpack_files(file_obj=self.main_file,
                                      compression='infer')
        voter_files = [n for n in new_files if 'vlist' in n['name'].lower()]
        hist_files = [n for n in new_files if 'ehist' in n['name'].lower()]

        if not self.ignore_checks:
            self.file_check(len(voter_files), len(hist_files))
        voter_df = combine_dfs(voter_files)
        hist_df = combine_dfs(hist_files)

        voter_df = self.config.coerce_strings(voter_df)
        if 'displayId' in voter_df.columns:
            voter_df.rename(columns={'displayId': self.config['voter_id']},
                            inplace=True)
        voter_df[self.config['voter_id']] = \
            voter_df[self.config['voter_id']].str.upper()
        voter_df[self.config['party_identifier']] = \
            voter_df[self.config['party_identifier']].str.replace('.', '')
        voter_df = self.config.coerce_numeric(
            voter_df, extra_cols=['apt_unit', 'ward', 'district',
                                  'congressional', 'legislative',
                                  'freeholder', 'school', 'fire'])

        # multiple active / inactive statuses are incompatible with our data
        # model; simplify them while also keeping the original data
        voter_df['unabridged_status'] = voter_df[self.config['voter_status']]
        voter_df[self.config['voter_status']] = \
            voter_df[self.config['voter_status']].map(simplify_status)

        # handle history:
        hist_df['election_name'] = hist_df['election_date'] + '_' + \
                                   hist_df['election_name']

        hist_df.dropna(subset=['election_name'], inplace=True)
        sorted_codes = sorted(hist_df['election_name'].unique().tolist())
        counts = hist_df['election_name'].value_counts()
        sorted_codes_dict = {k: {'index': int(i),
                                 'count': int(counts[k]),
                                 'date': handle_date(k)}
                             for i, k in enumerate(sorted_codes)}

        hist_df.sort_values('election_name', inplace=True)
        hist_df.rename(columns={'voter_id': self.config['voter_id']}, inplace=True)

        voter_df.set_index(self.config['voter_id'], drop=False, inplace=True)
        voter_groups = hist_df.groupby(self.config['voter_id'])

        # get extra data from history file that is missing from voter file
        voter_df['gender'] = voter_groups['voter_sex'].apply(lambda x: list(x)[-1])
        voter_df['registration_date'] = \
            voter_groups['voter_registrationDate'].apply(lambda x: list(x)[-1])

        self.column_check(list(voter_df.columns))

        voter_df = self.config.coerce_dates(voter_df)

        voter_df['all_history'] = voter_groups['election_name'].apply(list)
        voter_df['sparse_history'] =  voter_df['all_history'].map(insert_code_bin)
        voter_df['party_history'] = voter_groups['voter_party'].apply(list)
        voter_df['votetype_history'] = voter_groups['ballot_type'].apply(list)

        expected_cols = self.config['ordered_columns'] + \
                        self.config['ordered_generated_columns']
        voter_df = self.reconcile_columns(voter_df, expected_cols)
        voter_df = voter_df[expected_cols]

        self.meta = {
            "message": "new_jersey2_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(voter_df.to_csv(encoding='utf-8',
                                                        index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_wisconsin(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip")

        if not self.ignore_checks:
            self.file_check(len(new_files))

        config = Config("wisconsin")
        preferred_files = [x for x in new_files
                           if (".txt" in x["name"])]
        if len(preferred_files) > 0:
            main_file = preferred_files[0]
        else:
            main_file = new_files[0]

        main_df = self.read_csv_count_error_lines(
            main_file["obj"], sep='\t', error_bad_lines=False)
        logging.info("dataframe memory usage: {}".format(main_df.memory_usage(deep=True).sum()))
        # convert "Voter Status" to "voter_status" for backward compatibility
        main_df.rename(columns={"Voter Status": self.config["voter_status"]},
                       inplace=True)
        # drop rows with nan values for voterid and county
        main_df.dropna(subset=['Voter Reg Number', 'County'], inplace=True)
        gc.collect()
        # dummy columns for party and birthday
        main_df[self.config["party_identifier"]] = np.nan
        main_df[self.config["birthday_identifier"]] = 0

        def parse_histcols(col_name):
            try:
                parser.parse(col_name)
                return True
            except ValueError:
                return False

        # iterate through thethe dataframe, each column election column for wisconsin
        # has a monthname and year
        valid_elections = []
        for column in main_df:
            if parse_histcols(column):
                valid_elections.append(column)
        self.column_check(list(set(main_df.columns) - set(valid_elections)))
        # sort from oldest election available to newest
        valid_elections = sorted(valid_elections, key=lambda date: parser.parse(date))

        # election_counts: a pd series of the valid elections the the vote counts per election
        election_counts = main_df[valid_elections].count()
        # returns the decreasing counts of people who voted per election

        # election_counts.index[i] contains the election "name"
        # k contains the count of people who voted in that elections
        sorted_codes_dict = {
            election_counts.index[i]: {"index": i, "count": k,
                                       "date": str(datetime.strptime(
                                           election_counts.index[i], '%B%Y').date().strftime('%m/%d/%Y'))}
            for i, k in enumerate(election_counts)}

        sorted_codes = list(election_counts.index)

        def get_all_history(row):
            hist = []
            for i, k in row.iteritems():
                if pd.notnull(k):
                    hist.append(i)
            return hist

        def get_type_history(row):
            type_hist = []
            for i, k in row.iteritems():
                if pd.notnull(k):
                    hist = k.replace(" ", "")
                    type_hist.append(hist)
            return type_hist

        def insert_code_bin(row):
            sparse_hist = []
            for i, k in row.iteritems():
                if pd.notnull(k):
                    sparse_hist.append(sorted_codes_dict[i]["index"])
            return sparse_hist

        main_df['sparse_history'] = main_df[valid_elections].apply(insert_code_bin, axis=1)
        main_df['all_history'] = main_df[valid_elections].apply(get_all_history, axis=1)
        main_df['votetype_history'] = main_df[valid_elections].apply(get_type_history, axis=1)

        main_df.drop(columns=valid_elections, inplace=True)
        gc.collect()

        main_df = self.config.coerce_numeric(main_df, extra_cols=[
            'HouseNumber', 'ZipCode', 'UnitNumber'])
        main_df = self.config.coerce_dates(main_df)
        main_df = self.config.coerce_strings(main_df)

        self.meta = {
            "message": "wisconsin_{}".format(datetime.now().isoformat()),
            "array_encoding": sorted_codes_dict,
            "array_decoding": sorted_codes,
        }
        # need to tell it it's not compressed for test files otherwise it creates a malformed object
        # self.is_compressed = False
        logging.info("Wisconsin: writing out")

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(main_df.to_csv(encoding='utf-8',
                                                       index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_new_hampshire(self):
        config = Config('new_hampshire')
        new_files = self.unpack_files(file_obj=self.main_file,
                                      compression='unzip')
        if not self.ignore_checks:
            self.file_check(len(new_files))

        for f in new_files:
            # ignore ".mdb" files
            if ('.xlsx' in f['name']) or ('.csv' in f['name']):

                if ('history' in f['name'].lower()):
                    logging.info("Found history file: {}".format(f['name']))
                    if '.xlsx' in f['name']:
                        hist_df = pd.read_excel(f['obj'])
                    else:
                        hist_df = self.read_csv_count_error_lines(
                            f['obj'], error_bad_lines=False)
                    hist_df.drop_duplicates(inplace=True)

                elif ('checklist' in f['name'].lower()) or \
                     ('voters' in f['name'].lower()):
                    logging.info("Found voter file: {}".format(f['name']))
                    if '.xlsx' in f['name']:
                        voters_df = pd.read_excel(f['obj'])
                    else:
                        voters_df = self.read_csv_count_error_lines(
                            f['obj'], error_bad_lines=False)

        # add dummy columns for birthday and voter_status
        voters_df[self.config['birthday_identifier']] = 0
        voters_df[self.config['voter_status']] = np.nan

        self.column_check(list(voters_df.columns))
        voters_df = self.config.coerce_strings(voters_df)
        voters_df = self.config.coerce_numeric(
            voters_df, extra_cols=['ad_str3', 'mail_str3'])

        # collect histories
        hist_df['combined_name'] = hist_df['election_name'].str.replace(
            ' ', '_').str.lower() + '_' + hist_df['election_date']

        sorted_codes = hist_df['combined_name'].unique().tolist()
        sorted_codes.sort(key=lambda x: datetime.strptime(
            x.split('_')[-1], "%m/%d/%Y"))
        counts = hist_df['combined_name'].value_counts()
        sorted_codes_dict = {k: {'index': i,
                                 'count': int(counts.loc[k]),
                                 'date': k.split('_')[-1]}
                             for i, k in enumerate(sorted_codes)}

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]['index'] for k in arr]
            else:
                return np.nan

        voters_df = voters_df.set_index('id_voter', drop=False)
        voter_id_groups = hist_df.groupby('id_voter')
        voters_df['all_history'] = voter_id_groups['combined_name'].apply(list)
        voters_df['sparse_history'] = voters_df['all_history'].map(insert_code_bin)
        voters_df['election_type_history'] = voter_id_groups['election_type'].apply(list)
        voters_df['election_category_history'] = voter_id_groups['election_category'].apply(list)
        voters_df['votetype_history'] = voter_id_groups['ballot_type'].apply(list)
        voters_df['party_history'] = voter_id_groups['cd_part_voted'].apply(list)
        voters_df['town_history'] = voter_id_groups['town'].apply(list)

        self.meta = {
            "message": "new_hampshire_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(voters_df.to_csv(index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_virginia(self):
        new_files = self.unpack_files(file_obj=self.main_file,
                                      compression='unzip')

        # throw exception if missing one of the two files needed for processing
        valid_files = []
        for file in new_files:
            valid_files.append(file['name'].lower())

        if not self.ignore_checks:
            self.file_check(len(new_files))
        # faster to just join them into a tab separated string
        valid_files = '\t'.join(valid_files)
        if 'history' not in valid_files or 'registered' not in valid_files:
            raise ValueError("must supply both history and voter file")

        for f in new_files:
            if 'history' in f['name'].lower():
                logging.info('vote history found')
                hist_df = self.read_csv_count_error_lines(f['obj'], error_bad_lines=False, encoding="ISO-8859-1")
            elif 'registered' in f['name'].lower():
                logging.info("voter file found")
                voters_df = self.read_csv_count_error_lines(f['obj'], error_bad_lines=False, encoding="ISO-8859-1")
        voters_df[self.config["party_identifier"]] = np.nan
        self.column_check(list(voters_df.columns))
        voters_df = self.config.coerce_strings(voters_df)
        voters_df = self.config.coerce_numeric(
                voters_df, extra_cols=['TOWNPREC_CODE_VALUE', 'SUPERDIST_CODE_VALUE',
                                       'HOUSE_NUMBER', 'MAILING_ZIP' ])
        voters_df = self.config.coerce_dates(voters_df)

        hist_df['combined_name'] = hist_df['ELECTION_NAME'].str.replace(
                ' ', '_').str.lower() + '_' + hist_df['ELECTION_DATE']

        # Gathers the votetype columns that are initially boolean and replaces them with the word version of their name
        # collect all the columns where the value is True, combine to one votetype history separated by underscores
        # parsing in features will pull out the appropriate string
        hist_df['votetype_history'] = np.where(hist_df['VOTE_IN_PERSON'], 'inPerson_', '')
        hist_df['votetype_history'] += np.where(hist_df['PROTECTED'], 'protected_', '')
        hist_df['votetype_history'] += np.where(hist_df['ABSENTEE'], 'absentee_', '')
        hist_df['votetype_history'] += np.where(hist_df['PROVISIONAL'], 'provisional_', '')
        # replace the empty strings with nan for cleaner db cell values
        hist_df['votetype_history'].replace('', np.nan, inplace=True)

        sorted_codes = hist_df['combined_name'].unique().tolist()
        sorted_codes.sort(key=lambda x: datetime.strptime(
                x.split('_')[-1], "%m/%d/%Y"))
        counts = hist_df['combined_name'].value_counts()

        sorted_codes_dict = {k: {'index': i,
                                 'count': int(counts.loc[k]),
                                 'date': k.split('_')[-1]}
                             for i, k in enumerate(sorted_codes)}

        def insert_code_bin(arr):
            if isinstance(arr, list):
                return [sorted_codes_dict[k]['index'] for k in arr]
            else:
                return np.nan
        voters_df = voters_df.set_index('IDENTIFICATION_NUMBER', drop=False)
        voter_id_groups = hist_df.groupby('IDENTIFICATION_NUMBER')
        voters_df['all_history'] = voter_id_groups['combined_name'].apply(list)
        voters_df['sparse_history'] = voters_df['all_history'].map(insert_code_bin)
        voters_df['election_type_history'] = voter_id_groups['ELECTION_TYPE'].apply(list)
        voters_df['party_history'] = voter_id_groups['PRIMARY_TYPE_CODE_NAME'].apply(list)
        voters_df['votetype_history'] = voter_id_groups['votetype_history'].apply(list)
        gc.collect()

        self.meta = {
                "message": "virginia_{}".format(datetime.now().isoformat()),
                "array_encoding": json.dumps(sorted_codes_dict),
                "array_decoding": json.dumps(sorted_codes),
        }

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(voters_df.to_csv(index=False)),
                        s3_bucket=self.s3_bucket)

    def preprocess_washington(self):
        new_files = [n for n in self.unpack_files(self.main_file, compression='unzip') \
            if ('pdf' not in n['name'].lower())]

        # there should be only one voter file
        voter_file = [n for n in new_files if 'vrdb' in n['name'].lower()][0]
        hist_files = [n for n in new_files if 'history' in n['name'].lower()]

        df_voter = pd.read_csv(voter_file['obj'], sep='\t', encoding='latin-1', dtype=str)
        df_hist = pd.concat([pd.read_csv(n['obj'], sep='\t', encoding='latin-1', dtype=str) \
                                for n in hist_files], ignore_index=True)

        # --- handling the voter history file --- #

        # can't find voter history documentation in any yaml, hardcoding column name
        election_dates = pd.to_datetime(df_hist.loc[:,'ElectionDate'], errors='coerce').dt

        elections, counts = np.unique(election_dates.date, return_counts=True)
        sorted_elections_dict = {str(k): {'index': i,
                                          'count': int(counts[i]),
                                          'date': k.strftime('%Y-%m-%d')}
             for i, k in enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'all_history'] = election_dates.date.apply(str)
        df_hist.loc[:, 'sparse_history'] = df_hist.loc[:, 'all_history'].map(
            lambda x: int(sorted_elections_dict[x]['index']))
        df_hist.loc[:, 'county_history'] = df_hist.loc[:, self.config['primary_locale_identifier']]

        voter_groups = df_hist.groupby(self.config['voter_id'])
        all_history = voter_groups['all_history'].apply(list)
        sparse_history = voter_groups['sparse_history'].apply(list)
        county_history = voter_groups['county_history'].apply(list)
        df_hist = pd.concat([all_history, sparse_history, county_history], axis=1)

        # --- handling the voter file --- #

        # some columns have become obsolete
        df_voter = df_voter.loc[:, df_voter.columns.isin(self.config['column_names'])]
        df_voter = df_voter.set_index(self.config['voter_id'])

        # pandas loads any numeric column with NaN values as floats
        # causing formatting trouble during execute() with a few columns
        # saw this solution in other states (arizona & texas)
        to_numeric = [df_voter.loc[:,col].str.isnumeric().all() for col in df_voter.columns]
        df_voter.loc[:,to_numeric] = df_voter.loc[:,to_numeric].fillna(-1).astype(int)

        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_strings(df_voter,
            exclude=[self.config['primary_locale_identifier'], self.config['voter_id']])
        df_voter = self.config.coerce_dates(df_voter)

        # add voter history
        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'washington_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        self.is_compressed = False

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_west_virginia(self):
        new_files = [n for n in self.unpack_files(self.main_file, compression='unzip')]

        # only one voter file, no voter history
        voter_file = [n for n in new_files if 'wv' in n['name'].lower()][0]

        df_voter = pd.read_csv(voter_file['obj'], sep='|',
            encoding='latin-1',
            dtype=str,
            header=0)

        # --- handling voter file --- #

        gender_conversion_dict = {}
        for c, v in self.config['gender_codes'].items():
            for i in v:
                if i is None:
                    gender_conversion_dict[' '] = c
                gender_conversion_dict[i] = c

        df_voter.loc[:, 'SEX'] = df_voter.loc[:, 'SEX'].map(gender_conversion_dict)

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_strings(df_voter, exclude=[self.config['voter_id']])

        # coerce_strings does not convert party_identifier but conversion is needed in this instance
        df_voter.loc[:,self.config['party_identifier']] = (df_voter
            .loc[:,self.config['party_identifier']]
            .str.replace('\W', ' ').str.strip())

        df_voter = df_voter.set_index(self.config['voter_id'])

        self.meta = {
            'message': 'west_virginia_{}'.format(datetime.now().isoformat())
# vote history not available
#            'array_encoding': json.dumps(),
#            'array_decoding': json.dumps()
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_oregon(self):
        new_files = [f for f in
                     self.unpack_files(self.main_file, compression='unzip')
                     if 'readme' not in f['name'].lower()]

        voter_file = [n for n in new_files if not 'readme' in n['name'].lower()][0]
        # don't have access to them yet
        # hist_files = ...

        # --- handling voter file --- #

        df_voter = (pd.read_csv(voter_file['obj'], sep='\t', dtype=str)
                    .dropna(how='all', axis=1))

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_strings(df_voter,
                                              exclude=['STATE'])
        df_voter = self.config.coerce_numeric(df_voter)

        df_voter.loc[:,self.config['voter_id']] = df_voter.loc[:,self.config['voter_id']].str.zfill(9).astype('str')
        df_voter.loc[:,'UNLISTED'] = df_voter.loc[:,'UNLISTED'].map({'yes':True, 'no':False})
        # when vote history is received

        self.meta = {
            'message': 'oregon_{}'.format(datetime.now().isoformat()),
            # vote history not available yet
            # 'array_encoding': json.dumps(),
            # 'array_decoding': json.dumps()
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=None, encoding='latin-1')))

    def preprocess_oklahoma(self):
        new_files = self.unpack_files(self.main_file)

        voter_files = [n for n in new_files if 'vr.csv' in n['name'].lower()]
        hist_files = [n for n in new_files if 'vh.csv' in n['name'].lower()]
        precinct_file = [n for n in new_files if 'precinct' in n['name'].lower()][0]

        # --- handling the vote history file --- #

        df_hist = pd.concat([pd.read_csv(n['obj'], dtype=str) for n in hist_files],
                            ignore_index=True)

        election_dates = pd.to_datetime(df_hist.loc[:,'ElectionDate'], errors='coerce').dt
        elections, counts = np.unique(election_dates.date, return_counts=True)
        sorted_elections_dict = {str(k): {'index': i,
                                          'count': int(counts[i]),
                                          'date': k.strftime('%m/%d/%Y')}
                                 for i, k in enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'all_history'] = election_dates.date.apply(str)
        df_hist.loc[:, 'sparse_history'] = df_hist.loc[:, 'all_history'].map(
            lambda x: int(sorted_elections_dict[x]['index']))

        voter_groups = df_hist.groupby(self.config['voter_id'])
        all_history = (voter_groups['all_history'].apply(list))
        sparse_history = (voter_groups['sparse_history'].apply(list))
        votetype_history = (voter_groups['VotingMethod'].apply(list).rename('votetype_history'))
        df_hist = pd.concat([all_history, sparse_history, votetype_history], axis=1)

        # --- handling the voter file --- #

        # no primary locale column, county code is in file name only
        dfs = []
        for n in voter_files:
            df = pd.read_csv(n['obj'], dtype=str)
            df.loc[:, 'county_code'] = str(n['name'][-9:-7])
            dfs.append(df)

        df_voter = pd.concat(dfs, ignore_index=True)

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_strings(df_voter, exclude=[self.config['voter_id']])
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = df_voter.loc[:, ~df_voter.columns.str.contains('Hist\w+\d')]
        df_voter = df_voter.set_index(self.config['voter_id'])

        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'oklahoma_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
            }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_arkansas(self):
        new_files = self.unpack_files(self.main_file)

        voter_file = [n for n in new_files if 'vr.csv' == n['name'].lower()][0]
        hist_file = [n for n in new_files if 'vh.csv' == n['name'].lower()][0]

        # --- handling the vote history file --- #
        df_hist = pd.read_csv(hist_file['obj'], dtype=str)

        elections = pd.Series(self.config['elections'])
        election_votetype = elections + 'HowVoted'
        election_party = elections + 'PartyVoted'
        election_county = elections + 'CountyVotedIn'

        election_cols = zip(*[elections, election_votetype, election_party, election_county])

        election_dfs = []
        for e in election_cols:
            election_df = df_hist.set_index(self.config['voter_id'])
            election_df = election_df.loc[:, election_df.columns.isin(e)]
            election_df = election_df.dropna(how='all')
            election_df.columns = ['all_history', 'county_history', 'party_history', 'votetype_history']
            election_df.loc[:, 'all_history'] = e[0]
            election_dfs.append(election_df.reset_index())

        df_hist = pd.concat(election_dfs, ignore_index=True)
        df_hist = df_hist.fillna('NP').applymap(lambda x: x.strip(' '))

        elections, counts = np.unique(df_hist.all_history, return_counts=True)
        order = np.argsort(counts)[::-1]
        counts = counts[order]
        elections = elections[order]
        election_years = list(pd.to_datetime(('20' + pd.Series(elections).str.extract('(\d{2}(?!\d))', expand=False))).dt.year)

        sorted_elections_dict = {k: {'index': i,
                                     'count': int(counts[i]),
                                     'date': str(election_years[i])}
                                 for i, k in enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = df_hist.all_history.map(lambda x:
            int(sorted_elections_dict[x]['index']))

        group = df_hist.groupby(self.config['voter_id'])
        df_hist = pd.concat([group[col].apply(list) for col in df_hist.columns[1:]], axis=1)

        # --- handling the voter file --- #
        df_voter = pd.read_csv(voter_file['obj'], dtype=str)

        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_strings(df_voter,
                                              exclude=[self.config['voter_id']])

        df_voter = df_voter.set_index(self.config['voter_id']).join(df_hist)

        self.meta = {
            'message': 'arkansas_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_wyoming(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')

        voter_file = [n for n in new_files if 'statewide' in n['name'].lower()][0]
        hist_files = [n for n in new_files if 'history' in n['name'].lower()]

        # --- handling voter history --- #

        election_col = self.config['election_columns']
        elections = self.config['elections']

        df_hist = []

        for file in hist_files:
            text = file['obj'].readline()
            file['obj'].seek(0)

            if b'\t' in text:
                df = pd.read_csv(file['obj'], sep='\t', dtype=str)
            elif b',' in text:
                df = pd.read_csv(file['obj'], sep=',', dtype=str)

            election_type = file['name'][:file['name'].find(' Vot')]

            if not election_type in elections:
                print('Warning:', election_type, 'not in documentation. Some fields may be excluded.')

            for var, names in election_col.items():
                for col in df.columns:
                    if col in names:
                        df = df.rename({col: var}, axis=1)
                if var not in df.columns:
                    df.loc[:, var] = 'NP'

            df = df.loc[:, election_col.keys()]

            df.loc[:, 'election_type'] = election_type

            df_hist.append(df)

        df_hist = (pd.concat(df_hist, ignore_index=True)
            .dropna(how='any')
            .applymap(lambda x: str(x).strip()))

        df_hist.loc[:,'all_history'] = (df_hist.loc[:,'election_type']
            .str.lower()
            .str.extract('(\d+\s+[g|p]\w+)', expand=False)
            .str.split('\s').str.join('_'))
        df_hist.loc[:,'election_date'] = pd.to_datetime(df_hist.loc[:,'election_date'].replace('NP', pd.NaT)).dt.strftime('%m/%d/%Y')

        election_dates_dict = df_hist.groupby('all_history')['election_date'].first().to_dict()
        elections, counts = np.unique(df_hist.loc[:,'all_history'], return_counts=True)

        sorted_elections_dict = {str(k): {'index': i,
                                          'count': int(counts[i]),
                                          'date': election_dates_dict[k]}
                                 for i, k in enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = df_hist.loc[:, 'all_history'].map(
            lambda x: int(sorted_elections_dict[x]['index']))

        voter_groups = df_hist.sort_values('election_type').groupby(self.config['voter_id'])

        all_history = voter_groups['all_history'].apply(list)
        sparse_history = voter_groups['sparse_history'].apply(list)
        votetype_history = (voter_groups['vote_method'].apply(list)
            .rename('votetype_history'))
        party_history = (voter_groups[self.config['party_identifier']].apply(list)
            .rename('party_history'))
        precinct_history = (voter_groups['precinct'].apply(list)
            .rename('precinct_history'))

        df_hist = pd.concat(
            [all_history, sparse_history, votetype_history,
             party_history, precinct_history], axis=1)

        # --- handling voter file --- #

        df_voter = pd.read_csv(voter_file['obj'], dtype=str)

        df_voter = self.config.coerce_strings(df_voter,
            exclude=[self.config['voter_id']])
        df_voter = self.config.coerce_numeric(df_voter,
            extra_cols=['Zip (RA)', 'Split', 'Precinct', 'ZIP (MA)', 'House', 'Senate'])
        df_voter = self.config.coerce_dates(df_voter)

        df_voter.loc[:,self.config['voter_id']] = df_voter.loc[:,self.config['voter_id']].str.zfill(9).astype(str)
        df_voter = df_voter.set_index(self.config['voter_id'])

        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'wyoming_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        self.is_compressed = False

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_rhode_island(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')

        hist_file = [n for n in new_files if 'history.txt' in n['name'].lower()][0]
        voter_file = [n for n in new_files if 'voter.txt' in n['name'].lower()][0]

        # --- handling voter history --- #

        df_hist = pd.read_csv(hist_file['obj'], skiprows=1, sep='|', dtype=str)

        election_keys = ['election_names',
                         'election_dates',
                         'election_precints',
                         'election_party',
                         'election_votetype']

        election_dfs = []
        for election in zip(*[self.config[k] for k in election_keys]):
            cols = [self.config['voter_id']] + list(election)
            election_df = df_hist.loc[:, cols].dropna()
            election_df.columns = [self.config['voter_id'],
                                   'all_history',
                                   'date',
                                   'precinct_history',
                                   'party_history',
                                   'votetype_history']
            election_dfs.append(election_df)

        election_df = pd.concat(election_dfs, ignore_index=True)

        election_dates = pd.to_datetime(election_df.date)
        election_df.loc[:, 'date'] = election_dates.astype(str)
        election_df.loc[:, 'all_history'] = (election_dates.dt.strftime('%Y_%m_%d_')
                                             + election_df.all_history.str.split(' ').str.join('_'))

        elections = (election_df.groupby(['all_history', 'date'])[self.config['voter_id']]
                     .count().reset_index().values)

        sorted_elections_dict = {k[0]: {'index': i,
                                        'count': int(k[2]),
                                        'date': k[1]}
                                         for i, k in enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        election_df.loc[:, 'sparse_history'] = election_df.loc[:, 'all_history'].map(
            lambda x: int(sorted_elections_dict[x]['index']))

        df_group = election_df.sort_values('date', ascending=True).groupby(self.config['voter_id'])
        df_hist = pd.concat([df_group[c].apply(list) for c in election_df.columns if 'history' in c], axis=1)

        # --- handling vote file --- #

        df_voter = pd.read_csv(voter_file['obj'], sep='|', skiprows=1, dtype=str)
        df_voter = self.config.coerce_strings(df_voter,
            exclude=[self.config['voter_id']])
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter.loc[:, 'ZIP CODE'] = df_voter.loc[:, 'ZIP CODE'].astype(str).str.zfill(5).fillna('-')
        df_voter.loc[:, 'ZIP4 CODE'] = df_voter.loc[:, 'ZIP4 CODE'].fillna('0').astype(int).astype(str)

        df_voter = df_voter.set_index(self.config['voter_id']).join(df_hist)

        self.meta = {
            'message': 'rhode_island_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_south_dakota(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')

        voter_file = [n for n in new_files if 'searchexport' in n['name'].lower()][0]
        hist_file = [n for n in new_files if 'history' in n['name'].lower()][0]

        # --- handling voter history --- #

        df_hist = pd.read_csv(hist_file['obj'], dtype=str).rename(self.config['election_columns'], axis=1)

        df_hist.loc[:, 'all_history'] = df_hist.date + '_' + df_hist.election.str.lower()

        elections = (df_hist.groupby(['all_history', 'date'])[self.config['voter_id']]
                     .count().reset_index()
                     .values)
        sorted_elections_dict = {
            k[0]: {'index': i,
                   'count': int(k[2]),
                   'date': k[1]} for i, k in  enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = df_hist.loc[:, 'all_history'].map(
            lambda x: sorted_elections_dict[x]['index'])

        voter_groups = df_hist.groupby(self.config['voter_id'])
        df_hist = pd.concat(
            [voter_groups[c].apply(list) for c in
             ['all_history', 'sparse_history', 'votetype_history']
             ], axis=1)

        # --- handling voter file --- #

        df_voter = pd.read_csv(voter_file['obj'], skiprows=2, dtype=str)
        df_voter = self.config.coerce_strings(df_voter,
            exclude=[self.config['voter_id']])
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.set_index(self.config['voter_id']).join(df_hist)

        self.meta = {
            'message': 'south_dakota_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_montana(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')

        voter_file = [n for n in new_files if 'voter_ex' in n['name'].lower()][0]
        hist_file = [n for n in new_files if 'voter_his' in n['name'].lower()][0]

        # --- handling voter history --- #

        df_hist = (pd.read_csv(hist_file['obj'], dtype=str)
                   .rename({'Voter ID': self.config['voter_id']}, axis=1))

        election_codes = {str(v):k for k, v in self.config['election_codes'].items()}
        votetype_codes = {str(v):k for k, v in self.config['votetype_codes'].items()}

        df_hist.loc[:, 'BALLOTSTAGE/STATUS'] = (df_hist.loc[:, 'BALLOTSTAGE/STATUS']
                                                .map({
                                                    'Processed/Accepted': 'absentee-ACCEPTED',
                                                    'Sent': 'absentee-SENT',
                                                    'Processed/Rejected': 'absentee-REJECTED',
                                                    'Undeliverable': 'absentee-UNDELIVERABLE'
                                                      }).fillna('non-absentee'))

        df_hist = df_hist.loc[df_hist['BALLOTSTAGE/STATUS'].isin(['non-absentee', 'absentee-ACCEPTED']), :]

        df_hist.loc[:, 'ELECTION_TYPE'] = df_hist.loc[:, 'ELECTION_TYPE'].map(election_codes)

        # if the election code does not exist, take a clean version of the election description
        df_hist.loc[df_hist['ELECTION_TYPE'].isna(), 'ELECTION_DESCRIPTION'] = (df_hist
                                                                                .loc[df_hist['ELECTION_TYPE'].isna(),
                                                                                     'ELECTION_DESCRIPTION']
                                                                                .str.lower()
                                                                                .str.split(' ')
                                                                                .str.join('_'))
        # will use later
        election_dates = pd.to_datetime(df_hist.loc[:, 'ELECTION_DATE'])
        df_hist.loc[:, 'ELECTION_DATE'] = election_dates.dt.strftime('%Y-%m-%d')

        # creating election ids
        df_hist.loc[:, 'all_history'] = (election_dates.dt.strftime('%Y_%m_%d_') + df_hist.loc[:, 'ELECTION_TYPE'])
        df_hist.loc[:, 'votetype_history'] = df_hist.loc[:, 'VVM_ID'].map(votetype_codes)
        df_hist.loc[:, 'county_history'] = df_hist.loc[:, 'JS_CODE'].fillna(0)

        elections = (df_hist.groupby(['all_history', 'ELECTION_DATE'])[self.config['voter_id']]
                     .count().reset_index().values)

        sorted_elections_dict = {k[0]: {'index': i,
                                        'count': int(k[2]),
                                        'date': str(k[1])} for i, k in  enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = df_hist.loc[:, 'all_history'].map(lambda x: sorted_elections_dict[x]['index'])

        df_hist = df_hist.loc[:, [self.config['voter_id'],
                                  'all_history',
                                  'votetype_history',
                                  'county_history',
                                  'sparse_history']]

        df_group = df_hist.groupby(self.config['voter_id'])
        groups = []
        for col in df_hist.columns[1:]:
            group = df_group[col].apply(list)
            groups.append(group)

        df_hist = pd.concat(groups, axis=1)

        # --- handling voter file --- #

        df_voter = pd.read_csv(voter_file['obj'], sep='\t', index_col=False)
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.set_index(self.config['voter_id']).join(df_hist)

        self.meta = {
            'message': 'montana_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_alaska(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')
        voter_file = [n for n in new_files if 'voter' in n['name'].lower()][0]

        df_voter = pd.read_csv(voter_file['obj'], dtype=str).drop('UN', axis=1)
        df_hist = df_voter.loc[:, [self.config['voter_id']] + self.config['election_columns']]

        # --- handling the vote history file --- #

        df_hist = df_hist.set_index(self.config['voter_id']).stack().reset_index().iloc[:, [0, 2]]
        df_hist.columns = [self.config['voter_id'], 'election']

        df_hist = df_hist.join(df_hist.election.str.split(' ', expand=True))
        df_hist = df_hist.rename({0: 'all_history', 1: 'votetype_history'}, axis=1)

        df_hist = df_hist.join(df_hist.all_history.str.split('(?<=^\d{2})', expand=True))
        df_hist = df_hist.rename({0: 'election_year', 1: 'election_type'}, axis=1)

        df_hist.election_year = '20' + df_hist.election_year

        elections, counts = np.unique(df_hist.loc[:, ['all_history', 'election_year']]
                                      .apply(tuple, axis=1), return_counts=True)

        sorted_elections_dict = {k[0]: {'index': i,
                                        'count': int(counts[i]),
                                        'date': int(k[1])}
                                 for i, k in enumerate(elections)}
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = df_hist.all_history.map(lambda x: sorted_elections_dict[x]['index'])

        df_hist = pd.concat([df_hist.groupby(self.config['voter_id'])[c].apply(list)
                             for c in ['all_history',
                                       'votetype_history',
                                       'sparse_history']], axis=1)

        # --- handling the voter file --- #

        df_voter = df_voter.loc[:, ~df_voter.columns.isin(self.config['election_columns'])]
        df_voter = df_voter.set_index(self.config['voter_id'])

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'alaska_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_connecticut(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')
        voter_files = [n for n in new_files if 'EXT' in n['name']]

        election_columns = self.config['election_columns']
        electiontype_columns = self.config['electiontype_columns']
        votetype_columns = self.config['votetype_columns']
        election_date_columns = self.config['election_date_columns']

        electiontype_codes = {v: k for k, v in self.config['election_type_code'].items()}
        votetype_codes = {v: k for k, v in self.config['absentee_ballot_code'].items()}

        df_voter = pd.concat([pd.read_csv(f['obj'],
                                          names=self.config['column_names'],
                                          index_col=False,
                                          sep=',',
                                          dtype=str,
                                          skipinitialspace=True)
                              for f in voter_files], ignore_index=True)

        # --- handling the vote history file --- #

        df_hist = df_voter.set_index(self.config['voter_id']).loc[:, election_columns]

        election_df = []
        election_zip = list(zip(
            election_date_columns,
            electiontype_columns,
            votetype_columns))

        for c in election_zip:
            election = df_hist.loc[~df_hist[c[0]].isna(), c]
            election.columns = ['electiondate', 'electiontype', 'votetype']
            electiondate = pd.to_datetime(election.loc[:, 'electiondate'])

            election.loc[:, 'electiondate'] = electiondate
            election.loc[:, 'electiontype'] = election.loc[:, 'electiontype'].str.strip().map(electiontype_codes).fillna('NP').str.lower()
            election.loc[:, 'votetype_history'] = election.loc[:, 'votetype'].str.strip().map(votetype_codes).fillna('NP').str.lower()
            election.loc[:, 'all_history'] = electiondate.dt.strftime('%Y_%m_%d_') + election.loc[:, 'electiontype']

            election = election.loc[:, ['electiondate', 'votetype_history', 'all_history']]

            election_df.append(election)

        df_hist = pd.concat(election_df).reset_index()

        elections = (df_hist
                     .groupby(['all_history', 'electiondate'])
                     [self.config['voter_id']]
                     .count().reset_index().values)
        sorted_elections_dict = {
            k[0]: {'index': i,
                   'count': int(k[2]),
                   'date': k[1].strftime('%Y-%m-%d')
            } for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist = df_hist.drop('electiondate', axis=1)

        df_hist.loc[:, 'sparse_history'] = (df_hist
                                            .all_history
                                            .map(lambda x: sorted_elections_dict[x]['index']))

        group = df_hist.groupby(self.config['voter_id'])
        election_df = []

        for c in df_hist.columns[1:]:
            election_df.append(group[c].apply(list))

        df_hist = pd.concat(election_df, axis=1)

        # --- handling the voter file --- #

        df_voter = df_voter.loc[:, ~df_voter.columns.isin(election_columns)]
        df_voter = df_voter.set_index(self.config['voter_id'])

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'connecticut_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_vermont(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')
        voter_file = [n for n in new_files if 'voter file' in n['name'].lower()][0]

        df_voter = (pd.read_csv(voter_file['obj'], sep='|', dtype=str)
                    .iloc[:, :len(self.config['column_names'])])
        df_voter.columns = self.config['column_names']

        df_hist = df_voter.loc[:, [self.config['voter_id']] + self.config['election_columns']]
        for c in df_hist.columns[1:]:
            df_hist.loc[:, c] = (df_hist
                                 .loc[:, c]
                                 .map({'T': c[:c.find(' Part')].replace(' ', '_'),
                                       'F': np.nan}))

        df_hist = (df_hist
                   .set_index(self.config['voter_id'])
                   .stack().reset_index(level=1, drop=True)
                   .reset_index())
        df_hist.columns = [self.config['voter_id'], 'all_history']

        elections, counts = np.unique(df_hist.all_history, return_counts=True)

        sorted_elections_dict = {
            k: {
                'index': i,
                'count': int(counts[i]),
                'date': str(k[:4])
            } for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = (df_hist
                                            .all_history
                                            .apply(lambda x: sorted_elections_dict[x]['index']))

        group = df_hist.groupby(self.config['voter_id'])
        df_hist = pd.concat(
            [group[col].apply(list) for col in df_hist.columns[1:]],
            axis=1)

        df_voter = df_voter.loc[:, ~df_voter.columns.isin(self.config['election_columns'])]
        df_voter = df_voter.set_index(self.config['voter_id'])

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'vermont_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_delaware(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')
        voter_file = [n for n in new_files if 'voter_reg' in n['name'].lower()][0]
        df_voter = pd.read_csv(voter_file['obj'], sep='\t', dtype=str)

        # --- handling vote history --- #

        df_hist = (df_voter
                   .set_index(self.config['voter_id'])
                   .loc[:, self.config['election_columns']]
                   .stack().reset_index(level=1, drop=True)
                   .reset_index())
        df_hist.columns = [self.config['voter_id'], 'all_history']

        elections, counts = np.unique(df_hist.all_history, return_counts = True)
        order = np.argsort(counts)[::-1]
        elections = elections[order]
        counts = counts[order]
        election_year = list(pd.Series(elections)
                             .str[-2:]
                             .apply(lambda x: '20' + x if int(x) < 50 else '19' + x))

        sorted_elections_dict = {
            k: {
                'index': i,
                'count': int(counts[i]),
                'date': str(election_year[i])
            } for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = (df_hist
                                            .all_history
                                            .apply(lambda x: sorted_elections_dict[x]['index']))

        group = df_hist.groupby(self.config['voter_id'])
        df_hist = pd.concat([group[c].apply(list) for c in ['all_history', 'sparse_history']], axis=1)

        # --- handling voter file  --- #

        df_voter = (df_voter
                    .loc[:, ~df_voter.columns.isin(self.config['election_columns'])]
                    .set_index(self.config['voter_id']))
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'delaware_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_maryland(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')
        hist_file = [n for n in new_files if 'history.txt' in n['name'].lower()][0]
        voter_file = [n for n in new_files if 'list.txt' in n['name'].lower()][0]
        # separate file with extra data, absentee votes still counted in voter_file
        abs_file = [n for n in new_files if 'absentee.txt' in n['name'].lower()][0]

        # --- handling voter history --- #
        df_hist = pd.read_csv(hist_file['obj'], sep='\t', dtype=str)

        election_dates = pd.to_datetime(df_hist.loc[:, 'Election Date'])
        election_names = (df_hist
                          .loc[:, 'Election Description']
                          .str.extract('(\D+)', expand=False)
                          .str.strip()
                          .str.replace(' ', '_'))

        df_hist.loc[:, 'all_history'] = (election_dates.dt.strftime('%Y_%m_%d_')
                                         + election_names.str.lower())
        df_hist.loc[:, 'earlyvote_history'] = 'N'
        df_hist.loc[~df_hist.loc[:, 'Early Voting Location'].isna(), 'earlyvote_history'] = 'Y'
        df_hist.loc[:, 'votetype_history'] = (df_hist
                                              .loc[:, 'Voting Method']
                                              .str.replace(' ', '_')
                                              .str.lower())
        df_hist.loc[:, 'party_history'] = (df_hist
                                           .loc[:, 'Political Party']
                                           .str.lower())
        df_hist.loc[:, 'jurisd_history'] = (df_hist
                                            .loc[:, 'Jurisdiction Code']
                                            .astype(str)
                                            .str.strip())
        df_hist.loc[:, 'Election Date'] = pd.to_datetime(df_hist.loc[:, 'Election Date'])

        elections = (df_hist
                     .groupby(['all_history', 'Election Date'])['Voter ID']
                     .count()
                     .reset_index()
                     .values)
        sorted_elections_dict = {
            k[0]: {
                'index': i,
                'count': int(k[2]),
                'date': k[1].strftime('%Y-%m-%d')
            } for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = (df_hist
                                            .all_history
                                            .apply(lambda x: sorted_elections_dict[x]['index']))
        history = list(df_hist.loc[:, 'all_history':].columns)
        df_hist = (df_hist.loc[:, ['Voter ID'] + history]
                   .rename({'Voter ID': self.config['voter_id']}, axis=1))
        group = df_hist.groupby(self.config['voter_id'])
        df_hist = pd.concat([group[c].apply(list) for c in df_hist.columns[1:]], axis=1)

        # --- handling voter file --- #
        df_voter = (pd.read_csv(voter_file['obj'], sep='\t', dtype=str)
                    .iloc[:, :len(self.config['column_names'])]
                    .set_index(self.config['voter_id']))
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)

        df_voter = df_voter.join(df_hist)

        self.meta = {
            'message': 'maryland_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def preprocess_dc(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')
        voter_file = [n for n in new_files if 'voters' in n['name'].lower()][0]

        df_voter = pd.read_excel(voter_file['obj'],
                                 sheet_name='DC VH EXPORT (ALL)',
                                 dtype=str)

        df_voter.loc[:, 'REGISTERED'] = (pd.to_datetime(df_voter.loc[:, 'REGISTERED'])
                                         .dt.strftime('%m/%d/%Y'))

        # --- handling vote history --- #
        df_hist = (df_voter
                   .loc[:, df_voter.columns.isin(self.config['election_columns'])])
        elections = df_hist.columns
        elections = elections.str.extract('(?P<date>\d+)-(?P<type>\D+)')
        election_dates = (elections.date
                          .apply(
                              lambda x:
                                  x[:2] + '/20' + x[-2:] if int(x[-2:]) < 90
                                  else x[:2] + '/19' + x[-2:]))
        election_types = (elections.type
                          .map({'P': 'primary', 'G': 'general', 'S': 'special'}))
        election_keys = (list(
            pd.to_datetime(election_dates, format='%m/%Y').dt.strftime('%Y_%m_')
            + election_types))

        df_hist.columns = election_keys

        votetype_codes = self.config['votetype_codes']
        votetype_codes['N'] = np.nan
        votetype_codes['E'] = np.nan

        df_hist = (df_hist
                   .apply(lambda x: x.map(votetype_codes))
                   .stack().reset_index())
        df_hist.columns = ['index', 'all_history', 'votetype_history']

        elections, counts = np.unique(df_hist.all_history, return_counts=True)
        election_dates = {k: v for k, v in zip(election_keys, election_dates)}
        sorted_elections_dict = {
            k: {
                'index': i,
                'count': int(counts[i]),
                'date': pd.to_datetime(election_dates[k]).strftime('%Y-%m-%d')
            } for i, k in enumerate(elections)
        }
        sorted_elections = list(sorted_elections_dict.keys())

        df_hist.loc[:, 'sparse_history'] = (df_hist
                                            .all_history
                                            .apply(lambda x:
                                                sorted_elections_dict[x]['index']))
        df_hist = pd.concat(
            [df_hist.groupby('index')[c].apply(list) for c in
             ['all_history', 'votetype_history', 'sparse_history']],
            axis=1)

        # --- handling voter file --- #
        df_voter = (df_voter
                    .loc[:, ~df_voter.columns.isin(self.config['election_columns'])])
        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = df_voter.join(df_hist).rename_axis('temp_id')

        self.meta = {
            'message': 'dc_{}'.format(datetime.now().isoformat()),
            'array_encoding': json.dumps(sorted_elections_dict),
            'array_decoding': json.dumps(sorted_elections)
        }

        return FileItem(name='{}.processed'.format(self.config['state']),
                        io_obj=StringIO(df_voter.to_csv(index=True, encoding='latin-1')))

    def execute(self):
        return self.state_router()

    def state_router(self):
        routes = {
            'nevada': self.preprocess_nevada,
            'arizona': self.preprocess_arizona,
            'arizona2': self.preprocess_arizona2,
            'florida': self.preprocess_florida,
            'new_york': self.preprocess_new_york,
            'michigan': self.preprocess_michigan,
            'missouri': self.preprocess_missouri,
            'iowa': self.preprocess_iowa,
            'pennsylvania': self.preprocess_pennsylvania,
            'georgia': self.preprocess_georgia,
            'new_jersey': self.preprocess_new_jersey,
            'new_jersey2': self.preprocess_new_jersey2,
            'north_carolina': self.preprocess_north_carolina,
            'kansas': self.preprocess_kansas,
            'ohio': self.preprocess_ohio,
            'minnesota': self.preprocess_minnesota,
            'texas': self.preprocess_texas,
            'colorado': self.preprocess_colorado,
            'wisconsin': self.preprocess_wisconsin,
            'new_hampshire': self.preprocess_new_hampshire,
            'virginia': self.preprocess_virginia,
            'washington': self.preprocess_washington,
            'west_virginia': self.preprocess_west_virginia,
            'oregon': self.preprocess_oregon,
            'oklahoma': self.preprocess_oklahoma,
            'arkansas': self.preprocess_arkansas,
            'wyoming': self.preprocess_wyoming,
            'rhode_island': self.preprocess_rhode_island,
            'south_dakota': self.preprocess_south_dakota,
            'montana': self.preprocess_montana,
            'alaska': self.preprocess_alaska,
            'connecticut': self.preprocess_connecticut,
            'vermont': self.preprocess_vermont,
            'delaware': self.preprocess_delaware,
            'maryland': self.preprocess_maryland,
            'dc': self.preprocess_dc
        }
        if self.config["state"] in routes:
            f = routes[self.config["state"]]
            logging.info("preprocessing {}".format(self.config["state"]))
            return f()
        else:
            raise NotImplementedError("preprocess_{} has not yet been "
                                      "implemented for the Preprocessor object"
                                      .format(self.config["state"]))


if __name__ == '__main__':
    print(ohio_get_last_updated())
