import uuid
from datetime import datetime
from subprocess import Popen, PIPE
from datetime import date as dt
import pandas as pd
from dateutil import parser
import json
from constants import *
from configs.configs import Config
from storage import generate_s3_key, date_from_str, \
    df_to_postgres_array_string, strcol_to_array, get_surrounding_dates, \
    get_metadata_for_key, format_column_name
from applications.custom_errors import MissingElectionCodesError
from storage import s3, normalize_columns
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


def ohio_get_last_updated():
    html = requests.get("https://www6.sos.state.oh.us/ords/f?p=VOTERFTP:STWD",
                        verify=False).text
    soup = bs4.BeautifulSoup(html, "html.parser")
    results = soup.find_all("td", {"headers": "DATE_MODIFIED"})
    return max(parser.parse(a.text) for a in results)


def nc_date_grab():
    nc_file = urlopen(
        'https://s3.amazonaws.com/dl.ncsbe.gov?delimiter=/&prefix=data/')
    data = nc_file.read()
    nc_file.close()
    root = xml.etree.ElementTree.fromstring(data)

    def nc_parse_xml(file_name):
        for child in root:
            if "Contents" in child.tag:
                z = 0
                for i in child:
                    if file_name in i.text:
                        z += 1
                        continue
                    if z == 1:
                        return i.text
    file_date_vf = nc_parse_xml(file_name="data/ncvoter_Statewide.zip")
    file_date_his = nc_parse_xml(file_name="data/ncvhis_Statewide.zip")
    if file_date_his[0:10] != file_date_vf[0:10]:
        logging.info(
            "Different dates between files, reverting to voter file date")
    file_date_vf = parser.parse(file_date_vf).isoformat()[0:10]
    return file_date_vf


def get_object(key, fn):
    with open(fn, "w+") as obj:
        s3.Bucket(S3_BUCKET).download_fileobj(Key=key, Fileobj=obj)


def get_object_mem(key, bucket=S3_BUCKET):
    file_obj = BytesIO()
    s3.Bucket(bucket).download_fileobj(Key=key, Fileobj=file_obj)
    return file_obj


def concat_and_delete(in_list):
    outfile = StringIO()

    for f_obj in in_list:
        s = f_obj["obj"].read()
        outfile.write(s.decode())
    outfile.seek(0)
    return outfile


class FileItem(object):
    """
    in this case, name is always a string and obj is a StringIO/BytesIO object
    """

    def __init__(self, name, key=None, filename=None, io_obj=None):
        if not any([key, filename, io_obj]):
            raise ValueError("must supply at least one key,"
                             " filename, or io_obj but "
                             "all are none")
        if key is not None:
            self.obj = get_object_mem(key)
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
        return "FileItem: name={}, obj={}, size={}"\
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
                 force_file=None, testing=False):
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
        if force_date is not None:
            self.download_date = parser.parse(force_date).isoformat()
        else:
            self.download_date = datetime.now().isoformat()
        if force_file is not None:
            working_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
            logging.info("copying {} to {}".format(force_file, working_file))
            shutil.copy2(force_file, working_file)
            self.main_file = FileItem(
                "loader_force_file", filename=working_file)
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
        if self.state == "new_york":
            new_loc = "/tmp/voteshield_{}".format(uuid.uuid4())
            with open(new_loc, 'wb') as fh:
                fh.write(file_name.getvalue())
            new_loc_decomp = new_loc + "_decompressed"
            logging.info("decompressing unzip {} into {} (NY file = on disk)"
                         .format(new_loc, new_loc_decomp))
            subprocess.call(['unzip', new_loc, '-d', new_loc_decomp])
            os.remove(new_loc)
            file_names = [os.path.join(new_loc_decomp, f) for f in
                          os.listdir(new_loc_decomp)]
            logging.info("file_names = {}".format(file_names))
            # NY also has memory issues, so just read into csv from disk
            file_objs = [{"name": name, "obj": name} for name in file_names]
        else:
            zip_file = ZipFile(file_name)
            file_names = zip_file.namelist()
            logging.info("decompressing unzip {} into {}".format(file_name,
                                                                 file_names))
            file_objs = []
            for name in file_names:
                file_objs.append({"name": name,
                                  "obj": BytesIO(zip_file.read(name))})

        return file_objs

    def gunzip_decompress(self, file_name):
        """
        handles decompression for .gz files
        :param file_name: .gz file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        gzip_file = GzipFile(file_name)
        return [{"name": "decompressed_file", "obj": gzip_file}]

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

    def sevenzip_decompress(self, file_name):
        """
        handles decompression for 7zip files
        :param file_name: 7zip compressed file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        seven_zip_file = Archive7z(file_name)
        file_names = seven_zip_file.getnames()
        logging.info("decompressing 7zip {} into {}".format(file_name,
                                                            file_names))
        file_objs = [{"name": name, "obj": seven_zip_file.getmember(name)} for
                     name in file_names]
        return file_objs

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
        if compression_type is "infer":
            compression_type = inferred_compression
        logging.info("decompressing {} using {}".format(s3_file_obj["name"],
                                                        compression_type))

        if (s3_file_obj["name"].split(".")[-1] == "xlsx") or \
           (s3_file_obj["name"].split(".")[-1] == "txt") or \
           (s3_file_obj["name"].split(".")[-1] == "pdf"):
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
                new_files = self.gunzip_decompress(bytes_obj)

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
        s3.Object(S3_BUCKET, self.generate_key(file_class=file_class)).put(
            Body=file_item.obj, ServerSideEncryption='AES256')
        s3.Object(S3_BUCKET, self.generate_key(file_class=META_FILE_PREFIX) + ".json").put(
            Body=json.dumps(meta), ServerSideEncryption='AES256')


class Preprocessor(Loader):
    def __init__(self, raw_s3_file, config_file, **kwargs):

        super(Preprocessor, self).__init__(
            config_file=config_file, force_date=date_from_str(raw_s3_file),
            **kwargs)
        self.raw_s3_file = raw_s3_file

        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

    def s3_download(self):
        name = "/tmp/voteshield_{}" \
            .format(self.raw_s3_file.split("/")[-1])

        return FileItem(key=self.raw_s3_file, name=name)

    def unpack_files(self, file_obj, compression="unzip"):
        all_files = []

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

        all_files = [n for n in all_files if ".png" not in n["name"]]

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

    def preprocess_texas(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression='unzip')
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
            if ("count" not in i['name'] and
                    "MACOS" not in i['name'] and
                    "DS_Store" not in i['name'] and file_len != 0):

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
                new_df.columns = self.config.raw_file_columns()
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
                        io_obj=StringIO(df_voter.to_csv()))

    def preprocess_ohio(self):
        new_files = self.unpack_files(file_obj=self.main_file)
        for i in new_files:
            logging.info("Loading file {}".format(i))
            if "_22" in i['name']:
                df = pd.read_csv(i['obj'], encoding='latin-1',
                                 compression='gzip')
            elif ".txt" in i['name']:
                temp_df = pd.read_csv(i['obj'], encoding='latin-1',
                                      compression='gzip')
                df = pd.concat([df, temp_df], axis=0)

        # create history meta data
        voting_history_cols = list(filter(
            lambda x: any([pre in x for pre in (
                "GENERAL-", "SPECIAL-", "PRIMARY-")]), df.columns.values))
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
                        io_obj=StringIO(df.to_csv(encoding='utf-8')))

    def preprocess_minnesota(self):
        logging.info("Minnesota: loading voter file")
        new_files = self.unpack_files(
            compression='unzip', file_obj=self.main_file)
        voter_reg_df = pd.DataFrame(columns=self.config['ordered_columns'])
        voter_hist_df = pd.DataFrame(columns=self.config['hist_columns'])
        for i in new_files:
            if "election" in i['name'].lower():
                voter_hist_df = pd.concat(
                    [voter_hist_df, pd.read_csv(i['obj'])], axis=0)
            elif "voter" in i['name'].lower():
                voter_reg_df = pd.concat(
                    [voter_reg_df, pd.read_csv(i['obj'],
                                               encoding='latin-1')], axis=0)

        voter_reg_df[self.config["voter_status"]] = np.nan
        voter_reg_df[self.config["party_identifier"]] = np.nan
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
                        io_obj=StringIO(voter_reg_df.to_csv()))

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

            if "Registered_Voters_List" in i['name'] and not master_vf_version:
                logging.info("reading in {}".format(i['name']))
                df_voter = pd.concat(
                    [df_voter, pd.read_csv(i['obj'], encoding='latin-1')],
                    axis=0)

            elif "Master_Voting_History" in i['name'] and "MACOS" not in i['name']:
                if "Voter_Details" not in i['name']:
                    logging.info("reading in {}".format(i['name']))
                    new_df = pd.read_csv(i['obj'], compression='gzip')
                    df_hist = pd.concat([df_hist, new_df], axis=0)

                if "Voter_Details" in i['name'] and master_vf_version:
                    logging.info("reading in {}".format(i['name']))
                    new_df = pd.read_csv(i['obj'], compression='gzip')
                    new_df.columns = self.config['master_voter_columns']
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

        df_voter = df_voter.set_index(self.config["voter_id"])

        df_voter["all_history"] = all_history
        df_voter["vote_type"] = vote_type
        gc.collect()

        df_voter = self.config.coerce_strings(df_voter)
        df_voter = self.config.coerce_dates(df_voter)
        df_voter = self.config.coerce_numeric(df_voter)

        self.meta = {
            "message": "Colorado_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        gc.collect()
        logging.info("Colorado: writing out")
        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voter.to_csv(encoding='utf-8')))

    def preprocess_georgia(self):
        config = Config("georgia")
        logging.info("GEORGIA: loading voter and voter history file")
        new_files = self.unpack_files(
            compression='unzip', file_obj=self.main_file)
        vh_files = []
        for i in new_files:
            if "Georgia_Daily_VoterBase".lower() in i["name"].lower():
                logging.info("Detected voter file: " + i["name"])
                df_voters = pd.read_csv(i["obj"], sep="|", quotechar='"',
                                        quoting=3, error_bad_lines=False)
                df_voters.columns = self.config["ordered_columns"]
                df_voters['Registration_Number'] = df_voters[
                    'Registration_Number'].astype(str).str.zfill(8)
            elif "TXT" in i["name"]:
                vh_files.append(i)

        concat_history_file = concat_and_delete(vh_files)

        logging.info("Performing GA history manipulation")

        history = pd.read_csv(concat_history_file, sep="  ",
                              names=['Concat_str', 'Other'])

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
                        io_obj=StringIO(df_voters.to_csv()))

    def preprocess_nevada(self):
        new_files = self.unpack_files(self.main_file, compression='unzip')
        voter_file = new_files[0] if "ElgbVtr" in new_files[0]["name"] \
            else new_files[1]
        hist_file = new_files[0] if "VtHst" in new_files[0]["name"] else \
            new_files[1]

        df_hist = pd.read_csv(hist_file["obj"], header=None)
        df_hist.columns = self.config["hist_columns"]
        df_voters = pd.read_csv(voter_file["obj"], header=None)
        df_voters.columns = self.config["ordered_columns"]

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
                        io_obj=StringIO(df_voters.to_csv(index=False)))

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

        concat_voter_file = concat_and_delete(voter_files)
        concat_history_file = concat_and_delete(vote_history_files)
        gc.collect()

        logging.info("FLORIDA: loading voter history file")
        df_hist = pd.read_fwf(concat_history_file, header=None)
        df_hist.columns = self.config["hist_columns"]
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
        df_voters = pd.read_csv(concat_voter_file,
                                header=None, sep="\t")
        df_voters.columns = self.config["ordered_columns"]
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
                        io_obj=StringIO(df_voters.to_csv()))

    def preprocess_kansas(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression='unzip')
        for f in new_files:
            if (".txt" in f['name']) and ("._" not in f['name']) and \
               ("description" not in f['name'].lower()):
                logging.info("reading kansas file from {}".format(f['name']))
                df = pd.read_csv(f['obj'], sep="\t",
                                 index_col=False, engine='c',
                                 error_bad_lines=False)
        try:
            df.columns = self.config["ordered_columns"]
        except:
            df.columns = self.config["ordered_columns_new"]
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
            return(elect_year)

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
                        io_obj=StringIO(df.to_csv(encoding='utf-8',
                                                  index=False)))

    def preprocess_iowa(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression='unzip')
        logging.info("IOWA: reading in voter file")
        first_file = [f for f in new_files if "CD1" in f["name"] and
                      "Part1" in f["name"]][0]
        remaining_files = [f for f in new_files if "CD1" not in f["name"] or
                           "Part1" not in f["name"]]

        history_cols = self.config["election_columns"]
        main_cols = self.config['ordered_columns']
        buffer_cols = ["buffer0", "buffer1", "buffer2", "buffer3", "buffer4",
                       "buffer5", "buffer6", "buffer7", "buffer8", "buffer9"]
        total_cols = main_cols + history_cols + buffer_cols

        headers = pd.read_csv(first_file["obj"], nrows=1).columns
        headers = headers.tolist() + buffer_cols
        df_voters = pd.read_csv(first_file["obj"], skiprows=1, header=None,
                                names=headers)

        for i in remaining_files:
            skiprows = 1 if "Part1" in i["name"] else 0
            new_df = pd.read_csv(i["obj"], header=None, skiprows=skiprows,
                                 names=total_cols)
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
        wanted_cols = self.config["ordered_columns"] + \
            self.config["ordered_generated_columns"]
        df_voters = df_voters[wanted_cols]
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

        return FileItem(name="{}.processed".format(self.config["state"]),
                        io_obj=StringIO(df_voters.to_csv(encoding='utf-8',
                                                         index=False)))

    def preprocess_arizona(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip")
        new_files = [f for f in new_files if "LEGEND.xlsx" not in f["name"]]

        combined_file = self.concat_file_segments(new_files)

        main_df = pd.read_csv(combined_file)

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
                                                       index=False)))

    def preprocess_new_york(self):
        config = Config("new_york")
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="infer")
        self.main_file = list(filter(
            lambda x: x["name"][-4:] != ".pdf", new_files))[0]
        gc.collect()
        main_df = pd.read_csv(self.main_file["obj"],
                              header=None,
                              names=config["ordered_columns"],
                              encoding='latin-1')
        shutil.rmtree(os.path.dirname(self.main_file["name"]),
                      ignore_errors=True)
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
                                                       encoding='utf-8')))

    def preprocess_north_carolina(self):
        new_files = self.unpack_files(
            file_obj=self.main_file)  # array of dicts

        self.config = Config("north_carolina")
        for i in new_files:
            if ("ncvhis" in i['name']) and (".txt" in i['name']) and \
               ("MACOSX" not in i['name']):
                vote_hist_file = i
            elif ("ncvoter" in i['name']) and (".txt" in i['name']) and \
                 ("MACOSX" not in i['name']):
                voter_file = i
        voter_df = pd.read_csv(voter_file['obj'], sep="\t",
                               quotechar='"', encoding='latin-1')
        vote_hist = pd.read_csv(vote_hist_file['obj'], sep="\t",
                                quotechar='"')

        voter_df.columns = self.config["ordered_columns"]
        vote_hist.columns = self.config["hist_columns"]
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
                            index=True, encoding='utf-8')))

    def preprocess_missouri(self):
        new_files = self.unpack_files(
            file_obj=self.main_file, compression="unzip")
        preferred_files = [x for x in new_files
                           if ("VotersList" in x["name"]) and
                              (".txt" in x["name"])]
        if len(preferred_files) > 0:
            main_file = preferred_files[0]
        else:
            main_file = new_files[0]

        main_df = pd.read_csv(main_file["obj"], sep='\t')

        # convert "Voter Status" to "voter_status" for backward compatibility
        main_df.rename(columns={"Voter Status": self.config["voter_status"]},
                       inplace=True)

        # add empty column for party_identifier
        main_df[self.config["party_identifier"]] = np.nan

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
                                                       index=False)))

    def preprocess_michigan(self):
        config = Config('michigan')
        new_files = self.unpack_files(file_obj=self.main_file)
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
            vdf = pd.read_csv(voter_file['obj'],
                              encoding='latin-1',
                              na_filter=False,
                              error_bad_lines=False)
            # rename 'STATE' field to not conflict with our 'state' field
            vdf.rename(columns={'STATE': 'STATE_ADDR'}, inplace=True)
        else:
            raise NotImplementedError('File format not implemented')

        # TODO change to whatever reason code column is actually named
        reason_code_col = 'CANCELLATION_REASON'
        if reason_code_col in vdf.columns:
            vdf.rename(columns={reason_code_col: 'reason_code'}, inplace=True)

        def reconcile_columns(df, expected_cols):
            for c in expected_cols:
                if c not in df.columns:
                    df[c] = np.nan
            for c in df.columns:
                if c not in expected_cols:
                    df.drop(columns=[c], inplace=True)
            return df

        vdf = reconcile_columns(vdf, config['columns'])
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
            hdf = pd.read_csv(hist_file['obj'],
                              na_filter=False,
                              error_bad_lines=False)
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
                    edf = pd.read_csv(elec_codes['obj'],
                                      names=config['elec_code_columns'],
                                      na_filter=False)
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
                    date=this_date, state=self.state, testing=self.testing)
                if pre_key is not None:
                    nearest_meta = get_metadata_for_key(pre_key)
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
                                                   index=False)))

    def preprocess_pennsylvania(self):
        config = Config('pennsylvania')
        new_files = self.unpack_files(file_obj=self.main_file)
        voter_files = [f for f in new_files if "FVE" in f["name"]]
        election_maps = [f for f in new_files if "Election Map" in f["name"]]
        zone_codes = [f for f in new_files if "Codes" in f["name"]]
        zone_types = [f for f in new_files if "Types" in f["name"]]
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
            df = pd.read_csv(voter_file["obj"], sep='\t', names=dfcols)
            edf = pd.read_csv(election_map["obj"], sep='\t',
                              names=['county', 'number', 'title', 'date'])
            zdf = pd.read_csv(zones['obj'], sep='\t',
                              names=['county', 'number', 'code', 'title'])
            tdf = pd.read_csv(types['obj'], sep='\t',
                              names=['county', 'number', 'abbr', 'title'])
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
                    ', Type: ' + df["district_{}".format(i + 1)]\
                    .map(zdf.drop_duplicates('title').reset_index()
                         .set_index('title')['number'])\
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
                                                       index=False)))

    def preprocess_new_jersey(self):
        new_files = self.unpack_files(file_obj=self.main_file)
        config = Config("new_jersey")
        voter_files = [n for n in new_files if 'AlphaVoter' in n["name"]]

        hist_files = [n for n in new_files if 'History' in n["name"]]
        vdf = pd.DataFrame()
        hdf = pd.DataFrame()
        for f in voter_files:
            logging.info("Reading " + f["name"])
            new_df = pd.read_csv(f["obj"], sep='|',
                                 names=config['ordered_columns'],
                                 low_memory=False)
            new_df = self.config.coerce_dates(new_df)
            new_df = self.config.coerce_numeric(new_df, extra_cols=[
                "regional_school", "fire", "apt_no"])
            vdf = pd.concat([vdf, new_df], axis=0)
        for f in hist_files:
            logging.info("Reading " + f["name"])
            new_df = pd.read_csv(f["obj"], sep='|',
                                 names=config['hist_columns'],
                                 index_col=False,
                                 low_memory=False)
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
                                                   index=False)))

    def execute(self):
        return self.state_router()

    def state_router(self):
        routes = {
            'nevada': self.preprocess_nevada,
            'arizona': self.preprocess_arizona,
            'florida': self.preprocess_florida,
            'new_york': self.preprocess_new_york,
            'michigan': self.preprocess_michigan,
            'missouri': self.preprocess_missouri,
            'iowa': self.preprocess_iowa,
            'pennsylvania': self.preprocess_pennsylvania,
            'georgia': self.preprocess_georgia,
            'new_jersey': self.preprocess_new_jersey,
            'north_carolina': self.preprocess_north_carolina,
            'kansas': self.preprocess_kansas,
            'ohio': self.preprocess_ohio,
            'minnesota': self.preprocess_minnesota,
            'texas': self.preprocess_texas,
            'colorado': self.preprocess_colorado
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
