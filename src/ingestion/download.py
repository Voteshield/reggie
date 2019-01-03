import uuid
from datetime import datetime
from subprocess import Popen, PIPE

import bs4
import pandas as pd
import filetype
import requests
from dateutil import parser
import json
from constants import *
import zipfile
from configs.configs import Config
from storage import generate_s3_key, date_from_str, \
    df_to_postgres_array_string, strcol_to_array, get_surrounding_dates, \
    get_metadata_for_key, format_column_name
from storage import s3, normalize_columns
from xlrd.book import XLRDError
from pandas.io.parsers import ParserError
import shutil
import numpy as np
import subprocess
import sys
import gc


def ohio_get_last_updated():
    html = requests.get("https://www6.sos.state.oh.us/ords/f?p=VOTERFTP:STWD", verify=False).text
    soup = bs4.BeautifulSoup(html, "html.parser")
    results = soup.find_all("td", {"headers": "DATE_MODIFIED"})
    return max(parser.parse(a.text) for a in results)


def get_object(key, fn):
    with open(fn, "w+") as obj:
        s3.Bucket(S3_BUCKET).download_fileobj(Key=key, Fileobj=obj)


class Loader(object):
    """
    this object should be used to perform downloads directly from online resources which are specified by yaml files
    in the config directory.

    A Loader uses filesystem resources for temporarily storing the files on disk during the chunk concatenation process,
    therefore __enter__ and __exit__ are defined to allow safe usage of Loader in a 'with' statement:
    for example:
    ```
        with Loader() as l:
            l.download_chunks()
            l.s3_dump()
    ```
    """

    def __init__(self, config_file=CONFIG_OHIO_FILE, force_date=None,
                 force_file=None, clean_up_tmp_files=True, testing=False):
        self.config_file_path = config_file
        self.clean_up_tmp_files = clean_up_tmp_files
        config = Config(file_name=config_file)
        self.config = config
        self.chunk_urls = config[CONFIG_CHUNK_URLS] if CONFIG_CHUNK_URLS in config else []
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
            self.main_file = working_file
        else:
            self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())

        self.temp_files = [self.main_file]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.clean_up_tmp_files:
            self.clean_up()

    def clean_up(self):
        for fn in self.temp_files:
            if os.path.isfile(fn):
                try:
                    os.chmod(fn, 0777)
                    os.remove(fn)
                except OSError:
                    logging.warning("cannot remove {}".format(fn))
                    continue
            elif os.path.isdir(fn):
                shutil.rmtree(fn, ignore_errors=True)
        self.temp_files = []

    def download_src_chunks(self):
        """
        we expect each chunk to be a compressed (.gz) csv file
        :return:
        """
        self.obj_will_download = True
        if os.path.isfile(self.main_file):
            os.remove(self.main_file)
        first_success = False
        for i, url in enumerate(self.chunk_urls):
            logging.info("downloading chunk {} from {}".format(i, url))
            chunk_storage = "{}.{}.gz".format(self.main_file, str(i))
            with open(chunk_storage, "w+") as f:
                dl_proc = Popen(["curl", "--insecure", "-X", "GET", url],
                                stdout=f, stderr=PIPE)
                dl_proc.communicate()
                dl_proc.wait()

            p = Popen(["gunzip", chunk_storage], stdout=PIPE, stderr=PIPE)
            p.communicate()
            p.wait()
            decompressed_chunk = ".".join(chunk_storage.split(".")[:-1])
            try:
                df = pd.read_csv(decompressed_chunk)
                s = df.to_csv(header=not first_success)
                first_success = True
                logging.info("done with chunk {}".format(i))
            except ValueError:
                logging.warning("malformed response from {}".format(url))
                continue

            with open(self.main_file, 'a+') as f:
                f.write(s)
            self.temp_files.append(decompressed_chunk)

        self.compress()
        self.compute_checksum()
        self.download_date = datetime.now().isoformat()
        return self.checksum

    def compute_checksum(self):
        logging.info("calculating checksum")
        p = Popen(["cksum", self.main_file], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        self.checksum = out
        return self.checksum

    def compress(self, compression_type="gzip"):
        """
        intended to be called after the consolidated (processed) file has been
        created and saved in self.main_file
        :param compression_type: gzip is default
        :return: None
        """
        if not self.is_compressed:
            logging.info("compressing")
            p = Popen([compression_type, self.main_file], stdout=PIPE,
                      stderr=PIPE)
            p.communicate()
            if self.main_file[-3:] != ".gz":
                self.main_file += ".gz"
            self.is_compressed = True
            self.temp_files.append(self.main_file)

    def unzip_decompress(self, file_name):
        """
        handles decompression for .zip files
        :param file_name: .zip file
        :return: name of new directory containing contents of file_name
        """
        new_loc = "{}_decompressed".format(os.path.abspath(file_name))
        logging.info("decompressing unzip {} to {}".format(file_name,
                                                           new_loc))
        os.mkdir(new_loc)
        r = subprocess.call(['unzip', file_name, '-d', new_loc])
        if r == 1:
            r = 0
        return new_loc, r

    def gunzip_decompress(self, file_name):
        """
        handles decompression for .gz files
        :param file_name: .gz file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        logging.info("decompressing {} {} to {}"
                     .format("gunzip",
                             file_name,
                             os.path.dirname(file_name)))

        p = Popen(["gunzip", file_name], stdout=PIPE,
                  stderr=PIPE, stdin=PIPE)
        p.communicate()
        if file_name[-3:] == ".gz":
            new_loc = file_name[:-3]
        else:
            new_loc = file_name
        return new_loc, p.returncode

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

        p = Popen(["bunzip2", file_name], stdout=PIPE,
                  stderr=PIPE, stdin=PIPE)
        p.communicate()
        if file_name[-4:] == ".bz2":
            new_loc = file_name[:-4]
        else:
            new_loc = file_name + ".out"
        return new_loc, p.returncode

    def sevenzip_decompress(self, file_name):
        """
        handles decompression for 7zip files
        :param file_name: 7zip compressed file
        :return: tuple containing (name of new decompressed file if
        successful, and a reference to the subprocess object which ran the
        decompression)
        """
        logging.info("decompressing {} {} to {}"
                     .format("7zip",
                             file_name,
                             os.path.dirname(file_name)))
        if file_name[-4:] == ".zip":
            new_loc = file_name[:-4] + ".out"
        else:
            new_loc = file_name + ".out"
        p = Popen(["7z", "e", "-so", file_name],
                  stdout=open(new_loc, "w+"), stderr=PIPE, stdin=PIPE)
        p.communicate()
        return new_loc, p.returncode

    def infer_compression(self, file_name):
        """
        infer file type and map to compression type
        :param file_name: file in question
        :return: string (de)compression type or None
        """
        guess = filetype.guess(file_name)
        compression_type = None
        if guess is not None:
            options = {"application/x-bzip2": "bunzip2",
                       "application/gzip": "gunzip",
                       "application/zip": "unzip"}
            compression_type = options.get(guess.mime, None)
            if compression_type is None:
                logging.info("unsupported file format: {}".format(guess.mime))
        else:
            logging.info(
                "could not infer the file type of {}".format(file_name))
        return compression_type

    def decompress(self, file_name, compression_type="gunzip"):
        """
        decompress a file using either unzip or gunzip, unless the file is an
        .xlsx file, in which case it is returned as is (these files are
        compressed by default, and are unreadable in their unpacked form by
        pandas)
        :param file_name: the path of the file to be decompressed
        :param compression_type: available options - ["unzip", "gunzip"]
        :return: a (str, bool) tuple containing the location of the processed
        file and whether or not it was actually
        decompressed
        """

        logging.info("decompressing {}".format(file_name))
        new_loc = "{}_decompressed".format(os.path.abspath(file_name))
        success = False

        if compression_type is "infer":
            compression_type = self.infer_compression(file_name)

        if file_name.split(".")[-1] == "xlsx":
            logging.info("did not decompress {}".format(file_name))
            shutil.rmtree(new_loc, ignore_errors=True)
            new_loc = file_name
            success = True
        else:
            if compression_type == "unzip":
                new_loc, p = self.unzip_decompress(file_name)
            elif compression_type == "bunzip2":
                new_loc, p = self.bunzip2_decompress(file_name)
            elif compression_type == "7zip":
                new_loc, p = self.sevenzip_decompress(file_name)
            else:
                new_loc, p = self.gunzip_decompress(file_name)

            if compression_type is not None and p == 0:
                logging.info("decompression done: {}".format(file_name))
                self.temp_files.append(new_loc)
                success = True
            else:
                logging.info("did not decompress {}".format(file_name))
                shutil.rmtree(new_loc, ignore_errors=True)
                new_loc = file_name

        self.is_compressed = False
        return new_loc, success

    def generate_key(self, file_class=PROCESSED_FILE_PREFIX):
        k = generate_s3_key(file_class, self.state, self.source,
                            self.download_date, "csv", "gz")
        return "testing/" + k if self.testing else k

    def s3_dump(self, file_class=PROCESSED_FILE_PREFIX):
        if self.config["state"] == 'ohio' and self.obj_will_download:
            self.download_date = ohio_get_last_updated().isoformat()
        meta = self.meta if self.meta is not None else {}
        meta["last_updated"] = self.download_date
        with open(self.main_file) as f:
            s3.Object(S3_BUCKET, self.generate_key(file_class=file_class))\
                .put(Body=f)
        s3.Object(S3_BUCKET,
                  self.generate_key(file_class=META_FILE_PREFIX) + ".json")\
            .put(Body=json.dumps(meta))


class Preprocessor(Loader):
    def __init__(self, raw_s3_file, config_file, **kwargs):

        super(Preprocessor, self).__init__(
            config_file=config_file, force_date=date_from_str(raw_s3_file),
            **kwargs)
        self.raw_s3_file = raw_s3_file

        if self.raw_s3_file is not None:
            self.s3_download()

    def s3_download(self):
        self.main_file = "/tmp/voteshield_{}" \
            .format(self.raw_s3_file.split("/")[-1])

        get_object(self.raw_s3_file, self.main_file)
        self.temp_files.append(self.main_file)

    def unpack_files(self, compression="unzip"):
        all_files = []

        def expand_recurse(file_name):

            if os.path.isdir(file_name):
                for f in os.listdir(file_name):
                    d = file_name + "/" + f
                    expand_recurse(d)
            else:
                decompressed_result, success = self.decompress(
                    file_name, compression_type=compression)

                if os.path.isdir(decompressed_result):
                    # is dir
                    for f in os.listdir(decompressed_result):
                        d = decompressed_result + "/" + f
                        expand_recurse(d)

                else:
                    # was file
                    all_files.append(decompressed_result)

        expand_recurse(self.main_file)

        if "format" in self.config and "ignore_files" in self.config["format"]:
            all_files = [n for n in all_files if n not in
                         self.config["format"]["ignore_files"]
                         and os.path.basename(n) not in
                         self.config["format"]["ignore_files"]]
        else:
            all_files = [n for n in all_files]
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
        if os.path.isfile(self.main_file):
            os.remove(self.main_file)
        self.main_file = ".".join(self.main_file.split(".")[:-1]) + '.concat'
        first_success = False
        last_headers = None

        def list_compare(a, b):
            i = 0
            for j, k in zip(a, b):
                if j != k:
                    return j, k, i
                i += 1
            return False

        file_names = sorted(file_names, key=lambda x: os.stat(x).st_size,
                            reverse=True)
        for f in file_names:
            try:
                if self.config["file_type"] == 'xlsx':
                    df = pd.read_excel(f)
                else:
                    df = pd.read_csv(f)
            except (XLRDError, ParserError):
                print("Skipping {} ... Unsupported format, or corrupt file"
                      .format(f))
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
            with open(self.main_file, "a+") as fo:
                fo.write(s)

        os.rename(self.main_file, self.main_file.split('.')[0] + '.csv')
        self.main_file = self.main_file.split('.')[0] + '.csv'

    def preprocess_georgia(self):
        config = Config("georgia")
        logging.info("GEORGIA: loading voter and voter history file")
        new_files = self.unpack_files(compression = 'unzip')
        vh_files = []
        for i in new_files:
            if "/Georgia_Daily_VoterBase" in i:
                logging.info("Detected voter file: " + i)
                df_voters = pd.read_csv(i, sep="|", quotechar='"', quoting=3,
                                        error_bad_lines=False)
                df_voters.columns = self.config["ordered_columns"]
                df_voters['Registration_Number'] = df_voters['Registration_Number'].astype(str).str.zfill(8)
                os.remove(i)
            elif "TXT" in i:
                vh_files.append(i)

        def concat_and_delete(in_list, concat_file):
            with open(concat_file, 'w') as outfile:
                for fname in in_list:
                    with open(fname) as infile:
                        outfile.write(infile.read())
                    os.remove(fname)
            return concat_file
        concat_history_file = concat_and_delete(
                vh_files, '/tmp/concat_history_file.txt')

        logging.info("Performing GA history manipulation")

        history = pd.read_csv(concat_history_file, sep = "  ", names = ['Concat_str', 'Other'])
        os.remove(concat_history_file)

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
        sorted_codes_dict = {k: {"index": i, "count": counts[i],
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
        self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
        df_voters.to_csv(self.main_file)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum


    def preprocess_nevada(self):
        new_files = self.unpack_files(compression='unzip')
        voter_file = new_files[0] if "ElgbVtr" in new_files[0] \
            else new_files[1]
        hist_file = new_files[0] if "VtHst" in new_files[0] else new_files[1]
        self.temp_files.extend([hist_file, voter_file])
        logging.info("NEVADA: loading historical file")
        df_hist = pd.read_csv(hist_file, header=None)
        df_hist.columns = self.config["hist_columns"]
        logging.info("NEVADA: loading main voter file")
        df_voters = pd.read_csv(voter_file, header=None)
        df_voters.columns = self.config["ordered_columns"]
        valid_elections = df_hist.date.unique().tolist()
        valid_elections.sort(key=lambda x: datetime.strptime(x, "%m/%d/%Y"))

        # NOTE: this function only works correctly if
        # df_hist is assumed to be sorted by date
        def place_vote_hist(g):
            group_idx = 0
            output = []
            for d in valid_elections[::-1]:
                if group_idx < len(g.date.values) and \
                        d == g.date.values[group_idx]:
                    output.append(g.vote_code.values[group_idx])
                    group_idx += 1
                else:
                    output.append('n')

            return output

        voting_histories = df_hist.groupby(self.config["voter_id"])\
            .apply(place_vote_hist)
        df_voters["tmp_id"] = df_voters[self.config["voter_id"]]
        df_voters = df_voters.set_index("tmp_id")
        df_voters["all_history"] = voting_histories
        self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(df_voters)
        df_voters.to_csv(self.main_file, index=False)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_florida(self):
        logging.info("preprocessing florida")
        new_files = self.unpack_files(compression='unzip')

        vote_history_files = []
        voter_files = []
        for i in new_files:
            if "_H_" in i:
                vote_history_files.append(i)
            elif ".txt" in i:
                voter_files.append(i)

        def concat_and_delete(in_list, concat_file):
            with open(concat_file, 'w') as outfile:
                for fname in in_list:
                    with open(fname) as infile:
                        outfile.write(infile.read())
                    os.remove(fname)
            return concat_file

        concat_voter_file = concat_and_delete(
            voter_files, '/tmp/concat_voter_file.txt')
        concat_history_file = concat_and_delete(
            vote_history_files, '/tmp/concat_voter_history.txt')

        logging.info("FLORIDA: loading voter history file")
        df_hist = pd.read_fwf(concat_history_file, header=None)
        df_hist.columns = self.config["hist_columns"]

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
        sorted_codes_dict = {k: {"index": i, "count": counts[i],
                                 "date": date_from_str(k)}
                             for i, k in enumerate(sorted_codes)}

        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))

        logging.info("FLORIDA: history apply")
        voter_groups = df_hist.groupby("VoterID")
        all_history = voter_groups["array_position"].apply(list)
        vote_type = voter_groups["vote_type"].apply(list)

        logging.info("FLORIDA: loading main voter file")
        df_voters = pd.read_csv(concat_voter_file,
                                header=None, sep="\t")
        df_voters.columns = self.config["ordered_columns"]
        df_voters = df_voters.set_index(self.config["voter_id"])

        df_voters["all_history"] = all_history
        df_voters["vote_type"] = vote_type

        df_voters = self.config.coerce_strings(df_voters)
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(df_voters, extra_cols=[
            "Precinct", "Precinct_Split", "Daytime_Phone_Number",
            "Daytime_Area_Code", "Daytime_Phone_Extension",
            "Mailing_Zipcode", "Residence_Zipcode",
            "Mailing_Address_Line_1", "Mailing_Address_Line_2",
            "Mailing_Address_Line_3", "Residence_Address_Line_1",
            "Residence_Address_Line_2"])

        self.meta = {
            "message": "florida_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        logging.info("FLORIDA: writing out")
        os.remove(concat_voter_file)
        os.remove(concat_history_file)
        self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
        df_voters.to_csv(self.main_file)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_iowa(self):
        new_files = self.unpack_files(compression='unzip')
        logging.info("IOWA: reading in voter file")
        first_file = [f for f in new_files if "CD1" in f and "Part1" in f][0]
        remaining_files = [f for f in new_files if "CD1" not in f or
                           "Part1" not in f]

        history_cols = self.config["election_columns"]
        main_cols = self.config['ordered_columns']
        buffer_cols = ["buffer0", "buffer1", "buffer2", "buffer3", "buffer4"]
        total_cols = main_cols + history_cols + buffer_cols
        df_voters = pd.read_csv(first_file, skiprows=1, header=None,
                                names=total_cols)

        for i in remaining_files:
            skiprows = 1 if "Part1" in i else 0
            new_df = pd.read_csv(i, header=None, skiprows=skiprows,
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

            df_voters[c] = df_voters[c].str.replace(prefix + key_delim * 3,
                                                    '')
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
        sorted_codes_dict = {j: {"index": i, "count": counts[i],
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

        df_voters.drop(columns=history_cols, inplace=True)
        for c in df_voters.columns:
            df_voters[c].loc[df_voters[c].isnull()] = ""
        df_voters = self.config.coerce_dates(df_voters)
        df_voters = self.config.coerce_numeric(df_voters, extra_cols=[
            "COMMUNITY_COLLEGE", "COMMUNITY_COLLEGE_DIRECTOR",
            "LOSST_CONTIGUOUS_CITIES", "PRECINCT", "SANITARY",
            "SCHOOL_DIRECTOR", "UNIT_NUM"])
        pd.set_option('max_columns', 200)
        pd.set_option('max_row', 6)

        df_voters.to_csv(self.main_file, index=False, compression="gzip")
        self.is_compressed = True
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_arizona(self):
        new_files = self.unpack_files(compression="unzip")
        new_files = [f for f in new_files if "LEGEND.xlsx" not in f]

        self.concat_file_segments(new_files)
        for f in new_files:
            logging.info("New file: " + f)
            os.remove(f)
        main_df = pd.read_csv(self.main_file)

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
            main_df[self.config["birthday_identifier"]],
            format=self.config["date_format"],
            errors='coerce')
        elections_key = [c.split("_")[-1] for c in voting_action_cols]

        main_df.drop(all_voting_history_cols, axis=1, inplace=True)
        main_df = self.config.coerce_numeric(main_df, extra_cols=[
            "text_mail_zip5", "text_mail_zip4", "text_phone_last_four",
            "text_phone_exchange", "text_phone_area_code",
            "precinct_part_text_name", "precinct_part",
            "occupation", "text_mail_carrier_rte",
            "text_res_address_nbr", "text_res_address_nbr_suffix",
            "text_res_unit_nbr", "text_res_carrier_rte",
            "text_mail_address1", "text_mail_address2", "text_mail_address3",
            "text_mail_address4"])
        main_df.to_csv(self.main_file, encoding='utf-8', index=False)
        self.meta = {
            "message": "arizona_{}".format(datetime.now().isoformat()),
            "array_dates": json.dumps(elections_key)
        }
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_new_york(self):
        config = Config("new_york")
        new_files = self.unpack_files(compression="infer")
        main_file = filter(lambda x: x[-4:] != ".pdf", new_files)[0]
        gc.collect()
        main_df = pd.read_csv(main_file,
                              header=None,
                              names=config["ordered_columns"])
        null_hists = main_df.voterhistory != main_df.voterhistory
        main_df.voterhistory[null_hists] = NULL_CHAR
        all_codes = main_df.voterhistory.str.replace(" ", "_") \
            .str.replace("[", "") \
            .str.replace("]", "")
        all_codes = all_codes.str.cat(sep=";")
        all_codes = np.array(all_codes.split(";"))
        main_df["all_history"] = strcol_to_array(main_df.voterhistory,
                                                 delim=";")
        unique_codes, counts = np.unique(all_codes, return_counts=True)

        count_order = counts.argsort()
        unique_codes = unique_codes[count_order]
        counts = counts[count_order]

        sorted_codes = unique_codes.tolist()
        sorted_codes_dict = {k: {"index": i, "count": counts[i]} for i, k in
                             enumerate(sorted_codes)}

        def insert_code_bin(arr):
            return [sorted_codes_dict[k]["index"] for k in arr]

        # in this case we save ny as sparse array since so many elections are
        # stored
        main_df.all_history = main_df.all_history.apply(insert_code_bin)
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
        os.remove(self.main_file)
        self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
        main_df.to_csv(self.main_file, index=False, compression="gzip",
                       encoding='utf-8')
        self.is_compressed = True
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_missouri(self):
        new_file = self.unpack_files(compression="7zip")
        new_file = new_file[0]
        main_df = pd.read_csv(new_file, sep='\t')

        # add empty columns for voter_status and party_identifier
        main_df[self.config["voter_status"]] = np.nan
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
            sorted_codes_dict = {k: {"index": i, "count": counts[i],
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
        main_df.to_csv(self.main_file, encoding='utf-8', index=False)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_michigan(self):
        config = Config("michigan")
        new_files = self.unpack_files()
        voter_file = ([n for n in new_files if 'entire_state_v' in n] + [None])[0]
        hist_file = ([n for n in new_files if 'entire_state_h' in n
                      or 'EntireStateVoterHistory' in n] + [None])[0]
        elec_codes = ([n for n in new_files if 'electionscd' in n] + [None])[0]
        logging.info("Detected voter file: " + voter_file)
        logging.info("Detected history file: " + hist_file)
        if(elec_codes):
            logging.info("Detected election code file: " + elec_codes)

        if voter_file[-3:] == "lst":
            vcolspecs = [[0, 35], [35, 55], [55, 75], [75, 78], [78, 82], [82, 83],
                         [83, 91], [91, 92], [92, 99], [99, 103], [103, 105],
                         [105, 135], [135, 141], [141, 143], [143, 156],
                         [156, 191], [191, 193], [193, 198], [198, 248],
                         [248, 298], [298, 348], [348, 398], [398, 448],
                         [448, 461], [461, 463], [463, 468], [468, 474],
                         [474, 479], [479, 484], [484, 489], [489, 494],
                         [494, 499], [499, 504], [504, 510], [510, 516],
                         [516, 517], [517, 519]]
            logging.info("MICHIGAN: Loading voter file")
            vdf = pd.read_fwf(voter_file, colspecs=vcolspecs,
                              names=config["ordered_columns"], na_filter=False)
            logging.info("Removing voter file")
            os.remove(voter_file)
        elif voter_file[-3:] == "csv":
            logging.info("MICHIGAN: Loading voter file")
            vdf = pd.read_csv(voter_file, na_filter=False,
                              error_bad_lines=False)\
                .drop(["COUNTY_NAME", "JURISDICTION_NAME",
                       "SCHOOL_DISTRICT_NAME", "STATE_HOUSE_DISTRICT_NAME",
                       "STATE_SENATE_DISTRICT_NAME",
                       "US_CONGRESS_DISTRICT_NAME",
                       "COUNTY_COMMISSIONER_DISTRICT_NAME",
                       "VILLAGE_DISTRICT_NAME", "UOCAVA_STATUS_NAME"], axis=1)
            vdf.columns = config["ordered_columns"]
            logging.info("Removing voter file")
            os.remove(voter_file)
        else:
            raise NotImplementedError("File format not implemented. Contact "
                                      "your local code monkey")
        if hist_file[-3:] == "lst":
            hcolspecs = [[0, 13], [13, 15], [15, 20], [20, 25], [25, 38], [38, 39]]
            logging.info("MICHIGAN: Loading historical file")
            hdf = pd.read_fwf(hist_file, colspecs=hcolspecs,
                              names=config["hist_columns"], na_filter=False)
            logging.info("Removing historical file")
            os.remove(hist_file)
        elif hist_file[-3:]:
            logging.info("MICHIGAN: Loading historical file")
            hdf = pd.read_csv(hist_file, na_filter=False,
                              error_bad_lines=False)\
                .drop(["COUNTY_NAME", "JURISDICTION_NAME",
                       "SCHOOL_DISTRICT_NAME"], axis=1)
            hdf.columns = config["hist_columns"]
            logging.info("Removing historical file")
            os.remove(hist_file)
        else:
            raise NotImplementedError("File format not implemented. Contact "
                                      "your local code monkey")

        if elec_codes:
            if elec_codes[-3:] == "lst":
                ecolspecs = [[0, 13], [13, 21], [21, 46]]
                edf = pd.read_fwf(elec_codes, colspecs=ecolspecs,
                                  names=config["elec_code_columns"],
                                  na_filter=False)
            elif elec_codes[-3:] == "csv":
                edf = pd.read_csv(elec_codes,
                                  names=config["elec_code_columns"],
                                  na_filter=False)
            else:
                raise NotImplementedError("File format not implemented. "
                                          "Contact your local code monkey")

            edf["Date"] = edf["Date"].apply(
                lambda x: pd.to_datetime(x, format='%m%d%Y')
            )
            edf.sort_values(by=["Date"])
            edf["Date"] = edf["Date"].apply(datetime.isoformat)
            sorted_codes = map(str, edf["Election_Code"].unique().tolist())
            edf["Election_Code"] = edf["Election_Code"].astype(str)

            edf = edf.set_index("Election_Code")
            edf["Title"] += '_'
            edf["Title"] = edf["Title"] + edf["Date"].map(str)
            counts = hdf["Election_Code"].value_counts()
            counts.index = counts.index.map(str)
            elec_dict = {
                k: {'index': i, 'count': counts.loc[k] if k in counts else 0,
                'date': edf.loc[k]["Date"], 'title': edf.loc[k]["Title"]}
                for i, k in enumerate(sorted_codes)}
        else:
            this_date = parser.parse(date_from_str(self.raw_s3_file)).date()
            pre_date, post_date, pre_key, post_key = get_surrounding_dates(
                date=this_date, state=self.state, testing=self.testing)
            old_meta = get_metadata_for_key(pre_key)
            sorted_codes = old_meta["array_decoding"]
            elec_dict = old_meta["array_encoding"]

        vdf = self.config.coerce_dates(vdf)
        vdf = self.config.coerce_numeric(vdf, extra_cols=["School_Precinct"])
        vdf = self.config.coerce_strings(vdf)

        hdf["Info"] = hdf["Election_Code"].map(str) + '_' + \
                      hdf["Absentee_Voter_Indicator"].map(str) + '_' + \
                      hdf['county_number'].map(str) + '_' + \
                      hdf["Jurisdiction"].map(str) + '_' + \
                      hdf["School_Code"].map(str)

        def get_sparse_history(group):
            sparse = []
            for ecode in group["Election_Code"].values:
                try:
                    sparse.append(elec_dict[str(ecode)]['index'])
                except KeyError:
                    continue

            return sparse

        def get_all_history(group):
            all_hist = []
            for ecode in group["Election_Code"].values:
                try:
                    all_hist.append(elec_dict[str(ecode)]["title"])
                except KeyError:
                    continue

            return all_hist

        def get_coded_history(group):
            coded = []
            for ecode in group["Election_Code"].values:
                coded.append(str(ecode))

            return coded

        vdf['tmp_id'] = vdf[self.config["voter_id"]]
        vdf = vdf.set_index('tmp_id')
        logging.info("Generating sparse history")
        group = hdf.groupby(config['voter_id'])
        vdf["sparse_history"] = group.apply(get_sparse_history)
        logging.info("Generating all history")
        vdf["all_history"] = group.apply(get_all_history)
        logging.info("Generating verbose history")
        vdf["verbose_history"] = group.apply(lambda x: x['Info'].values)
        logging.info("Generating coded history")
        vdf["coded_history"] = group.apply(get_coded_history)
        vdf[config["voter_id"]] = vdf[config["voter_id"]]\
            .astype(int, errors='ignore')
        vdf["party_identifier"] = "npa"
        vdf.fillna('')

        # get rid of stupid latin-1
        text_fields = [c for c, v in config["columns"].items()
                       if v == "text" or v == "varchar"]
        for field in text_fields:
            if (field in vdf) and (field != config["voter_status"]) \
                    and (field != config["party_identifier"]):
                vdf[field] = vdf[field].str.decode("latin-1")

        logging.info("Writing to csv")
        vdf.to_csv(self.main_file, encoding='utf-8', index=False)
        self.meta = {
            "message": "michigan_{}".format(datetime.now().isoformat()),
            "array_decoding": sorted_codes,
            "array_encoding": elec_dict
        }
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()

        return chksum

    def preprocess_pennsylvania(self):
        config = Config('pennsylvania')
        new_files = self.unpack_files()
        voter_files = [f for f in new_files if "FVE" in f]
        election_maps = [f for f in new_files if "Election Map" in f]
        zone_codes = [f for f in new_files if "Codes" in f]
        zone_types = [f for f in new_files if "Types" in f]
        counties = config["county_names"]
        main_df = None
        elections = 40
        dfcols = config["ordered_columns"][:-3]
        for i in range(elections):
            dfcols.extend(["district_{}".format(i+1)])
        for i in range(elections):
            dfcols.extend(["election_{}_vote_method".format(i + 1)])
            dfcols.extend(["election_{}_party".format(i+1)])
        dfcols.extend(config["ordered_columns"][-3:])

        for c in counties:
            logging.info("Processing {}".format(c))
            c = format_column_name(c)
            try:
                voter_file = next(f for f in voter_files if c in f.lower())
                election_map = next(f for f in election_maps if c in f.lower())
                zones = next(f for f in zone_codes if c in f.lower())
                types = next(f for f in zone_types if c in f.lower())
            except StopIteration:
                continue
            df = pd.read_csv(voter_file, sep='\t', names=dfcols)
            edf = pd.read_csv(election_map, sep='\t',
                              names=['county', 'number', 'title', 'date'])
            zdf = pd.read_csv(zones, sep='\t',
                              names=['county', 'number', 'code', 'title'])
            tdf = pd.read_csv(types, sep='\t',
                              names=['county', 'number', 'abbr', 'title'])
            df = df.replace('"')
            edf = edf.replace('"')
            zdf = zdf.replace('"')
            edf.index = edf["number"]
            os.remove(voter_file)
            os.remove(election_map)
            os.remove(zones)
            os.remove(types)
            
            for i in range(elections):
                s = pd.Series(index=df.index)
                # Blair isn't sending all their election codes
                try:
                    s[:] = edf.iloc[i]["title"] + ' ' + edf.iloc[i]["date"]+' '
                except IndexError:
                    s[:] = "UNSPECIFIED"
                df["election_{}".format(i)] = s + \
                                              df["election_{}_vote_method"
                                                  .format(i + 1)].apply(str) + ' ' + \
                                              df["election_{}_party"
                                                  .format(i + 1)]
                df.loc[df["election_{}_vote_method".format(i + 1)].isna(),
                       "election_{}".format(i)] = pd.np.nan
                df = df.drop("election_{}_vote_method".format(i + 1), axis=1)
                df = df.drop("election_{}_party".format(i + 1), axis=1)

                df["district_{}".format(i+1)] = df["district_{}".format(i+1)]\
                    .map(zdf.drop_duplicates('code').reset_index()
                         .set_index('code')['title'])
                df["district_{}".format(i+1)] += \
                    ', Type: ' + df["district_{}".format(i+1)]\
                    .map(zdf.drop_duplicates('title').reset_index()
                         .set_index('title')['number'])\
                    .map(tdf.set_index('number')['title'])

            df["all_history"] = df[["election_{}".format(i) for i in range(elections)]]\
                .values.tolist()
            df["all_history"] = df["all_history"].map(
                lambda L: list(filter(pd.notna, L)))
            df["districts"] = df[["district_{}".format(i+1) for i in range(elections)]]\
                .values.tolist()
            df["districts"] = df["districts"].map(
                lambda L: list(filter(pd.notna, L)))

            for i in range(elections):
                df = df.drop("election_{}".format(i), axis=1)
                df = df.drop("district_{}".format(i+1), axis=1)

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
        main_df.to_csv(self.main_file, encoding='utf-8', index=False)
        self.meta = {
            "message": "pennsylvania_{}".format(datetime.now().isoformat()),
        }
        self.temp_files.append(self.main_file)

        chksum = self.compute_checksum()

        return chksum

    def preprocess_new_jersey(self):
        new_files = self.unpack_files()
        config = Config("new_jersey")
        voter_files = [n for n in new_files if 'AlphaVoter' in n]
        hist_files = [n for n in new_files if 'History' in n]
        vdf = pd.DataFrame()
        hdf = pd.DataFrame()
        for f in voter_files:
            logging.info("Reading " + f)
            new_df = pd.read_csv(f, sep='|', names=config['ordered_columns'],
                                 low_memory=False)
            new_df = self.config.coerce_dates(new_df)
            new_df = self.config.coerce_numeric(new_df, extra_cols=[
                "regional_school", "fire", "apt_no"])
            vdf = pd.concat([vdf, new_df], axis=0)
        for f in hist_files:
            logging.info("Reading " + f)
            new_df = pd.read_csv(f, sep='|',
                                  names=config['hist_columns'],
                                  index_col=False,
                                 low_memory=False)
            new_df = self.config.coerce_numeric(new_df, col_list='hist_columns_type')
            hdf = pd.concat([hdf, new_df], axis=0)
        for f in voter_files:
            os.remove(f)
        for f in hist_files:
            os.remove(f)

        hdf['election_name'] = hdf['election_name'] + ' ' + \
                               hdf['election_date']
        hdf = self.config.coerce_dates(hdf, col_list='hist_columns_type')
        hdf.sort_values('election_date', inplace=True)
        hdf = hdf.dropna(subset=['election_name'])
        hdf = hdf.reset_index()
        elections = hdf["election_name"].unique().tolist()
        counts = hdf["election_name"].value_counts()
        elec_dict = {
            k: {'index': i, 'count': counts.loc[k] if k in counts else 0}
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
        vdf['all_history'] = hdf_id_group['election_code'].apply(list)
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
        vdf.to_csv(self.main_file, encoding='utf-8', index=False)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()

        return chksum

    def execute(self):
        self.state_router()

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
        }
        if self.config["state"] in routes:
            f = routes[self.config["state"]]
            logging.info("preprocessing {}".format(self.config["state"]))
            f()
        else:
            raise NotImplementedError("preprocess_{} has not yet been "
                                      "implemented for the Preprocessor object"
                                      .format(self.config["state"]))


if __name__ == '__main__':
    print(ohio_get_last_updated())
