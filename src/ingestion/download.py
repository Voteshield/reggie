import uuid
from datetime import datetime
from subprocess import Popen, PIPE

import bs4
import pandas as pd
import re
import sys
import requests
from dateutil import parser
import json
from constants import *
from storage import generate_s3_key, date_from_str, load_configs_from_file, df_to_postgres_array_string, \
    strcol_to_postgres_array_str, strcol_to_array, listcol_tonumpy
from storage import s3, normalize_columns, describe_insertion, write_meta, read_meta
import zipfile
from xlrd.book import XLRDError
from pandas.io.parsers import ParserError
import shutil
import numpy as np


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

    def __init__(self, config_file=CONFIG_OHIO_FILE, force_date=None, force_file=None, clean_up_tmp_files=True,
                 testing=False):
        self.config_file_path = config_file
        self.clean_up_tmp_files = clean_up_tmp_files
        config = load_configs_from_file(config_file=config_file)
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
                os.remove(fn)
            else:
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
                dl_proc = Popen(["curl", "--insecure", "-X", "GET", url], stdout=f, stderr=PIPE)
                dl_proc.communicate()
                dl_proc.wait()

            p = Popen(["gunzip", chunk_storage], stdout=PIPE, stderr=PIPE)
            p.communicate()
            p.wait()
            decompressed_chunk = ".".join(chunk_storage.split(".")[:-1])
            try:
                df = pd.read_csv(decompressed_chunk, comment="#")
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
        intended to be called after the consolidated (processed) file has been created and saved in self.main_file
        :param compression_type: gzip is default
        :return: None
        """
        if not self.is_compressed:
            logging.info("compressing")
            p = Popen([compression_type, self.main_file], stdout=PIPE, stderr=PIPE)
            p.communicate()
            if self.main_file[-3:] != ".gz":
                self.main_file += ".gz"
            self.is_compressed = True

    def decompress(self, file_name, compression_type="unzip"):
        logging.info("decompressing {}".format(file_name))
        new_loc = "{}_decompressed".format(os.path.abspath(file_name))
        if compression_type == "unzip":
            logging.info("decompressing unzip {} to {}".format(file_name, new_loc))
            os.mkdir(new_loc)
            p = Popen([compression_type, file_name, "-d", new_loc],
                      stdout=PIPE, stderr=PIPE, stdin=PIPE)
            p.communicate("A")
        else:
            logging.info("decompressing gunzip {} to {}".format(file_name, os.path.dirname(file_name)))
            p = Popen([compression_type, file_name], stdout=PIPE, stderr=PIPE, stdin=PIPE)
            p.communicate()
            if file_name[-3:] == ".gz":
                new_loc = file_name[:-3]
            else:
                new_loc = file_name
        if p.returncode == 0:
            logging.info("decompressing done: {}".format(file_name))
            self.temp_files.append(new_loc)
        else:
            logging.info("did not decompress {}".format(file_name))
            shutil.rmtree(new_loc, ignore_errors=True)
            new_loc = file_name

        self.is_compressed = False
        return new_loc, p.returncode == 0
    def decompress(self, file_name, compression_type="gunzip"):
        logging.info("decompressing {}".format(file_name))
        new_loc = "{}_decompressed".format(os.path.abspath(file_name))
        if compression_type == "unzip":
            logging.info("decompressing unzip {} to {}".format(file_name, new_loc))
            os.mkdir(new_loc)
            p = Popen([compression_type, file_name, "-d", new_loc],
                      stdout=PIPE, stderr=PIPE, stdin=PIPE)
            p.communicate("A")
        else:
            logging.info("decompressing gunzip {} to {}".format(file_name, os.path.dirname(file_name)))
            p = Popen([compression_type, file_name], stdout=PIPE, stderr=PIPE, stdin=PIPE)
            p.communicate()
            if file_name[-3:] == ".gz":
                new_loc = file_name[:-3]
            else:
                new_loc = file_name
        if p.returncode == 0:
            logging.info("decompressing done: {}".format(file_name))
            self.temp_files.append(new_loc)
        else:
            logging.info("did not decompress {}".format(file_name))
            shutil.rmtree(new_loc, ignore_errors=True)
            new_loc = file_name

        self.is_compressed = False
        return new_loc, p.returncode == 0

    def generate_key(self, file_class=PROCESSED_FILE_PREFIX):
        k = generate_s3_key(file_class, self.state, self.source,
                            self.download_date, "csv", self.is_compressed)
        return "testing/" + k if self.testing else k

    def s3_dump(self, file_class=PROCESSED_FILE_PREFIX):
        if self.config["state"] == 'ohio' and self.obj_will_download:
            self.download_date = ohio_get_last_updated().isoformat()
        meta = self.meta if self.meta is not None else {}
        meta["last_updated"] = self.download_date
        with open(self.main_file) as f:
            s3.Object(S3_BUCKET, self.generate_key(file_class=file_class))\
                .put(Body=f, Metadata=meta)


class Preprocessor(Loader):
    def __init__(self, raw_s3_file, config_file, **kwargs):
        super(Preprocessor, self).__init__(config_file=config_file,
                                           force_date=date_from_str(raw_s3_file), **kwargs)
        self.raw_s3_file = raw_s3_file
        if self.raw_s3_file is not None:
            self.s3_download()

    def s3_download(self):
        get_object(self.raw_s3_file, self.main_file)

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
                         self.config["format"]["ignore_files"]]
        else:
            all_files = [n for n in all_files]
        self.temp_files.extend(all_files)
        return all_files

    def concat_file_segments(self, file_names):
        """
        Serially concatenates the "file segments" into a single csv file. Should use this method when
        config["segmented_files"] is true. Should NOT be used to deal with files separated by column. Concatenates the
        files into self.main_file
        :param file_names: files to concatenate
        """
        if os.path.isfile(self.main_file):
            os.remove(self.main_file)
        first_success = False
        last_headers = None

        def list_compare(a, b):
            i = 0
            for j, k in zip(a, b):
                if j != k:
                    return j, k, i
                i += 1
            return False

        file_names = sorted(file_names, key=lambda x: os.stat(x).st_size, reverse=True)
        for f in file_names:
            print(f)
            try:
                if self.config["file_type"] == 'xlsx':
                    df = pd.read_excel(f)
                else:
                    df = pd.read_csv(f, comment="#")
            except (XLRDError, ParserError):
                print("Skipping {} ... Unsupported format, or corrupt file".format(f))
                continue
            if not first_success:
                last_headers = sorted(df.columns)
            df, _ = normalize_columns(df, last_headers)
            if list_compare(last_headers, sorted(df.columns)):
                mismatched_headers = list_compare(last_headers, df.columns)
                raise ValueError("file chunks contained different or misaligned headers:"
                                 "  {} != {} at index {}".format(*mismatched_headers))

            s = df.to_csv(header=not first_success, encoding='utf-8')
            first_success = True
            with open(self.main_file, "a+") as fo:
                fo.write(s)

    def preprocess_generic(self):
        logging.info("preprocessing generic")
        self.decompress(self.main_file)

    def preprocess_nevada(self):
        new_files = self.unpack_files(compression='unzip')
        voter_file = new_files[0] if "ElgbVtr" in new_files[0] else new_files[1]
        hist_file = new_files[0] if "VtHst" in new_files[0] else new_files[1]
        self.temp_files.extend([hist_file, voter_file])
        logging.info("NEVADA: loading historical file")
        df_hist = pd.read_csv(hist_file, header=None, comment="#")
        df_hist.columns = self.config["hist_columns"]
        logging.info("NEVADA: loading main voter file")
        df_voters = pd.read_csv(voter_file, header=None, comment="#")
        df_voters.columns = self.config["ordered_columns"]
        valid_elections = df_hist.date.unique().tolist()
        valid_elections.sort(key=lambda x: datetime.strptime(x, "%m/%d/%Y"))

        def place_vote_hist(g):
            group_idx = 0
            output = []
            for d in valid_elections[::-1]:
                if group_idx < len(g.date.values) and d == g.date.values[group_idx]:
                    output.append(g.vote_code.values[group_idx])
                    group_idx += 1
                else:
                    output.append('n')

            return output

        voting_histories = df_hist.groupby(self.config["voter_id"]).apply(place_vote_hist)
        df_voters["tmp_id"] = df_voters[self.config["voter_id"]]
        df_voters = df_voters.set_index("tmp_id")
        df_voters["all_history"] = voting_histories
        self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
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

        logging.info("FLORIDA: loading main voter file")
        df_voters = pd.read_csv(concat_voter_file, header=None, sep="\t")
        df_voters.columns = self.config["ordered_columns"]

        logging.info("Select & sort elections")
        df_hist = df_hist[df_hist["date"].map(lambda x: len(x)) > 5]
        logging.info("Map election names")
        df_hist["election_name"] = df_hist["date"] + "_" + \
            df_hist["election_type"]
        logging.info("Unique elections")
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
        sorted_codes_dict = {k: {"index": i, "count": counts[i]}
                             for i, k in enumerate(sorted_codes)}
        logging.info("Valid elections (len = {}) = {}".format(
            len(sorted_codes), sorted_codes))

        logging.info("Map array positions")
        df_hist["array_position"] = df_hist["election_name"].map(
            lambda x: int(sorted_codes_dict[x]["index"]))

        logging.info("Do history apply")
        voter_groups = df_hist.groupby("VoterID")
        all_history = voter_groups["array_position"].apply(list)
        logging.info("Do vote type apply")
        vote_type = voter_groups["vote_type"].apply(list)

        df_voters = df_voters.set_index(self.config["voter_id"])

        logging.info("Adding history columns to main df")
        df_voters["all_history"] = all_history
        df_voters["vote_type"] = vote_type

        self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
        df_voters.to_csv(self.main_file)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_arizona(self):
        new_files = self.unpack_files(compression="unzip")
        new_files = [f for f in new_files if "LEGEND.xlsx" not in f and "CANCELLED" not in f]
        self.concat_file_segments(new_files)
        main_df = pd.read_csv(self.main_file, comment="#")

        voting_action_cols = list(filter(lambda x: "party_voted" in x, main_df.columns.values))
        voting_method_cols = list(filter(lambda x: "voting_method" in x, main_df.columns.values))
        all_voting_history_cols = voting_action_cols + voting_method_cols

        main_df["all_history"] = df_to_postgres_array_string(main_df, voting_action_cols)
        main_df["all_voting_methods"] = df_to_postgres_array_string(main_df, voting_method_cols)
        main_df[self.config["birthday_identifier"]] = pd.to_datetime(main_df[self.config["birthday_identifier"]],
                                                                     format=self.config["date_format"],
                                                                     errors='coerce')
        elections_key = [c.split("_")[-1] for c in voting_action_cols]
        main_df.drop(all_voting_history_cols, axis=1, inplace=True)
        main_df.to_csv(self.main_file, encoding='utf-8', index=False)
        write_meta(self.main_file, message="arizona_{}".format(datetime.now().isoformat()), array_dates=elections_key)
        self.meta = {
            "message": "arizona_{}".format(datetime.now().isoformat()),
            "array_dates": json.dumps(elections_key)
        }
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_new_york(self):
        config = load_configs_from_file("new_york")
        new_files = self.unpack_files(compression="unzip")
        main_file = new_files[0]
        main_df = pd.read_csv(main_file, comment="#", header=None, names=config["ordered_columns"])
        main_df.voterhistory[main_df.voterhistory != main_df.voterhistory] = NULL_CHAR
        all_codes = main_df.voterhistory.str.replace(" ", "_").str.replace("[", "").str.replace("]", "")
        all_codes = all_codes.str.cat(sep=";")
        all_codes = np.array(all_codes.split(";"))
        main_df["all_history"] = strcol_to_array(main_df.voterhistory, delim=";")
        unique_codes, counts = np.unique(all_codes, return_counts=True)
        beginning_of_time = datetime(1979, 1, 1)

        def extract_date(s):
            date = date_from_str(s)
            year = [w for w in s.split("_") if w.isdigit()]
            if date is not None:
                output = parser.parse(date)
            elif len(year) > 0:
                output = datetime(year=int(year[0]), month=1, day=1)
            else:
                output = beginning_of_time
            return output
        sorted_codes = list(sorted(unique_codes, key=lambda c: extract_date(c), reverse=True))
        sorted_codes_dict = {k: i for i, k in enumerate(sorted_codes)}

        def insert_code_bin(arr):
            new_arr = [0] * len(sorted_codes)
            for item in arr:
                new_arr[sorted_codes_dict[item]] = 1
            return new_arr

        main_df.all_history = main_df.all_history.apply(insert_code_bin)

        self.meta = {
            "message": "new_york_{}".format(datetime.now().isoformat()),
            "array_dates": json.dumps(sorted_codes)
        }
        main_df.to_csv(self.main_file, encoding='utf-8', index=False)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def execute(self):
        self.state_router()

    def state_router(self):
        routes = {
            'nevada': self.preprocess_nevada,
            'arizona': self.preprocess_arizona,
            'florida':self.preprocess_florida,
            'new_york': self.preprocess_new_york
        }
        if self.config["state"] in routes:
            f = routes[self.config["state"]]
            logging.info("preprocessing {}".format(self.config["state"]))
            f()
        else:
            self.preprocess_generic()


if __name__ == '__main__':
    print(ohio_get_last_updated())
