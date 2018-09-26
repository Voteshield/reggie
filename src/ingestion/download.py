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
from storage import generate_s3_key, date_from_str, load_configs_from_file, \
    df_to_postgres_array_string, strcol_to_postgres_array_str, strcol_to_array,\
    listcol_tonumpy
from storage import s3, normalize_columns
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
        p = Popen(["unzip", file_name, "-d", new_loc],
                  stdout=PIPE, stderr=PIPE, stdin=PIPE)
        p.communicate("A")
        return new_loc, p

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
        return new_loc, p

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
        return new_loc, p

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
                  stdout=open(new_loc, "w"), stderr=PIPE, stdin=PIPE)
        p.communicate()
        return new_loc, p

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
        :return: a (str, bool) tuple containing the location of the processed file and whether or not it was actually
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

            if compression_type is not None and p.returncode == 0:
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
        super(Preprocessor, self).__init__(config_file=config_file,
                                           force_date=date_from_str(raw_s3_file), **kwargs)
        self.raw_s3_file = raw_s3_file
        if self.raw_s3_file is not None:
            self.s3_download()

    def s3_download(self):
        self.main_file = "/tmp/voteshield_{}"\
            .format(self.raw_s3_file.split("/")[-1])
        get_object(self.raw_s3_file, self.main_file)
        self.temp_files.append(self.main_file)

    def coerce_dates(self, df):
        """
        takes all columns with timestamp or date labels in the config file and forces the corresponding entries in the
        raw file into datetime objects
        :param df: dataframe to modify
        :return: modified dataframe
        """
        date_fields = [c for c, v in self.config["columns"].items() if v == "date" or v == "timestamp"]
        for field in date_fields:
            df[field] = df[field].apply(str)
            df[field] = pd.to_datetime(df[field], format=self.config["date_format"], errors='coerce')
        return df

    def coerce_numeric(self, df, extra_cols=[]):
        """
        takes all columns with int labels in the config file
        as well as any requested extra columns,
        and forces the corresponding entries in the
        raw file into numerics
        :param df: dataframe to modify
        :param extra_cols: other columns to convert
        :return: modified dataframe
        """
        numeric_fields = [c for c, v in self.config["columns"].items()
                          if v == "int"]
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce')
        for field in extra_cols:
            df[field] = pd.to_numeric(df[field],
                                      errors='coerce').fillna(df[field])
        return df

    def unpack_files(self, compression="unzip"):
        all_files = []

        def expand_recurse(file_name):
            decompressed_result, success = self.decompress(file_name, compression_type=compression)

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
        logging.info("unpacked: - {}".format(all_files))
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
        self.meta = {
            "message": "arizona_{}".format(datetime.now().isoformat()),
            "array_dates": json.dumps(elections_key)
        }
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_new_york(self):
        config = load_configs_from_file("new_york")
        new_files = self.unpack_files(compression="infer")
        main_file = filter(lambda x: x[-4:] != ".pdf", new_files)[0]
        main_df = pd.read_csv(main_file, comment="#",
                              header=None,
                              names=config["ordered_columns"])
        main_df.voterhistory[main_df.voterhistory != main_df.voterhistory] = NULL_CHAR
        all_codes = main_df.voterhistory.str.replace(" ", "_")\
            .str.replace("[", "")\
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
        main_df = self.coerce_dates(main_df)
        self.meta = {
            "message": "new_york_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        main_df.to_csv(self.main_file, index=False, compression="gzip")
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
            sorted_codes_dict = {k: {"index": i, "count": counts[i]}
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

        main_df = self.coerce_dates(main_df)
        main_df = self.coerce_numeric(main_df, extra_cols=[
            "Residential ZipCode", "Mailing ZipCode", "Precinct",
            "House Number", "Unit Number", "Split"])

        self.meta = {
            "message": "missouri_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
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
            'new_york': self.preprocess_new_york,
            'missouri': self.preprocess_missouri
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
