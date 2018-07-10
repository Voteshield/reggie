import uuid
from datetime import datetime
from subprocess import Popen, PIPE

import bs4
import pandas as pd
import requests
from dateutil import parser

from constants import *
from storage import generate_s3_key, date_from_str, load_configs_from_file, df_to_postgres_array_string
from storage import s3


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

    def __init__(self, config_file=CONFIG_OHIO_FILE, force_date=None, force_file=None, clean_up_tmp_files=True):
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

        if force_date is not None:
            self.download_date = parser.parse(force_date).isoformat()
        else:
            self.download_date = datetime.now().isoformat()

        if force_file is not None:
            self.main_file = force_file
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
        if not self.is_compressed:
            logging.info("compressing")
            p = Popen([compression_type, self.main_file], stdout=PIPE, stderr=PIPE)
            p.communicate()
            if self.main_file[-3:] != ".gz":
                self.main_file += ".gz"
            self.is_compressed = True

    def decompress(self, compression_type="gunzip"):
        if self.is_compressed:
            logging.info("decompressing {}".format(self.main_file))
            if compression_type == "unzip":
                logging.info("decompressing unzip {} to {}".format(self.main_file, os.path.dirname(self.main_file)))
                print([compression_type, self.main_file, "-d", os.path.dirname(self.main_file)])
                p = Popen([compression_type, self.main_file, "-d", os.path.dirname(self.main_file)],
                          stdout=PIPE, stderr=PIPE, stdin=PIPE)
                p.communicate("A")
            else:
                logging.info("decompressing gunzip {} to {}".format(self.main_file, os.path.dirname(self.main_file)))
                p = Popen([compression_type, self.main_file], stdout=PIPE, stderr=PIPE, stdin=PIPE)
                p.communicate()
            logging.info("decompressing done".format(self.main_file))
            if self.main_file[-3:] == ".gz":
                self.main_file = self.main_file[:-3]
            self.is_compressed = False

    def generate_key(self, file_class=PROCESSED_FILE_PREFIX):
        return generate_s3_key(file_class, self.state, self.source,
                               self.download_date, self.file_type, self.is_compressed)

    def s3_dump(self, file_class=PROCESSED_FILE_PREFIX):
        if self.config["state"] == 'ohio' and self.obj_will_download:
            self.download_date = ohio_get_last_updated().isoformat()
    
        with open(self.main_file) as f:
            s3.Object(S3_BUCKET, self.generate_key(file_class=file_class))\
                .put(Body=f, Metadata={"last_updated": self.download_date})


class Preprocessor(Loader):
    def __init__(self, raw_s3_file, config_file, clean_up_tmp_files=True):
        super(Preprocessor, self).__init__(config_file=config_file,
                                           force_date=date_from_str(raw_s3_file),
                                           clean_up_tmp_files=clean_up_tmp_files)
        self.raw_s3_file = raw_s3_file
        self.s3_download()

    def s3_download(self):
        get_object(self.raw_s3_file, self.main_file)

    def unpack_files(self, compression="unzip"):
        self.is_compressed = True
        dir_path = os.path.dirname(self.main_file)
        before = set(os.listdir(dir_path))
        print(before)
        self.decompress(compression_type=compression)
        after = set(os.listdir(dir_path))
        print(after)
        new_files = list(after.difference(before))
        if "ignore_files" in self.config["format"]:
            new_files = ["{}/{}".format(dir_path, n) for n in new_files if n not in
                         self.config["format"]["ignore_files"]]
        else:
            new_files = ["{}/{}".format(dir_path, n) for n in new_files]
        return new_files

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
            for j, k in zip(a, b):
                if j != k:
                    return False
            return True

        for f in file_names:
            if self.config["file_type"] == 'xlsx':
                df = pd.read_excel(f)
            else:
                df = pd.read_csv(f)
            s = df.to_csv(header=not first_success)
            if last_headers is not None and not list_compare(last_headers, df.columns):
                raise ValueError("file chunks contained different or misaligned headers")
            if not first_success:
                last_headers = df.columns
            first_success = True
            with open(self.main_file, "a+") as fo:
                fo.write(s)

    def preprocess_generic(self):
        logging.info("preprocessing generic")
        self.decompress()

    def preprocess_nevada(self):
        new_files = self.unpack_files(compression='unzip')
        voter_file = new_files[0] if "ElgbVtr" in new_files[0] else new_files[1]
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
        df_voters = df_voters.set_index(self.config["voter_id"])
        df_voters["all_history"] = voting_histories
        self.main_file = "/tmp/voteshield_{}.tmp".format(uuid.uuid4())
        df_voters.to_csv(self.main_file)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def preprocess_arizona(self):
        new_files = self.unpack_files(compression="unzip")
        new_files = [f for f in new_files if "LEGEND.xlsx" not in f and ""]
        self.concat_file_segments(new_files)
        main_df = pd.read_csv(self.main_file)
        voting_method_prefix = "voting_method"
        voting_party_prefix = "voting_party"
        all_voting_history_cols = filter(lambda x: any([pre in x for pre in
                                                        (voting_party_prefix, voting_method_prefix)]),
                                         main_df.columns.values)
        voting_action_cols = filter(lambda x: "party_voted" in x, main_df.columns.values)
        voting_method_cols = filter(lambda x: "voting_method" in x, main_df.columns.values)
        main_df["all_history"] = df_to_postgres_array_string(main_df, voting_action_cols)
        main_df["all_voting_methods"] = df_to_postgres_array_string(main_df, voting_method_cols)
        main_df.drop(all_voting_history_cols, axis=1, inplace=True)
        main_df.to_csv(self.main_file)
        self.temp_files.append(self.main_file)
        chksum = self.compute_checksum()
        return chksum

    def execute(self):
        self.state_router()

    def state_router(self):
        routes = {
            'nevada': self.preprocess_nevada,
            'arizona': self.preprocess_arizona
        }
        if self.config["state"] in routes:
            f = routes[self.config["state"]]
            logging.info("preprocessing {}".format(self.config["state"]))
            f()
        else:
            self.preprocess_generic()


if __name__ == '__main__':
    print(ohio_get_last_updated())
