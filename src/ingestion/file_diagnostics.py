import inspect
import json
import os
import uuid
from os import stat
import pandas as pd
from analysis import Snapshot, SnapshotConsistencyError
from storage import get_preceding_upload
from storage import get_raw_s3_uploads, load_configs_from_file, state_from_str, config_file_from_state
from ingestion.download import Preprocessor
from constants import logging, S3_BUCKET, PROCESSED_FILE_PREFIX, RAW_FILE_PREFIX
from storage.connections import s3
from zipfile import ZipFile, ZIP_DEFLATED
import subprocess


class TestFileBuilder(Preprocessor):
    def __init__(self, state=None, s3_key=None, local_file=None):
        if s3_key is not None and state is not None and state_from_str(s3_key) != state:
            raise ValueError("state and s3 must be in agreement if both are set")
        elif s3_key is None and state is not None and local_file is None:
            s3_keys = get_raw_s3_uploads(state=state, testing=False)
            if len(s3_keys) == 0:
                raise ValueError("no raw uploads available to create test file")
            else:
                s3_key = s3_keys[-1].key
        elif s3_key is not None and state is None:
            state = state_from_str(s3_key)
        elif local_file is None:
            raise ValueError("TestFileBuilder must be initialized with either 'state' or 's3_key' or 'local_file'")
        print(s3_key)
        config_file = config_file_from_state(state)
        super(TestFileBuilder, self).__init__(raw_s3_file=s3_key, config_file=config_file, force_file=local_file)
        if state is None:
            self.state = state_from_str(s3_key)
        else:
            self.state = state
        self.config = load_configs_from_file(state=self.state)
        self.local_file = local_file

    def get_smallest_counties(self, df, count=2):
        counties = df[self.config["county_identifier"]].value_counts().reset_index()
        counties["county"] = counties[counties.columns[0]]
        counties["count"] = counties[self.config["county_identifier"]]
        counties.drop(columns=[counties.columns[0], self.config["county_identifier"]], inplace=True)
        small_counties = counties.values[-count:, 0]
        return small_counties

    def filter_counties(self, df, counties):
        filtered_data = df[(df[self.config["county_identifier"]] == counties[0]) |
                           (df[self.config["county_identifier"]] == counties[1])]
        filtered_data.reset_index(inplace=True, drop=True)
        return filtered_data

    def sample(self, df, frac=0.01):
        sampled_data = df.head(n=int(frac * len(df)))
        sampled_data.reset_index(inplace=True, drop=True)
        return sampled_data

    def test_key(self, name):
        return "testing/{}/{}/{}".format(RAW_FILE_PREFIX, self.state, name)

    def __build_new_york(self):
        new_files = self.unpack_files()
        ny_file = new_files[0]
        truncated_file = ny_file + ".head"
        # self.temp_files.append(truncated_file)
        config = load_configs_from_file(state="new_york")
        os.system("head -4000 {0} > {1}".format(ny_file, truncated_file))

        df = pd.read_csv(truncated_file, names=config["ordered_columns"], header=None)
        two_small_counties = self.get_smallest_counties(df, count=2)
        filtered_data = self.filter_counties(df, counties=two_small_counties)
        filtered_data.to_csv(ny_file, header=False)
        filtered_data.to_csv("test.csv", header=False)
        with ZipFile(self.main_file, 'w', ZIP_DEFLATED) as zf:
            zf.write(ny_file, os.path.basename(ny_file))
        self.temp_files.append(ny_file)
        self.temp_files.append(truncated_file)

    def __build_arizona(self):
        new_files = self.unpack_files()
        self.temp_files.extend(new_files)
        for f in new_files:
            logging.info("reading {}".format(f))
            df = pd.read_excel(f)
            if df.shape[0] > 1000:
                logging.info("sampling {}".format(f))
                sampled = self.sample(df, frac=0.001)

                # insert malformed date
                sampled[self.config["birthday_identifier"]].iloc[-1] = "schfiftyfive"
                sampled.to_excel(f)
            else:
                logging.info("skipping...")
        with ZipFile(self.main_file, 'w', ZIP_DEFLATED) as zf:
            for f in new_files:
                zf.write(f, os.path.basename(f))

    def __build_ohio(self):
        """
        this only generates a truncated _processed_ file, no test raw file generator is written for ohio (todo)
        :return: None
        """
        df = pd.read_csv(self.main_file, compression='gzip', comment="#")
        two_small_counties = self.get_smallest_counties(df, count=2)
        filtered_data = self.filter_counties(df, counties=two_small_counties)
        filtered_data.to_csv(self.main_file, compression='gzip')
        logging.info("using '{}' counties".format(" and ".join([str(a) for a in two_small_counties.tolist()])))

    def build(self, file_name=None, save_local=False, save_remote=True):
        if file_name is None:
            file_name = self.raw_s3_file.split("/")[-1] if self.raw_s3_file is not None else \
                self.local_file.split("/")[-1]

        routes = {"ohio": self.__build_ohio,
                  "arizona": self.__build_arizona,
                  "new_york": self.__build_new_york}
        f = routes[self.state]
        f()

        if save_remote:
            with open(self.main_file) as f:
                s3.Object(S3_BUCKET, self.test_key(file_name)).put(Body=f.read(),
                                                                   Metadata={
                                                                       "last_updated": self.download_date
                                                                   })
        if save_local:
            os.rename(self.main_file, file_name)


class ProcessedTestFileBuilder(object):
    def __init__(self, s3_key, compression="gzip", size=5000, randomize=False):
        self.df, self.meta = Snapshot.load_from_s3(s3_key, compression=compression)
        self.state = state_from_str(s3_key)
        self.randomize = randomize
        self.size = size

    def test_key(self, name):
        return "/testing/{}/{}/{}".format(PROCESSED_FILE_PREFIX, self.state, name)

    def build(self, file_name):
        if self.randomize:
            df = self.df.sample(n=self.size)
        else:
            df = self.df.head(n=self.size)
        df.to_csv(file_name, compression='gzip')


class DiagnosticTest(object):
    """
    This class gets used to ensure that each uploaded snapshot is consistent
    with it's configuration file and with past snapshots. We run this check
    before inserting the new changes into the modifications table and it's
    descendants.
    """
    def __init__(self, file_path, config_file, preproc_obj):
        self.file_path = file_path
        self.configs = load_configs_from_file(config_file=config_file)
        self.preproc_obj = preproc_obj
        self.logs = ""
        self.config_file = config_file
        if "tmp" not in os.listdir("/"):
            os.system("mkdir /tmp")

    def log_msg(self, msg):
        try:
            f_name = inspect.stack()[1][3]
        except IndexError:
            f_name = "main"
        self.logs += "{}: {}\n".format(f_name, msg)

    def test_file_size(self):
        fchange_threshold = 0.15

        df = get_preceding_upload(self.configs["state"],
                                  self.preproc_obj.download_date)

        preceding_upload = json.loads(df.to_json(orient="records"))
        if len(preceding_upload) == 0:
            self.log_msg("-- PASSED -- since there is no preceding file, there"
                         " is no filesize check")
            return True
        else:
            preceding_upload = preceding_upload[0]
            if preceding_upload["size"] is not None:
                last_fs = preceding_upload["size"]
                this_fs = stat(self.file_path).st_size
                print(preceding_upload)
                bigger_file = last_fs if last_fs > this_fs else this_fs
                smaller_file = last_fs if last_fs <= this_fs else this_fs
                fchange = (bigger_file - smaller_file) / bigger_file
                if fchange > fchange_threshold:
                    if bigger_file == last_fs:
                        self.log_msg("-- FAILED -- compressed file shrank by "
                                     "{}% (>{}%) since last upload"
                                     .format(fchange, fchange_threshold * 100))
                    else:
                        self.log_msg("-- FAILED -- compressed file grew by "
                                     "{}% (>{}%) since last upload"
                                     .format(fchange, fchange_threshold * 100))
                    return False
                else:
                    self.log_msg("-- PASSED -- compressed file sizes changed "
                                 "by {}% (<{}%)"
                                 .format(fchange, fchange_threshold * 100))
            else:
                self.log_msg("-- PASSED -- no preceding file size")
        return True

    def test_snapshots_dryrun(self):
        tmp_test_file = "/tmp/voteshield_head_{}_{}.csv"\
            .format(uuid.uuid4(), self.preproc_obj.download_date)
        self.preproc_obj.temp_files.append(tmp_test_file)
        os.system("head -1000 {} > {}".format(self.file_path, tmp_test_file))
        print(self.file_path)
        try:
            test_s = Snapshot(config_file=self.config_file,
                              file_name=tmp_test_file)
            self.log_msg("-- PASSED --")
            return True
        except SnapshotConsistencyError as e:
            self.log_msg("-- FAILED -- " + str(e))
            return False

    def run_all_tests(self):
        t0 = self.test_file_size()
        t1 = self.test_snapshots_dryrun()
        return all([t0, t1]), self.logs


if __name__ == '__main__':
    import sys
    with TestFileBuilder(local_file=sys.argv[1], state=sys.argv[2]) as tf:
        tf.build()
