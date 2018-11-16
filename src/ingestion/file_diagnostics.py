import inspect
import json
import os
import uuid
from os import stat, remove
import pandas as pd
import numpy as np
from analysis import Snapshot, SnapshotConsistencyError
from storage import get_preceding_upload
from configs.configs import Config
from storage import get_raw_s3_uploads, state_from_str
from ingestion.download import Preprocessor
from constants import logging, S3_BUCKET, PROCESSED_FILE_PREFIX, RAW_FILE_PREFIX
from storage.connections import s3
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
import subprocess
from datetime import datetime
from subprocess import Popen, PIPE


class TestFileBuilder(Preprocessor):
    def __init__(self, s3_key=None, local_file=None, state=None):
        if s3_key is not None and state is None:
            state = state_from_str(s3_key)

        config_file = Config.config_file_from_state(state)
        super(TestFileBuilder, self).__init__(
              raw_s3_file=s3_key,
              config_file=config_file,
              force_file=local_file)
        if state is None:
            self.state = state_from_str(s3_key)
        else:
            self.state = state
        self.config = Config(state=self.state)
        self.local_file = local_file

    def get_smallest_counties(self, df, count=2):
        counties = df[self.config["county_identifier"]
                      ].value_counts().reset_index()
        counties["county"] = counties[counties.columns[0]]
        counties["count"] = counties[self.config["county_identifier"]]
        counties.drop(
            columns=[
                counties.columns[0],
                self.config["county_identifier"]],
            inplace=True)
        small_counties = counties.values[-count:, 0]
        return small_counties

    def filter_counties(self, df, counties):
        filtered_data = df[(
            df[self.config["county_identifier"]] == counties[0]) |
            (df[self.config["county_identifier"]] == counties[1])]
        filtered_data.reset_index(inplace=True, drop=True)
        return filtered_data

    def sample(self, df, frac=0.01):
        sampled_data = df.head(n=int(frac * len(df)))
        sampled_data.reset_index(inplace=True, drop=True)
        return sampled_data

    def test_key(self, name):
        return "testing/{}/{}/{}".format(RAW_FILE_PREFIX, self.state, name)

    def __build_florida(self):
        new_files = self.unpack_files()
        # insert code to get the method 1
        smallest_counties = []
        for i in new_files:
            if ("LIB" in i) or ("LAF" in i):
                smallest_counties.append(i)
        vote_history_files = []
        voter_files = []
        for i in smallest_counties:
            if "_H_" in i:
                vote_history_files.append(i)
            elif ".txt" in i:
                voter_files.append(i)
        # florida files are composed as nested zip files, but because of the
        # recursive file structure and because of how preprocess florida is
        # made it shouldn't matter
        with ZipFile(self.main_file, 'w', ZIP_DEFLATED) as zf:
            for f in smallest_counties:
                zf.write(f, os.path.basename(f))

    def __build_new_york(self):
        new_files = self.unpack_files()
        ny_file = new_files[0]
        truncated_file = ny_file + ".head"
        config = Config(state="new_york")
        os.system("head -4000 {0} > {1}".format(ny_file, truncated_file))

        df = pd.read_csv(
            truncated_file,
            names=config["ordered_columns"],
            header=None)
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

                sampled[self.config["birthday_identifier"]
                        ].iloc[-1] = "schfiftyfive"
                sampled.to_excel(f)
            else:
                logging.info("skipping...")
        with ZipFile(self.main_file, 'w', ZIP_DEFLATED) as zf:
            for f in new_files:
                zf.write(f, os.path.basename(f))

    def __build_iowa(self):
        new_files = self.unpack_files()

        #read in dataframe here, haven't figured this out yet

        print(new_files)
        smaller_files = []
        for x in new_files:
            if "CD1" in x and "Part1" in x:
                smaller_files.append(x)

        with ZipFile(self.main_file, 'w', ZIP_DEFLATED) as zf:
            for f in smaller_files:
                zf.write(f, os.path.basename(f))

    def __build_ohio(self):
        """
        this only generates a truncated _processed_ file, no test raw file
        generator is written for ohio (todo)
        :return: None
        """
        df = pd.read_csv(self.main_file, compression='gzip')
        two_small_counties = self.get_smallest_counties(df, count=2)
        filtered_data = self.filter_counties(df, counties=two_small_counties)
        filtered_data.to_csv(self.main_file, compression='gzip')
        logging.info("using '{}' counties".format(
            " and ".join([str(a) for a in two_small_counties.tolist()])))

    def __build_missouri(self):
        new_file = self.unpack_files(compression="7zip")
        new_file = new_file[0]
        df = pd.read_csv(new_file, sep='\t')
        two_small_counties = self.get_smallest_counties(df, count=2)
        filtered_data = self.filter_counties(df, counties=two_small_counties)
        filtered_data.to_csv(new_file, index=False, sep='\t')
        # have to delete current main_file before
        # we can 7zip new file to that location
        os.remove(self.main_file)
        p = Popen(["7z", "a", "-tzip", self.main_file, new_file],
                  stdout=PIPE, stderr=PIPE, stdin=PIPE)
        out, err = p.communicate()
        self.temp_files.append(new_file)

    def __build_michigan(self):
        new_files = self.unpack_files()
        for file in new_files:
            if 'state_h' in file:
                hist_file = file
            if 'state_v' in file:
                voter_file = file
        config = Config(state='michigan')
        logging.info("Detected voter file: " + voter_file)
        logging.info("Detected history file: " + hist_file)

        vcolspecs = [[0, 35], [35, 55], [55, 75], [75, 78], [78, 82], [82, 83], [83, 91],
                  [91, 92], [92, 99], [99, 103], [103, 105], [105, 135], [135, 141],
                  [141, 143], [143, 156], [156, 191], [191, 193], [193, 198], [198, 248],
                  [248, 298], [298, 348], [348, 398], [398, 448], [448, 461], [461, 463],
                  [463, 468], [468, 474], [474, 479], [479, 484], [484, 489], [489, 494],
                  [494, 499], [499, 504], [504, 510], [510, 516], [516, 517], [517, 519], [519,520]]
        hcolspecs = [[0, 13], [13, 15], [15, 20], [20, 25], [25, 38], [38, 39]]
        vdf = pd.read_fwf(voter_file, chunksize=1000000, colspecs=vcolspecs, names=config["ordered_columns"], na_filter=False)
        vdf = vdf.next()
        two_small_counties = self.get_smallest_counties(vdf, count=2)
        filtered_data = self.filter_counties(vdf, counties=two_small_counties)
        hdf = pd.read_fwf(hist_file, chunksize=1000000, colspecs=hcolspecs, names=config["hist_columns"], na_filter=False)
        hdf = hdf.next()
        hdf = self.sample(hdf, frac=0.001)
        with open(new_files[0], 'w+') as vfile:
            fmt = '%35s %20s %20s %3s %4s %1s %8s %1s %7s %4s %2s %30s %6s %2s %13s %35s %2s %5s %50s %50s %50s %50s %50s' \
                  '%13s %2s %5s %6s %5s %5s %5s %5s %5s %5s %6s %6s %1s %2s %1s'
            np.savetxt(vfile, filtered_data.values, fmt=fmt)
        p = Path(voter_file)
        temp_dir = Path(voter_file).parent.parent
        Path(str(temp_dir) + '/' + p.parent.name[:-13]).unlink()
        with ZipFile(str(temp_dir) + '/' + p.parent.name[:-13], 'w', ZIP_DEFLATED) as zf:
            zf.write(voter_file, os.path.basename(new_files[0]))
        Path(voter_file).unlink()
        Path(voter_file).parent.rmdir()
        with open(hist_file, 'w+') as hfile:
            fmt = '%13s %2s %5s %5s %13s %1s'
            np.savetxt(hfile, hdf.values, fmt=fmt)
        p = Path(hist_file)
        Path(str(temp_dir) + '/' + p.parent.name[:-13]).unlink()
        with ZipFile(str(temp_dir) + '/' + p.parent.name[:-13], 'w', ZIP_DEFLATED) as zf:
            zf.write(hist_file, os.path.basename(hist_file))
        Path(hist_file).unlink()
        Path(hist_file).parent.rmdir()

        def zipper(path, zf):
            for root, dirs, files in os.walk(path):
                for f in files:
                    zf.write(os.path.join(str(temp_dir), f))

        logging.info("Main file: " + self.main_file)
        with ZipFile(self.main_file, 'w', ZIP_DEFLATED) as zf:
            zipper(self.main_file + '_decompressed', zf)

    def __upload_michigan(self):
        # numpy doesn't want to create fixed width files without a separator,
        # so it must be created locally
        return

    def __build_new_jersey(self):
        # built manually
        return

    def build(self, file_name=None, save_local=False, save_remote=True):
        if file_name is None:
            file_name = self.raw_s3_file.split("/")[-1] \
                if self.raw_s3_file is not None else \
                self.local_file.split("/")[-1]

        routes = {"ohio": self.__build_ohio,
                  "arizona": self.__build_arizona,
                  "new_york": self.__build_new_york,
                  "michigan": self.__build_michigan,
                  "florida": self.__build_florida,
                  "missouri": self.__build_missouri,
                  "iowa": self.__build_iowa,
                  "new_jersey": self.__build_new_jersey
                  }

        f = routes[self.state]
        f()

        if save_remote:
            logging.info(self.test_key(file_name))
            with open(self.main_file) as f:
                s3.Object(
                    S3_BUCKET, self.test_key(file_name)).put(
                    Body=f.read(), Metadata={
                        "last_updated": self.download_date})
        if save_local:
            os.rename(self.main_file, file_name)


class ProcessedTestFileBuilder(object):
    def __init__(self, s3_key, compression="gzip", size=5000, randomize=False):
        self.df, self.meta = Snapshot.load_from_s3(
            s3_key, compression=compression)
        self.state = state_from_str(s3_key)
        self.randomize = randomize
        self.size = size

    def test_key(self, name):
        return "/testing/{}/{}/{}".format(PROCESSED_FILE_PREFIX,
                                          self.state, name)

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
        self.configs = Config(config_file=config_file)
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
        except pd.errors.ParserError as e:
            test_s = Snapshot(config_file=self.config_file,
                              file_name=self.file_path,
                              s3_compression="gzip",
                              date=datetime.now().isoformat())
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
