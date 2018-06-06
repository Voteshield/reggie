import inspect
import json
import os
import uuid
from os import stat

from analysis import Snapshot, SnapshotConsistencyError
from storage import get_preceding_upload
from storage import load_configs_from_file


class DiagnosticTest(object):
    """
    This class gets used to ensure that each uploaded snapshot is consistent with it's configuration file and with past
    snapshots. We run this check before inserting the new changes into the modifications table and it's descendants.
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
        df = get_preceding_upload(self.configs["state"], self.preproc_obj.download_date)
        preceding_upload = json.loads(df.to_json(orient="records"))
        if len(preceding_upload) == 0:
            self.log_msg("-- PASSED -- since there is no preceding file, there is no filesize check")
            return True
        else:
            preceding_upload = preceding_upload[0]
            last_fs = preceding_upload["size"]
            this_fs = stat(self.file_path).st_size
            bigger_file = last_fs if last_fs > this_fs else this_fs
            smaller_file = last_fs if last_fs <= this_fs else this_fs
            fchange = (bigger_file - smaller_file) / bigger_file
            if fchange > fchange_threshold:
                if bigger_file == last_fs:
                    self.log_msg("-- FAILED -- compressed file shrank by {}% (>{}%) since last upload"
                                 .format(fchange, fchange_threshold * 100))
                else:
                    self.log_msg("-- FAILED -- compressed file grew by {}% (>{}%) since last upload"
                                 .format(fchange, fchange_threshold * 100))
                return False
            else:
                self.log_msg("-- PASSED -- compressed file sizes changed by {}% (<{}%)".format(fchange,
                                                                                               fchange_threshold * 100))
        return True

    def test_snapshots_dryrun(self):
        tmp_test_file = "/tmp/voteshield_head_{}_{}.csv".format(uuid.uuid4(), self.preproc_obj.download_date)
        self.preproc_obj.temp_files.append(tmp_test_file)
        os.system("head -1000 {} > {}".format(self.file_path, tmp_test_file))
        print(self.file_path)
        try:
            test_s = Snapshot(config_file=self.config_file, file_name=tmp_test_file)
            self.log_msg("-- PASSED --")
            return True
        except SnapshotConsistencyError as e:
            self.log_msg("-- FAILED -- " + str(e))
            return False

    def run_all_tests(self):
        t0 = self.test_file_size()
        t1 = self.test_snapshots_dryrun()
        return all([t0, t1]), self.logs
