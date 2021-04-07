from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from dateutil import parser
from reggie.ingestion.utils import MissingNumColumnsError, format_column_name, MissingFilesError
import logging
import pandas as pd
import datetime
from io import StringIO, BytesIO, SEEK_END, SEEK_SET
import numpy as np
from datetime import datetime
import gc
import json
import re

"""
Todo:
Add Important Columns and check order (currently precicnts are added before County)

"""


class PreprocessOklahoma(Preprocessor):
    def __init__(self, raw_s3_file, config_file, force_date=None, **kwargs):

        if force_date is None:
            force_date = date_from_str(raw_s3_file)

        super().__init__(
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            **kwargs
        )
        self.raw_s3_file = raw_s3_file
        self.processed_file = None

    def execute(self):
        if self.raw_s3_file is not None:
            self.main_file = self.s3_download()

        new_files = self.unpack_files(self.main_file)
        # voter_files = [n for n in new_files if "ctysw_vr" in n["name"].lower()]
        # probably don't need all these loopw
        precincts_file = [x for x in new_files if 'precincts' in x["name"].lower()][0]
        voter_files = list(filter(lambda v: re.match('cty[0-9]+_vr', v["name"].lower()), new_files))
        # self.file_check(len(voter_files))
        # hist_files = [n for n in new_files if "ctysw_vh" in n["name"].lower()]
        hist_files = list(filter(lambda v: re.match('cty[0-9]+_vh', v["name"].lower()), new_files))
        vdf = pd.DataFrame()
        hdf = pd.DataFrame()
        dtypes = self.config['dtypes']
        cty_map = dict([(value, key) for key, value in self.config['county_codes'].items()])

        def county_map(pct):
            def mapping(prec):
                county = cty_map[prec[:2]]
                return county

            return pd.Series(
                map(mapping, pct.tolist())
            )

        for file in voter_files:
            if "vr.csv" in file["name"].lower():
                temp_vdf = pd.read_csv(file["obj"], encoding='latin', dtype=dtypes)
                vdf = pd.concat([vdf, temp_vdf], ignore_index=True)
        vdf.drop_duplicates(inplace=True)


        precincts = pd.read_csv(precincts_file["obj"], encoding='latin', dtype={'PrecinctCode': 'string'})
        precincts.rename(columns={"PrecinctCode": "Precinct"}, inplace=True)
        if precincts.empty:
            raise ValueError("Missing Precicnts file")
        vdf = vdf.merge(precincts, how='left', on='Precinct')

        vdf['County'] = county_map(vdf['Precinct'])
        self.reconcile_columns(vdf, self.config["columns"])
        for file in hist_files:
            temp_hdf = pd.read_csv(file["obj"], dtype={'VoterID': 'string'})
            hdf = pd.concat(
                [hdf, temp_hdf], ignore_index=True,
            )

        valid_elections, counts = np.unique(hdf["ElectionDate"], return_counts=True)
        count_order = counts.argsort()[::-1]
        valid_elections = valid_elections[count_order]
        counts = counts[count_order]
        sorted_codes = valid_elections.tolist()
        sorted_codes_dict = {
            k: {"index": i, "count": int(counts[i]), "date": date_from_str(k)}
            for i, k in enumerate(sorted_codes)
        }
        hdf["array_position"] = hdf["ElectionDate"].map(
            lambda x: int(sorted_codes_dict[x]["index"])
        )
        vdf.set_index(self.config["voter_id"], drop=False, inplace=True)
        voter_groups = hdf.groupby(self.config["voter_id"])
        vdf["all_history"] = voter_groups["ElectionDate"].apply(list)
        vdf["sparse_history"] = voter_groups["array_position"].apply(list)
        vdf["votetype_history"] = voter_groups["VotingMethod"].apply(list)
        self.meta = {
            "message": "oklahoma_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        # raise ValueError("stopping")
        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(vdf.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
