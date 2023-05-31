import datetime
import gc
import json
import logging
import re

from datetime import datetime
from dateutil import parser
from io import StringIO, BytesIO, SEEK_END, SEEK_SET

import numpy as np
import pandas as pd

from reggie.ingestion.download import (
    Preprocessor,
    date_from_str,
    FileItem,
    concat_and_delete,
)
from reggie.ingestion.utils import (
    format_column_name,
    MissingFilesError,
    MissingNumColumnsError,
)


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
        precincts_file = [x for x in new_files if 'precincts' in x["name"].lower()][0]
        if precincts_file is None:
            raise ValueError("Missing Precincts File")
        voter_files = list(filter(lambda v: re.search('cty[0-9]+_vr.csv', v["name"].lower()), new_files))
        self.file_check(len(voter_files) + 1)
        hist_files = list(filter(lambda v: re.search('cty[0-9]+_vh.csv', v["name"].lower()), new_files))
        vdf = pd.DataFrame()
        hdf = pd.DataFrame()
        dtypes = self.config['dtypes']
        cty_map = dict([(value, key) for key, value in self.config['county_codes'].items()])

        # Returns the string county name for the county code contained in the first two characters of the precicnct string
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

        # Read and merge the precincts file to the main df
        precinct_dtypes = {'PrecinctCode': 'string', 'CongressionalDistrict': 'int64', 'StateSenateDistrict': 'int64', 
                           'StateHouseDistrict': 'int64', 'CountyCommissioner': 'int64', 'PollSite': 'string'}
        precinct_ints = [k for k,v in precinct_dtypes.items() if v == "int64"]
        precincts = pd.read_csv(precincts_file["obj"], encoding='latin')
        precincts.dropna(subset=precinct_ints, inplace=True)
        for k, v in precinct_dtypes.items():
            precincts[k] = precincts[k].astype(v)
        precincts.rename(columns={"PrecinctCode": "Precinct"}, inplace=True)
        if precincts.empty:
            raise ValueError("Missing Precicnts file")
        vdf = vdf.merge(precincts, how='left', on='Precinct')

        # Add the county column
        vdf['County'] = county_map(vdf['Precinct'])

        # At one point OK added some columns, this adds them to older files for backwards compatibility
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

        # The hist columns in the vdf are unecessary because we get a separate hist file that is more complete.
        hist_columns = [col for col in vdf.columns if "voterhist" in col.lower() or "histmethod" in col.lower()]
        vdf = self.config.coerce_numeric(vdf)
        vdf = self.config.coerce_strings(vdf)
        vdf = self.config.coerce_dates(vdf)
        vdf.drop(hist_columns, inplace=True)
        vdf.set_index(self.config["voter_id"], drop=False, inplace=True)
        voter_groups = hdf.groupby(self.config["voter_id"])
        vdf["all_history"] = voter_groups["ElectionDate"].apply(list)
        vdf["sparse_history"] = voter_groups["array_position"].apply(list)
        vdf["votetype_history"] = voter_groups["VotingMethod"].apply(list)

        # Check the file for all the proper locales
        self.locale_check(
            set(vdf[self.config["primary_locale_identifier"]]),
        )

        self.meta = {
            "message": "oklahoma_{}".format(datetime.now().isoformat()),
            "array_encoding": json.dumps(sorted_codes_dict),
            "array_decoding": json.dumps(sorted_codes),
        }

        self.processed_file = FileItem(
            name="{}.processed".format(self.config["state"]),
            io_obj=StringIO(vdf.to_csv(encoding="utf-8", index=False)),
            s3_bucket=self.s3_bucket,
        )
