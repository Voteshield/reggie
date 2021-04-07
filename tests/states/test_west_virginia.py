import pytest

# Dependencies for testings
import os
import pandas as pd

# Dependencies to test
from reggie import convert_voter_file


def test_wv_preprocessor():
    """
    Basic smoke test to make sure the parts are there.
    """
    test_data_directory = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "data",
        "west_virginia",
        "WV-TEST.zip",
    )

    _, _, preprocessor = convert_voter_file(
        state="west_virginia",
        local_file=test_data_directory,
        file_date="2020-01-01",
        write_file=False,
    )
    df_processed = preprocessor.processed_df

    # Check voter ids
    voter_id_list = df_processed.index.values.tolist()
    expected_voter_id_list = ["000000001", "000000002", "000000003"]
    assert len(voter_id_list) == len(expected_voter_id_list)
    assert all([a == b for a, b in zip(voter_id_list, expected_voter_id_list)])
