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

    # Check column names
    # ?? What about ID_VOTER (it's an index, which isn't a column?)
    actual_list = df_processed.columns.values.tolist()
    expected_list = [
        "County_ID",
        "County_Name",
        "FIRST NAME",
        "Mid",
        "LAST NAME",
        "Suffix",
        "DATE OF BIRTH",
        "SEX",
        "HOUSE NO",
        "STREET",
        "STREET2",
        "UNIT",
        "CITY",
        "STATE",
        "ZIP",
        "MAIL HOUSE NO",
        "MAIL STREET",
        "MAIL STREET2",
        "MAIL UNIT",
        "MAIL CITY",
        "MAIL STATE",
        "MAIL ZIP",
        "REGISTRATION DATE",
        "PartyAffiliation",
        "Status",
        "Congressional District",
        "Senatorial District",
        "Delegate District",
        "Magisterial District",
        "Precinct_Number",
        "POLL_NAME",
        "all_history",
        "votetype_history",
        "challenged_history",
    ]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check voter ids
    actual_list = df_processed.index.values.tolist()
    expected_list = ["000000001", "000000002", "000000003"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check first names
    actual_list = list(df_processed["FIRST NAME"])
    expected_list = ["FIRST01", "FIRST02", "FIRST03"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check party
    actual_list = list(df_processed["PartyAffiliation"])
    expected_list = ["independent", "unaffiliated", "republican"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check status
    actual_list = list(df_processed["Status"])
    expected_list = ["active", "active", "inactive"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check congressional districts
    actual_list = list(df_processed["Congressional District"])
    expected_list = ["1", "1", "1"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check history index
    actual_list = list(df_processed["all_history"])
    expected_list = [
        [
            "1",
            "100",
            "1000",
            "1001",
            "1002",
            "1003",
            "1004",
            "1005",
            "10000",
            "10001",
            "10002",
            "10003",
            "10004",
            "10005",
        ],
        ["1", "100", "1000", "1000", "1003", "1004", "1005", "10001", "10003"],
        [
            "1",
            "1001",
            "1002",
            "1009",
            "1010",
            "1003",
            "1004",
            "20000",
            "20001",
            "20002",
            "20003",
            "20004",
            "20005",
            "20006",
            "10002",
            "10003",
            "10005",
        ],
    ]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check votetype history
    actual_list = list(df_processed["votetype_history"])
    expected_list = [
        [
            "unknown",
            "unknown",
            "unknown",
            "unknown",
            "unknown",
            "regular",
            "early",
            "regular",
            "regular",
            "early",
            "regular",
            "regular",
            "regular",
            "regular",
        ],
        [
            "unknown",
            "unknown",
            "unknown",
            "unknown",
            "regular",
            "regular",
            "regular",
            "regular",
            "regular",
        ],
        [
            "unknown",
            "unknown",
            "unknown",
            "unknown",
            "unknown",
            "regular",
            "regular",
            "regular",
            "regular",
            "regular",
            "early",
            "regular",
            "regular",
            "early",
            "regular",
            "early",
            "absentee",
        ],
    ]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check challenged history
    actual_list = list(df_processed["challenged_history"])
    expected_list = [
        [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            True,
            None,
            None,
            None,
            None,
        ],
        [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            False,
            None,
            False,
            None,
        ],
    ]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])
