import pytest

# Dependencies for testings
import os
import datetime
import json
import pandas as pd

# Dependencies to test
from reggie import convert_voter_file


def test_wv_preprocessor():
    """
    Tests that the West Virginia preprocessor produces the data
    as expected.
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
    actual_list = df_processed.columns.values.tolist()
    expected_list = [
        "ID_VOTER",
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

    # Check county ids
    actual_list = list(df_processed["County_ID"])
    expected_list = ["barbour", "barbour", "berkeley"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check raw county names
    actual_list = list(df_processed["County_Name"])
    expected_list = ["BARBOUR", "BARBOUR", "BERKELEY"]
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
        ["1", "100", "1000", "1002", "1003", "1004", "1005", "10001", "10003"],
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

    # Check meta data message
    now = datetime.datetime.now()
    year = now.strftime("%Y")
    assert preprocessor.meta["message"].startswith(f"west_virginia_{year}")

    # Check meta data array_encoding
    expected_dict = {
        "1000": {"index": 0, "count": 2, "date": "05/14/1996"},
        "100": {"index": 1, "count": 2, "date": "11/05/1996"},
        "1": {"index": 2, "count": 3, "date": "03/18/2000"},
        "1001": {"index": 3, "count": 2, "date": "05/09/2000"},
        "1002": {"index": 4, "count": 3, "date": "11/07/2000"},
        "1009": {"index": 5, "count": 1, "date": "05/14/2002"},
        "1010": {"index": 6, "count": 1, "date": "11/05/2002"},
        "1003": {"index": 7, "count": 3, "date": "05/11/2004"},
        "1004": {"index": 8, "count": 3, "date": "11/02/2004"},
        "20000": {"index": 9, "count": 1, "date": "06/14/2005"},
        "1005": {"index": 10, "count": 2, "date": "06/25/2005"},
        "20001": {"index": 11, "count": 1, "date": "11/07/2006"},
        "20002": {"index": 12, "count": 1, "date": "07/14/2007"},
        "20003": {"index": 13, "count": 1, "date": "05/13/2008"},
        "20004": {"index": 14, "count": 1, "date": "06/14/2011"},
        "20005": {"index": 15, "count": 1, "date": "01/21/2012"},
        "10000": {"index": 16, "count": 1, "date": "05/10/2016"},
        "20006": {"index": 17, "count": 1, "date": "05/10/2016"},
        "10001": {"index": 18, "count": 2, "date": "11/08/2016"},
        "10004": {"index": 19, "count": 1, "date": "10/07/2017"},
        "10002": {"index": 20, "count": 2, "date": "05/08/2018"},
        "10003": {"index": 21, "count": 3, "date": "11/06/2018"},
        "10005": {"index": 22, "count": 2, "date": "06/09/2020"},
    }
    assert preprocessor.meta["array_encoding"] == json.dumps(expected_dict)

    # Check meta data array_decoding
    expected_list = [
        "1000",
        "100",
        "1",
        "1001",
        "1002",
        "1009",
        "1010",
        "1003",
        "1004",
        "20000",
        "1005",
        "20001",
        "20002",
        "20003",
        "20004",
        "20005",
        "10000",
        "20006",
        "10001",
        "10004",
        "10002",
        "10003",
        "10005",
    ]
    actual_list = preprocessor.meta["array_decoding"]
    assert actual_list == json.dumps(expected_list)