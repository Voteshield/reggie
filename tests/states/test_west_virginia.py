import pytest

# Dependencies for testings
import os
import datetime
import json
import pandas as pd

# Dependencies to test
from reggie import convert_voter_file
from reggie.ingestion.preprocessor.west_virginia_preprocessor import (
    VOTER_FILE_REGEX,
    VOTER_HISTORY_REGEX,
)


@pytest.mark.parametrize(
    "str_to_test, expected",
    [
        ("WV PStatewide_VH.txt", None),
        ("Statewide_VRTEST.txt", True),
        (".Statewide_VRTEST.txt", None),
        ("._Statewide_VRTEST.txt", None),
        ("WV 2019-03-14.txt", True),
    ],
)
def test_wv_preprocessor_voter_regex(str_to_test, expected):
    """
    Tests that the West Virginia regexes for finding voter
    files works as expected.
    """
    if expected == True:
        assert VOTER_FILE_REGEX.match(str_to_test) is not None
    else:
        VOTER_FILE_REGEX.match(str_to_test) == expected


@pytest.mark.parametrize(
    "str_to_test, expected",
    [
        ("WV PStatewide_VH.txt", True),
        ("WV StatewideVH030121.txt", True),
        (".Statewide_VH.txt", None),
        ("._WV Statewide_VH.txt", None),
        ("Statewide_VRTEST.txt", None),
        ("WV 2019-03-14.txt", None),
    ],
)
def test_wv_preprocessor_history_regex(str_to_test, expected):
    """
    Tests that the West Virginia regexes for finding voter
    files works as expected.
    """
    if expected == True:
        assert VOTER_HISTORY_REGEX.match(str_to_test) is not None
    else:
        VOTER_HISTORY_REGEX.match(str_to_test) == expected


def test_wv_preprocessor_basic():
    """
    Tests that the West Virginia preprocessor produces the data
    as expected.
    """
    test_data_directory = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "data",
        "west_virginia",
        "basic",
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
        "gender",
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
        "sparse_history",
    ]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check voter ids
    actual_list = df_processed.index.values.tolist()
    expected_list = ["000000001", "000000002", "000000003", "000000004"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check county ids
    actual_list = list(df_processed["County_ID"])
    expected_list = ["barbour", "barbour", "berkeley", "jackson"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check raw county names
    actual_list = list(df_processed["County_Name"])
    expected_list = ["BARBOUR", "BARBOUR", "BERKELEY", "JACKSON"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check first names
    actual_list = list(df_processed["FIRST NAME"])
    expected_list = ["first01", "first02", "first03", "no history"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check gender
    actual_list = list(df_processed["gender"])
    expected_list = ["male", "female", "female", "male"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check party
    actual_list = list(df_processed["PartyAffiliation"])
    expected_list = ["independent", "unaffiliated", "republican", "unknown"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check status
    actual_list = list(df_processed["Status"])
    expected_list = ["active", "active", "inactive", "active"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check congressional districts
    actual_list = list(df_processed["Congressional District"])
    expected_list = ["1", "1", "1", "2"]
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
            "33333",
        ],
        [],
    ]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check history sparse index
    actual_list = list(df_processed["sparse_history"])
    expected_list = [
        [2, 1, 0, 3, 4, 7, 8, 10, 16, 18, 20, 21, 19, 22],
        [2, 1, 0, 4, 7, 8, 10, 18, 21],
        [2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 20, 21, 22, 23],
        [],
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
            "absentee",
        ],
        [],
    ]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check challenged history
    actual_list = list(df_processed["challenged_history"])
    expected_list = [
        [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            True,
            False,
            False,
            False,
            False,
        ],
        [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
        [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
        ],
        [],
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
        "1009": {"index": 5, "count": 2, "date": "05/14/2002"},
        "1010": {"index": 6, "count": 2, "date": "11/05/2002"},
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
        "33333": {"index": 23, "count": 1, "date": ""},
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
        "33333",
    ]
    actual_list = preprocessor.meta["array_decoding"]
    assert actual_list == json.dumps(expected_list)


def test_wv_preprocessor_no_gender():
    """
    Tests that the West Virginia preprocessor produces the data
    as expected.
    """
    test_data_directory = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "data",
        "west_virginia",
        "no_gender",
        "WV-TEST-no-gender.zip",
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
        "gender",
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
        "sparse_history",
    ]
    assert len(actual_list) == len(expected_list)
    print(actual_list)
    print(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])

    # Check gender
    actual_list = list(df_processed["gender"])
    expected_list = ["unknown", "unknown", "unknown", "unknown"]
    assert len(actual_list) == len(expected_list)
    assert all([a == b for a, b in zip(actual_list, expected_list)])
