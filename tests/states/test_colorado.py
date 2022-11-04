import pytest
import os

from reggie.configs.configs import Config
from reggie.ingestion.preprocessor.state_router import state_router


def test_colorado_pkg_unzipping():
    """
    Tests that a fake Colorado file package will
    unzip with the correct file names and paths.
    """
    test_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "data",
        "colorado",
        "fake_colorado_file_package.zip",
    )

    state = "colorado"
    preprocessor = state_router(
        state,
        raw_s3_file=None,
        config_file=Config.config_file_from_state(state),
        force_file=test_file,
        force_date="2022-01-01",
    )
    new_files = preprocessor.unpack_files(
        compression="unzip", file_obj=preprocessor.main_file
    )

    # Test that the unzip function correctly produces 2 files,
    # with their full path names:
    assert new_files[0]["name"] == "/fake_colorado_file_package/EX-002 Voting History Files/Fake_History_File.txt.gz"
    assert new_files[1]["name"] == "/fake_colorado_file_package/EX-003 Master Voter List/Fake_Registered_Voters_List_ Part1.txt"
