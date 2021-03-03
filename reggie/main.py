"""
Main Reggie file that provides the method to convert voter files.
"""

from reggie.configs.configs import Config
from reggie.ingestion.preprocessor.state_router import state_router
import datetime


def convert_voter_file(state=None, local_file=None, file_date=None, write_file=False):
    config_file = Config.config_file_from_state(state)
    file_date = str(datetime.datetime.strptime(file_date, "%Y-%m-%d").date())
    preprocessor = state_router(
        state,
        raw_s3_file=None,
        config_file=config_file,
        force_file=local_file,
        force_date=file_date,
    )
    preprocessor.execute()
    if not write_file:
        return (
            preprocessor.output_dataframe(preprocessor.processed_file),
            preprocessor.meta,
        )
    preprocessor.local_dump(preprocessor.processed_file)
