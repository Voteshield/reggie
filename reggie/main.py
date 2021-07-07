"""
Main Reggie file that provides the method to convert voter files.
"""

from reggie.configs.configs import Config
from reggie.ingestion.preprocessor.state_router import state_router
import datetime


def convert_voter_file(state=None, local_file=None, file_date=None, write_file=False):
    """Main Reggie function; processes a voter file, which is often more than one file, so will likely be a compressed file such as a .zip file.

    Parameters
    ----------
    state : string, optional
        State identifier which is the lower case version of the state name with underscores replacing spaces, by default None
    local_file : string, optional
        Path to file to process, by default None
    file_date : string, optional
        The snapshot date in format "YYYY-MM-DD", by default None
    write_file : bool, optional
        Whether to write the file out into a CSV file, which will be automatically named and write to the local directory, by default False

    Returns
    -------
    tuple
        If `write_file` is falsey, this function will return a tuple with the following objects:
            - The processed voter file as a CSV string
            - The meta data object
            - The preprocessor object
    """
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
            preprocessor,
        )
    preprocessor.local_dump(preprocessor.processed_file)
