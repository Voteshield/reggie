from reggie.configs.configs import Config
from reggie.ingestion.download import Preprocessor
import datetime
import click


def convert_voter_file(state=None, local_file=None, file_date=None):
    config_file = Config.config_file_from_state(state)
    file_date = str(datetime.datetime.strptime(file_date, '%Y-%m-%d').date())
    with Preprocessor(None,
                      config_file,
                      force_file=local_file,
                      force_date=file_date) as preprocessor:
        file_item = preprocessor.execute()
        preprocessor.main_file = file_item
        preprocessor.dump(file_item=preprocessor.main_file)


@click.command(help="convert a non-standard voter file")
@click.option("--state", required=True, default=None,
              help="U.S. state name: e.g. florida")
@click.option("--local_file", required=True, default=None,
              help="location and name of file: e.g. 'FL_2019-01-01.zip'")
@click.option("--file_date", required=True,
              default=None,
              help="date of voter file in format 'YYYY-MM-DD'")
def convert_cli(state, local_file, file_date):
    if file_date is None:
        file_date = datetime.datetime.today().date().isoformat()
    convert_voter_file(state=state, local_file=local_file, file_date=file_date)
