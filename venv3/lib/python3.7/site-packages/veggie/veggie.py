from veggie.configs.configs import Config
from veggie.download import Preprocessor
import datetime
import click


def convert_voter_file(state=None, local_file=None,
                       date=None, write_file=True):
    config_file = Config.config_file_from_state(state)
    date = str(datetime.datetime.strptime(date, '%Y-%m-%d').date())
    with Preprocessor(local_file,
                      config_file,
                      date) as preprocessor:
        preprocessor.execute()
        if not write_file:
            return(preprocessor.dataframe, preprocessor.meta)
        preprocessor.dump()


@click.command(help="convert a non-standard voter file")
@click.option("--state", required=True, default=None,
              help="U.S. state name: e.g. florida")
@click.option("--local_file", required=True, default=None,
              help="location and name of file: e.g. 'FL_2019-01-01.zip'")
@click.option("--date", required=True,
              default=None,
              help="date of voter file in format 'YYYY-MM-DD'")
@click.option("--write_file", required=False, default=True, is_flag=True)
def convert_cli(state, local_file, date, write_file):
    if date is None:
        date = datetime.datetime.today().date().isoformat()
    convert_voter_file(state=state, local_file=local_file,
                       date=date, write_file=write_file)

