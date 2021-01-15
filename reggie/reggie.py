from reggie.configs.configs import Config
from reggie.ingestion.preprocessor.state_router import StateRouter
import datetime
import click


def convert_voter_file(state=None, local_file=None,
                       file_date=None, write_file=False):
    config_file = Config.config_file_from_state(state)
    file_date = str(datetime.datetime.strptime(file_date, '%Y-%m-%d').date())
    with StateRouter(None, raw_s3_file=None,
                     config_file=config_file,
                     force_file=local_file,
                     force_date=file_date) as preprocessor:
        file_item = preprocessor.execute()
        if not write_file:
            return(preprocessor.output_dataframe(file_item),
                   preprocessor.meta)
        preprocessor.local_dump(file_item)


@click.command(help="convert a non-standard voter file")
@click.option("--state", required=True, default=None,
              help="U.S. state name: e.g. florida")
@click.option("--local_file", required=True, default=None,
              help="location and name of file: e.g. 'FL_2019-01-01.zip'")
@click.option("--file_date", required=True,
              default=None,
              help="date of voter file in format 'YYYY-MM-DD'")
@click.option("--write_file", required=False, default=True, is_flag=True)
def convert_cli(state, local_file, file_date, write_file):
    if file_date is None:
        file_date = datetime.datetime.today().date().isoformat()
    convert_voter_file(state=state, local_file=local_file,
                       file_date=file_date, write_file=write_file)
