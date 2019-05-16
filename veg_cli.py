"exec" "`dirname $(dirname $(readlink -f \"$0\"))`/venv/bin/python" "$0" "$@"

import click
from configs.configs import Config
from download import Preprocessor
import datetime

@click.group()
def cli():
    pass

@cli.command(help="convert a non-standard voter file")
@click.option("--state", required=True, default=None, help="U.S. state name: e.g. florida")
@click.option("--local_file", required=True, default=None, help="location and name of file: e.g. 'FL_2019-01-01.zip'")
@click.option("--date", required=True, default=datetime.datetime.today().date().isoformat(), help="date of voter file in format 'YYYY-MM-DD'")
@click.option("--write_file", required=False, default=True, is_flag=True)
def convert_voter_file(state=None, local_file=None, date=None, write_file=True):
    config_file = Config.config_file_from_state(state)
    date = str(datetime.datetime.strptime(date, '%Y-%m-%d').date())
    print(date)
    with Preprocessor(local_file,
                      config_file,
                      date) as preprocessor:
        preprocessor.execute()
        if not write_file:
        	return(preprocessor.dataframe, preprocessor.meta)
        preprocessor.dump()


if __name__ == '__main__':
    cli()
