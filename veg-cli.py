"exec" "`dirname $(dirname $(readlink -f \"$0\"))`/venv/bin/python" "$0" "$@"

import click
from configs.configs import Config
from download import Preprocessor
import datetime

@click.group()
def cli():
    pass

@cli.command(help="convert a non-standard voter file")
@click.option("--state", required=True, help="US state name")
@click.option("--local_file", required=True, help="location of file")
@click.option("--date", required=True, help="date of voter file in format 'YYYY-MM-DD'")
def convert_voter_file(state=None, local_file=None, date=None):
    config_file = Config.config_file_from_state(state)
    date = str(datetime.datetime.strptime(date, '%Y-%m-%d').date())

    with Preprocessor(local_file,
                      config_file,
                      date) as preprocessor:
        preprocessor.execute()
        preprocessor.dump()


if __name__ == '__main__':
    cli()
