import click
from configs.configs import Config
from download import Preprocessor


@click.group()
def cli():
    pass

@cli.command(help="convert a non-standard voter file")
@click.option("--state", required=True, help="US state name")
@click.option("--local_file", required=True, help="location of file")
@click.option("--out_file", required=True, help="destination of resulting voter file")
def convert_voter_file(state=None, local_file=None, out_file=None):
    config_file = Config.config_file_from_state(state)

    with Preprocessor(local_file,
                      config_file,
                      out_file) as preprocessor:
        preprocessor.execute()


if __name__ == '__main__':
    cli()
