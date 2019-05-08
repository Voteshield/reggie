import click
from convert import convert
from configs.configs import Config
from ingestion.download import Loader, Preprocessor


@click.group()
def cli():
    pass

@cli.command(help="convert a non-standard voter file")
@click.option("--state", required=True, help="US state name")
@click.option("--file_path", required=True, help="location of file")
@click.option("--destination_path", required=False, help="destination of resulting voter file")
def convert_voter_file(state=None, file_path=None, destination_path=None, s3_key=None):
    config_file = Config.config_file_from_state(state)

    with Preprocessor(s3_key,
                      config_file,
                      testing=testing) as preprocessor:

        file_item = preprocessor.execute()
        preprocessor.main_file = file_item
        preprocessor.compress()
        preprocessor.s3_dump(file_item=preprocessor.main_file)
    print(config_file)
    print_hello()


if __name__ == '__main__':
    cli()
