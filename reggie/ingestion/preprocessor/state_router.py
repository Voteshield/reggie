import logging

from reggie.ingestion.download import Preprocessor
from reggie.ingestion.preprocessor.iowa_preprocessor import PreprocessIowa
from reggie.ingestion.preprocessor.arizona2_preprocessor import (
    PreprocessArizona2,
)
from reggie.ingestion.preprocessor.colorado_preprocessor import (
    PreprocessColorado,
)
from reggie.ingestion.preprocessor.michigan_preprocessor import (
    PreprocessMichigan,
)
from reggie.ingestion.preprocessor.ohio_preprocessor import PreprocessOhio

# Check the function paramaters here, some might not need to be set here or need better names


class StateRouter:
    def __init__(
        self,
        state,
        raw_s3_file,
        config_file,
        force_date=None,
        force_file=None,
        testing=False,
        ignore_checks=False,
        s3_bucket="",
        **kwargs
    ):
        print(config_file)
        self.state = state
        self.raw_s3_file = raw_s3_file
        self.config_file = config_file
        self.force_date = force_date
        self.force_file = force_file
        self.testing = testing
        self.ignore_checks = ignore_checks
        self.s3_bucket = s3_bucket
        self.routes = {
            "arizona2": PreprocessArizona2,
            "colorado": PreprocessColorado,
            "iowa": PreprocessIowa,
            "ohio": PreprocessOhio,
            "michigan": PreprocessMichigan,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def execute(self, **kwargs):
        if self.state in self.routes:
            # F is now some typs of class object?
            logging.info(
                "state: {}, config: {}, main file: {}".format(
                    self.state, self.config_file, self.raw_s3_file
                )
            )
            f = self.routes[self.state](
                raw_s3_file=self.raw_s3_file,
                config_file=self.config_file,
                force_date=self.force_date,
                force_file=self.force_file,
                testing=self.testing,
                ignore_checks=self.ignore_checks,
                s3_bucket=self.s3_bucket,
                **kwargs
            )
            logging.info("preprocessing {}".format(self.state))
            f.main_file = f.processed_file
            f.compress()
            return f
        else:
            raise NotImplementedError(
                "preprocess_{} has not yet been "
                "implemented for the Preprocessor object".format(self.state)
            )
