import logging

from reggie.ingestion.download import Preprocessor
from reggie.ingestion.preprocessor.iowa_preprocessor import PreprocessIowa
from reggie.ingestion.preprocessor.arizona2_preprocessor import (
    PreprocessArizona2,
)
from reggie.ingestion.preprocessor.colorado_preprocessor import (
    PreprocessColorado,
)
from reggie.ingestion.preprocessor.michigan_preprocessor import PreprocessMichigan

# Check the function paramaters here, some might not need to be set here or need better names
# Need to pull out: Alaska


def state_router(
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
    # if state == 'iowa':
    #     f = PreprocessIowa(raw_s3_file=raw_s3_file, config_file=config_file, force_date=None, force_file=None,
    #              testing=False, ignore_checks=False, s3_bucket=s3_bucket, **kwargs)
    #     print(f.processed_file)
    print("before routes in state_router: ", state, raw_s3_file, config_file)

    # (
    #     raw_s3_file=raw_s3_file,
    # config_file=config_file,
    # force_date=None,
    # force_file=None,
    # testing=False,
    # ignore_checks=False,
    # s3_bucket=s3_bucket,
    # ** kwargs
    # ),
    routes = {
        "arizona2": PreprocessArizona2,
        "colorado": PreprocessColorado,
        "iowa": PreprocessIowa,
        "michigan": PreprocessMichigan,
    }
    if state in routes:
        # F is now some typs of class object?
        logging.info(
            "state: {}, config: {}, main file: {}".format(
                state, config_file, raw_s3_file
            )
        )
        f = routes[state](
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=None,
            force_file=None,
            testing=False,
            ignore_checks=False,
            s3_bucket=s3_bucket,
            **kwargs
        )
        logging.info("preprocessing {}".format(state))
        return f
    else:
        raise NotImplementedError(
            "preprocess_{} has not yet been "
            "implemented for the Preprocessor object".format(state)
        )
