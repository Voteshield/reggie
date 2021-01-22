import logging
from reggie.configs.configs import Config
from .arizona_preprocessor import PreprocessArizona
from .arizona2_preprocessor import PreprocessArizona2
from .colorado_preprocessor import PreprocessColorado
from .florida_preprocessor import PreprocessFlorida
from .georgia_preprocessor import PreprocessGeorgia
from .iowa_preprocessor import PreprocessIowa
from .kansas_preprocessor import PreprocessKansas
from .michigan_preprocessor import PreprocessMichigan
from .minnesota_preprocessor import PreprocessMinnesota
from .missouri_preprocessor import PreprocessMissouri
from .nevada_preprocessor import PreprocessNevada
from .new_hampshire_preprocessor import PreprocessNewHampshire
from .new_jersey_preprocessor import PreprocessNewJersey
from .new_jersey2_preprocessor import PreprocessNewJersey2
from .new_york_preprocessor import PreprocessNewYork
from .north_carolina_preprocessor import PreprocessNorthCarolina
from .ohio_preprocessor import PreprocessOhio
from .pennsylvania_preprocessor import PreprocessPennsylvania
from .texas_preprocessor import PreprocessTexas
from .virginia_preprocessor import PreprocessVirginia
from .wisconsin_preprocessor import PreprocessWisconsin


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
    routes = {
        "arizona": PreprocessArizona,
        "arizona2": PreprocessArizona2,
        "colorado": PreprocessColorado,
        "florida": PreprocessFlorida,
        "georgia": PreprocessGeorgia,
        "iowa": PreprocessIowa,
        "kansas": PreprocessKansas,
        "michigan": PreprocessMichigan,
        "minnesota": PreprocessMinnesota,
        "missouri": PreprocessMissouri,
        "nevada": PreprocessNevada,
        "new_hampshire": PreprocessNewHampshire,
        "new_jersey": PreprocessNewJersey,
        "new_jersey2": PreprocessNewJersey2,
        "new_york": PreprocessNewYork,
        "north_carolina": PreprocessNorthCarolina,
        "ohio": PreprocessOhio,
        "pennsylvania": PreprocessPennsylvania,
        "texas": PreprocessTexas,
        "virginia": PreprocessVirginia,
        "wisconsin": PreprocessWisconsin,
    }
    if state in routes:
        f = routes[state](
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            force_file=force_file,
            testing=testing,
            ignore_checks=ignore_checks,
            s3_bucket=s3_bucket,
            **kwargs
        )
        # logging.info("preprocessing {}".format(state))
        return f
    else:
        raise NotImplementedError(
            "preprocess_{} has not yet been "
            "implemented for the Preprocessor object".format(state)
        )


# class StateRouter:
#     def __init__(
#         self,
#         state,
#         raw_s3_file,
#         config_file,
#         force_date=None,
#         force_file=None,
#         testing=False,
#         ignore_checks=False,
#         s3_bucket="",
#         **kwargs
#     ):
#         self.raw_s3_file = raw_s3_file
#         self.config_file = config_file
#         config = Config(file_name=config_file)
#         self.state = state if state is not None else config["state"]
#         self.force_date = force_date
#         self.force_file = force_file
#         self.testing = testing
#         self.ignore_checks = ignore_checks
#         self.s3_bucket = s3_bucket
#         self.routes = {
#             "arizona": PreprocessArizona,
#             "arizona2": PreprocessArizona2,
#             "colorado": PreprocessColorado,
#             "florida": PreprocessFlorida,
#             "georgia": PreprocessGeorgia,
#             "iowa": PreprocessIowa,
#             "kansas": PreprocessKansas,
#             "michigan": PreprocessMichigan,
#             "minnesota": PreprocessMinnesota,
#             "missouri": PreprocessMissouri,
#             "nevada": PreprocessNevada,
#             "new_hampshire": PreprocessNewHampshire,
#             "new_jersey": PreprocessNewJersey,
#             "new_jersey2": PreprocessNewJersey2,
#             "new_york": PreprocessNewYork,
#             "north_carolina": PreprocessNorthCarolina,
#             "ohio": PreprocessOhio,
#             "pennsylvania": PreprocessPennsylvania,
#             "texas": PreprocessTexas,
#             "virginia": PreprocessVirginia,
#             "wisconsin": PreprocessWisconsin,
#         }
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         return
#
#     def execute(self, **kwargs):
#         if self.state in self.routes:
#             f = self.routes[self.state](
#                 raw_s3_file=self.raw_s3_file,
#                 config_file=self.config_file,
#                 force_date=self.force_date,
#                 force_file=self.force_file,
#                 testing=self.testing,
#                 ignore_checks=self.ignore_checks,
#                 s3_bucket=self.s3_bucket,
#                 **kwargs
#             )
#             logging.info("preprocessing {}".format(self.state))
#             return f
#         else:
#             raise NotImplementedError(
#                 "preprocess_{} has not yet been "
#                 "implemented for the Preprocessor object".format(self.state)
#             )
