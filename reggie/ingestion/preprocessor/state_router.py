import logging
from reggie.configs.configs import Config
from .arizona_preprocessor import PreprocessArizona
from .arizona2_preprocessor import PreprocessArizona2
from .california_preprocessor import PreprocessCalifornia
from .colorado_preprocessor import PreprocessColorado
from .florida_preprocessor import PreprocessFlorida
from .georgia_preprocessor import PreprocessGeorgia
from .iowa_preprocessor import PreprocessIowa
from .kansas_preprocessor import PreprocessKansas
from .maine_preprocessor import PreprocessMaine
from .michigan_preprocessor import PreprocessMichigan
from .minnesota_preprocessor import PreprocessMinnesota
from .missouri_preprocessor import PreprocessMissouri
from .nebraska_preprocessor import PreprocessNebraska
from .nevada_preprocessor import PreprocessNevada
from .new_hampshire_preprocessor import PreprocessNewHampshire
from .new_jersey_preprocessor import PreprocessNewJersey
from .new_jersey2_preprocessor import PreprocessNewJersey2
from .new_york_preprocessor import PreprocessNewYork
from .north_carolina_preprocessor import PreprocessNorthCarolina
from .ohio_preprocessor import PreprocessOhio
from .oklahoma_preprocessor import PreprocessOklahoma
from .pennsylvania_preprocessor import PreprocessPennsylvania
from .texas_preprocessor import PreprocessTexas
from .vermont_preprocessor import PreprocessVermont
from .virginia_preprocessor import PreprocessVirginia
from .washington_preprocessor import PreprocessWashington
from .west_virginia_preprocessor import PreprocessWestVirginia
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
        "california": PreprocessCalifornia,
        "colorado": PreprocessColorado,
        "florida": PreprocessFlorida,
        "georgia": PreprocessGeorgia,
        "iowa": PreprocessIowa,
        "kansas": PreprocessKansas,
        "maine": PreprocessMaine,
        "michigan": PreprocessMichigan,
        "minnesota": PreprocessMinnesota,
        "missouri": PreprocessMissouri,
        "nebraska": PreprocessNebraska,
        "nevada": PreprocessNevada,
        "new_hampshire": PreprocessNewHampshire,
        "new_jersey": PreprocessNewJersey,
        "new_jersey2": PreprocessNewJersey2,
        "new_york": PreprocessNewYork,
        "north_carolina": PreprocessNorthCarolina,
        "ohio": PreprocessOhio,
        "oklahoma": PreprocessOklahoma,
        "pennsylvania": PreprocessPennsylvania,
        "texas": PreprocessTexas,
        "vermont": PreprocessVermont,
        "virginia": PreprocessVirginia,
        "washington": PreprocessWashington,
        "west_virginia": PreprocessWestVirginia,
        "wisconsin": PreprocessWisconsin,
    }
    if state in routes:
        state_preprocessor = routes[state](
            raw_s3_file=raw_s3_file,
            config_file=config_file,
            force_date=force_date,
            force_file=force_file,
            testing=testing,
            ignore_checks=ignore_checks,
            s3_bucket=s3_bucket,
            **kwargs
        )
        return state_preprocessor
    else:
        raise NotImplementedError(
            "preprocess_{} has not yet been "
            "implemented for the Preprocessor object".format(state)
        )
