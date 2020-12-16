from reggie.ingestion.download import Preprocessor
from reggie.ingestion.preprocessor.iowa_preprocessor import PreprocessIowa


#needs some type of driver file to avoid circular imports

def state_router(state, raw_s3_file, config_file, force_date=None, force_file=None,
                 testing=False, ignore_checks=False, s3_bucket="", **kwargs):
    if state == 'iowa':
        f = PreprocessIowa(raw_s3_file=raw_s3_file, config_file=config_file, force_date=None, force_file=None,
                 testing=False, ignore_checks=False, s3_bucket=s3_bucket, **kwargs)
        print(f.processed_file)
    return f
