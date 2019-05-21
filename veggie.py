from configs.configs import Config
from download import Preprocessor
import datetime


def convert_voter_file(state=None, local_file=None,
                       date=None, write_file=True):
    config_file = Config.config_file_from_state(state)
    date = str(datetime.datetime.strptime(date, '%Y-%m-%d').date())
    with Preprocessor(local_file,
                      config_file,
                      date) as preprocessor:
        preprocessor.execute()
        if not write_file:
        	return(preprocessor.dataframe, preprocessor.meta)
        preprocessor.dump()
