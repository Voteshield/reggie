from configs.configs import Config
def state_download(state):

	config_file = Config.config_file_from_state(state=state)
	configs = Config(file_name=config_file)
	if state == "north_carolina":
		list_files = configs['data_chunk_links']
		print(list_files)
