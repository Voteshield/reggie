from configs.configs import Config
import requests
from bs4 import BeautifulSoup
import urllib2
import datetime
import zipfile
import boto3
from ingestion.download import Loader
from constants import RAW_FILE_PREFIX
from selenium import webdriver


def state_download(state):

	#config_file = Config.config_file_from_state(state=state)
	#configs = Config(file_name=config_file)
	today = datetime.datetime.now().strftime("%Y-%m-%d")

	if state == "north_carolina":
		list_files = configs['data_chunk_links']
		zipped_files = []
		for i, url in enumerate(list_files):
			print(url)
			target_path = "/tmp/" + state + str(i) + ".zip"
			print(target_path)
			zipped_files.append(target_path)
			print(zipped_files)
			response = requests.get(url, stream=True)
			handle = open(target_path, "wb")
			for chunk in response.iter_content(chunk_size=512):
			    if chunk:  # filter out keep-alive new chunks
			        handle.write(chunk)
			handle.close()

		file_to_zip = today + ".zip"
		with zipfile.ZipFile(file_to_zip, 'w') as myzip:
			for f in zipped_files:
				myzip.write(f)
		with Loader(config_file=config_file, force_date=today,
                 force_file=file_to_zip) as loader:
			loader.s3_dump(file_class=RAW_FILE_PREFIX)


	if state == "practice":
		date_grab()



def date_grab():
	browser = webdriver.Chrome()
	url = "http://example.com/login.php"
	browser.get(url)
	quote_page = "https://dl.ncsbe.gov/?prefix=data/"
	headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3"}
	r = requests.get(quote_page, headers=headers).content

	soup = BeautifulSoup(r, "html.parser")
	print(soup)






