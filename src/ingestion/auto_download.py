from configs.configs import Config
import requests
from bs4 import BeautifulSoup
import xmltodict
import urllib2
import datetime
import zipfile
import boto3
from ingestion.download import Loader
from constants import RAW_FILE_PREFIX
import xml.etree.ElementTree
import logging





def state_download(state):

	config_file = Config.config_file_from_state(state=state)
	configs = Config(file_name=config_file)
	today = nc_date_grab()

	if state == "north_carolina":
		list_files = configs['data_chunk_links']
		zipped_files = []
		for i, url in enumerate(list_files):
			target_path = "/tmp/" + state + str(i) + ".zip"
			zipped_files.append(target_path)
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
			#config files not required by loader, fix this


def nc_date_grab():
	nc_file = urllib2.urlopen('https://s3.amazonaws.com/dl.ncsbe.gov?delimiter=/&prefix=data/')
	data = nc_file.read()
	nc_file.close()	
	root = xml.etree.ElementTree.fromstring(data)
	a = xml.etree.ElementTree.fromstring(data).findall('.//Key')
	for child in root:
		if "Contents" in child.tag:
			z = 0
			for i in child:
				if "data/ncvoter_Statewide.zip" in i.text:
					z += 1
					continue
				if z == 1:
					file_date_vf = i.text
					break

	for child in root:
		if "Contents" in child.tag:
			z = 0
			for i in child:
				if "data/ncvhis_Statewide.zip" in i.text:
					z += 1
					continue
				if z == 1:
					file_date_his = i.text
					break
	if file_date_his[0:10] != file_date_vf[0:10]:
		logging.info("Different dates between files, reverting to voter file date")

	file_date_vf = str(datetime.datetime.strptime(file_date_vf[0:10], "%Y-%m-%d"))[0:10]
	return(file_date_vf)
	

					

	
		
	"""
	from lxml import etree
	root = etree.fromstring(data)
	print(etree.tostring(root, pretty_print = True))
	"""
	"""	
	for child in root:
		for i in child:
			print(i.attrib)		
	"""	
			#e = xml.etree.ElementTree.parse(data)
	#root = e.getroot()
	#print(e)

	






