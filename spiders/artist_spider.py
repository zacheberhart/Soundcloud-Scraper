import scrapy
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging

from sc_scraper.items import Artist
from sc_scraper.wrappers import Soundcloud

import logging
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options


class ArtistSpider(scrapy.Spider):
	"""Collect the Internal Soundcloud ID and then all necessary data from
	the /user endpoint on Soundcloud's API"""

	# $ scrapy crawl [name]
	name = 'artists'
	LOGGING = True

	def __init__(self, limit = None, *args, **kwargs):
		super(ArtistSpider, self).__init__(*args, **kwargs)
		self.api = Soundcloud(wait = 0.25)
		self.limit = int(limit)
		self.start_urls = self.api.get_start_urls(spider = self.name, url_limit = self.limit)

	# log all output
	if LOGGING:
		configure_logging(install_root_handler=False)
		logging.basicConfig(
			filename = 'log.txt',
			format = '%(levelname)s: %(message)s',
			level = logging.INFO
		)

	custom_settings = {
		'ITEM_PIPELINES': {
			'sc_scraper.pipelines.ArtistPipeline': 0,
        }
    }

	def parse(self, response):

		# get internal ID from profile URL and call SC's /user endpoint
		internal_id = self.api.get_internal_sc_user_id(response.url)
		user = self.api.get_user(internal_id, db_data_only = True)

		# add to Artist Item
		artist = Artist()
		artist['item_type'] = 'artist'
		artist['dt_crawled'] = self.api.get_timestamp()
		artist['retrieved_tracks'] = False
		for k in user.keys():
			artist[k] = user[k]

		yield artist

		

