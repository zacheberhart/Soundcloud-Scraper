import scrapy
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging

from sc_scraper.items import ProfileUrl
from sc_scraper.wrappers import Soundcloud

import logging
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options



class ProfileUrlSpider(scrapy.Spider):
	"""Collect the Internal Soundcloud ID and then all necessary data from
	the /user endpoint on Soundcloud's API"""

	# $ scrapy crawl [name]
	name = 'profile_urls'
	LOGGING = True

	def __init__(self, start_url_limit = 1, *args, **kwargs):
		super(ProfileUrlSpider, self).__init__(*args, **kwargs)

		self.api = Soundcloud(wait = 0.3)
		self.url_limit = int(start_url_limit)
		self.start_urls = self.api.get_start_urls(spider = self.name,
											 url_limit = self.url_limit,
											 follower_max = 750000)
		self.internal_ids = dict(self.api.get_scraped_ids_from_db())
		self.scrape_min = dict(self.api.get_min_scraped_profile_urls())
		self.scrape_urls_from = ['followers', 'following', 'stream', 'likes', 'comments']

	# log all output
	if LOGGING:
		configure_logging(install_root_handler=False)
		logging.basicConfig(
			filename='log.txt',
			format='%(levelname)s: %(message)s',
			level=logging.INFO
		)

	custom_settings = {
		'ITEM_PIPELINES': {
			'sc_scraper.pipelines.ProfileUrlPipeline': 0,
        }
    }

	def parse(self, response):

		# debug
		#print('STARTING: {}'.format(response.url))

		# get internal ID from profile URL and call SC's /user endpoint
		permalink = self.api.parse_permalink_from_url(response.url)
		
		# try scraping min number of profiles for max of 3 tries
		for _ in range(3):
			new_permalinks = self.api.scrape_profile_urls_from_user(self.internal_ids[permalink],
																	inc_data = self.scrape_urls_from)
			if len(new_permalinks) >= self.scrape_min[permalink]: break

		# update the profile_url table so that the seed url is marked as scraped with dt and profiles_scraped
		self.api.toggle_profile_url_scraped(permalink, n_scraped = len(new_permalinks))

		# add all new permalinks to profile_urls table
		for permalink in new_permalinks:
			item = ProfileUrl()
			item['item_type'] = 'profile_url'
			item['permalink'] = permalink
			item['dt_crawled'] = 0
			item['profiles_scraped'] = 0
			yield item


