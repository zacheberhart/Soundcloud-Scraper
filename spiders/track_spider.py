import scrapy
from scrapy.selector import Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging

from sc_scraper.items import Track, Artist, Comment, Reposters, Likers, ToggleScraped
from sc_scraper.wrappers import Soundcloud, collect_garbage

import gc
import logging
from time import sleep
from random import shuffle
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options


class TrackSpider(scrapy.Spider):
	"""Collect all Track data for the given Artists/Users from the /tracks endpoint."""

	# $ scrapy crawl [name]
	name = 'tracks'
	LOGGING = False

	def __init__(self, limit = 1, incl_already_scraped = False, *args, **kwargs):
		super(TrackSpider, self).__init__(*args, **kwargs)

		self.api = Soundcloud(wait = 0.3)
		self.limit = int(limit)
		self.incl_already_scraped = bool(incl_already_scraped)
		self.counter = 0
		self.ENGAGEMENT_TYPES = ['reposters', 'comments'] # engage types to scrape when getting tracks
		# self.artists = self.api.get_scraped_ids_from_db(to_retrieve_tracks = True,
		# 												order_by_dt_added = True,
		# 												limit = self.limit)
		self.artists = self.api.get_artist_ids_from_seed_list(incl_already_scraped = self.incl_already_scraped,
															  limit = self.limit)
		self.start_urls = ['https://soundcloud.com/' + permalink for permalink in list(self.artists.keys())]
		shuffle(self.start_urls)

	# log all output
	if LOGGING:
		configure_logging(install_root_handler = False)
		logging.basicConfig(
			filename='log.txt',
			format='%(levelname)s: %(message)s',
			#level=logging.INFO
			level=logging.ERROR
		)

	custom_settings = {
		'ITEM_PIPELINES': {
			'sc_scraper.pipelines.TrackPipeline'        :   0,
			'sc_scraper.pipelines.ArtistPipeline'       : 100,
			'sc_scraper.pipelines.CommentPipeline'      : 200,
			'sc_scraper.pipelines.LikersPipeline'       : 300,
			'sc_scraper.pipelines.RepostersPipeline'    : 400,
			'sc_scraper.pipelines.ToggleScrapedPipeline': 500,
		}
	}

	@collect_garbage
	def get_engagers(self, track_id, data, engagement_type):
		if engagement_type == 'reposters': item = Reposters()
		if engagement_type == 'likers'   : item = Likers()
		item['item_type'] = engagement_type
		item['track_id'] = track_id
		item[engagement_type] = self.api.join_ids(data[engagement_type], 'user_id')
		return item

	#@collect_garbage # this has a lot of iterations, maybe remove decorator
	def parse_engagement(self, data, engagement_type):
		
		# init appropriate engagement Item
		if engagement_type == 'reposters': item = Artist() ; item['item_type'] = 'artist'
		if engagement_type == 'comments' : item = Comment(); item['item_type'] = 'comment'
		
		# engagement specific Fields
		if engagement_type != 'comments':
			item['dt_crawled'] = self.api.get_timestamp()
			item['retrieved_tracks'] = False

		# add data to Item Fields
		for k in data.keys():
			item[k] = data[k]
		return item

	@collect_garbage
	def override_engagement_count(self, track, engagement_data):
		track_engagement = {
			'likes_count': len(engagement_data.get('likers', [])),
			'reposts_count': len(engagement_data.get('reposters', [])),
			'comment_count': len(engagement_data.get('comments', []))
		}
		for engagement_count in track_engagement.keys():
			if track[engagement_count] == 0 and track_engagement[engagement_count] > 0:
				track[engagement_count] = track_engagement[engagement_count]
		return track


	def parse(self, response):

		print(f'\n\n\nCrawled Count: {self.counter}\n\n\n')

		# get artist id from db and their tracks (stream) from api
		artist_id = self.artists[self.api.parse_permalink_from_url(response.url)]
		tracks = self.api.get_user_tracks(artist_id, 'stream', db_data_only = True)
		n_tracks = len(tracks) # for progress print only

		# build item to mark as scraped in db when finished crawling
		# NOTE: this is important to do as an item rather than a function call because
		# of out of memory crashes.
		toggle_scraped = ToggleScraped()
		toggle_scraped['item_type'] = 'toggle_scraped'
		toggle_scraped['user_id']   = artist_id

		# get a list of tracks that have already been added to db to prevent duplicate crawls
		self.crawled_tracks = self.api.get_data_from_db('tracks', ['track_id']).track_id.tolist()

		# clean up
		gc.collect()

		# loop through each track in the artist's stream and add to db if not already there
		for t in tracks:

			track_id = int(t['track_id'])

			print()
			print(f'scraped from: {artist_id} ({n_tracks} tracks)')
			print(f'track_id: {t["track_id"]}')
			print(f'permalink: {t["permalink"]}')
			print(f'playback_count: {t["playback_count"]}')
			print(f'likes_count: {t["likes_count"]}')
			print(f'comment_count: {t["comment_count"]}')
			print()

			#if track_id not in crawled_tracks:
			if not self.api.binary_search(self.crawled_tracks, 0, len(self.crawled_tracks)-1, track_id):

				print('\n\n============ NEW TRACK ============')
				print(f'=== Artists Crawled Count: {self.counter} ===\n\n')
			
				# init objects
				track = Track()
				track['item_type']  = 'track'
				track['dt_crawled'] = self.api.get_timestamp()

				# get engagement specified (any combo of the three avail) for given track
				engagement = self.api.get_multi_track_engagement(
					track_id = track_id,
					engagement_types = self.ENGAGEMENT_TYPES,
					db_data_only = True,
					expected_engagement = {'comments': int(t['comment_count'])}
				)

				# there is an issue with the API when sometimes 0 is returned for the
				# engagement (it is usually reposts). use this as an override when it occurs.
				t = self.override_engagement_count(t, engagement)

				# add user IDs of all reposts &| likes to respective tables
				for engagement_type in [k for k in engagement.keys() if k != 'comments']:
					print(engagement_type)
					if len(engagement[engagement_type]) > 0:
						yield self.get_engagers(track_id, engagement, engagement_type)
				
				# add each engagement to their respective Item/table
				for engagement_type in engagement.keys():
					for _data in engagement[engagement_type]:
						yield self.parse_engagement(_data, engagement_type)

				# add track to Item
				for k in t.keys():
					track[k] = t[k]
				
				yield track

		# toggle artist tracks retrieved
		yield toggle_scraped
		#self.api.toggle_user_scraped(artist_id)
		self.counter += 1
		gc.collect()

		

