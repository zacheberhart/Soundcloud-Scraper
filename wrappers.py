import sqlite3
import re
import requests
import logging
import pickle
import pandas as pd
import gc
from scrapy.exceptions import CloseSpider
from time import time, sleep
from copy import deepcopy
from random import randint
from math import ceil
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
#pd.options.mode.chained_assignment = None

# garbage collection decorator
def collect_garbage(func):
	def wrapper(*args, **kwargs):
		print(f'\nCollected Garbage before {func.__name__}: {gc.collect()}\n')
		return func(*args, **kwargs)
	return wrapper

class Soundcloud():

	def __init__(self, wait = 2):
		self.db_path = '/root/sc_scraper/sc.db'
		self.base_v1 = 'http://api.soundcloud.com'
		self.base_v2 = 'https://api-v2.soundcloud.com'
		self.client_id = ''
		self.wait = wait
		self.close_spider = False


	def _connect_to_db(self):
		'''Connect to the "sc.db" Database'''
		self.conn = sqlite3.connect(self.db_path)
		self.curs = self.conn.cursor()


	def _execute_query(self, query):
		'''Execute a query using the current db connection'''
		self.curs.execute(query)
		self.conn.commit()


	def _query_db(self, query):
		self._connect_to_db()
		data = pd.read_sql(query, self.conn)
		self.conn.close()
		return data


	def _rand_sleep(self):
		'''randomize sleep times in alignment with Scrapy settings'''
		sleep(self.wait * (randint(50, 150) / 100))


	def vacuum(self):
		'''vacuum sqlite db'''
		self._connect_to_db()
		self.conn.execute('VACUUM;')
		self.conn.close()


	def get_timestamp(ms = True):
		if ms: return int(time() * 1000)
		else: return int(time())


	def open_pkl(self, fname):
		with open(fname, 'rb') as f:
			return pickle.load(f)


	def binary_search(self, arr, l, r, x):
		'''Recursive binary search for more efficient scraped track/artist check'''
		if r >= l:
			mid = l + (r - l) // 2
			if arr[mid] == x: return True
			elif arr[mid] > x: return self.binary_search(arr, l, mid-1, x) 
			else: return self.binary_search(arr, mid + 1, r, x)
		else:
			return False


	@collect_garbage
	def get_data_from_db(self, table_name, cols):
		return self._query_db('SELECT {} FROM {};'.format(','.join(['permalink'] + cols), table_name))


	@collect_garbage
	def get_scraped_ids_from_db(self, to_retrieve_tracks = False, order_by_dt_added = False, limit = None):
		'''Get the permalinks and internal IDs of each artist/user that has been
		added to "artists" table'''

		# droplet is running out of memory (4GB) when trying to retrieve >13 million artists
		# add a temporary hard limit on the database query until this can be refactored to better handle the args
		artists  = self._query_db('SELECT user_id, permalink, dt_crawled, retrieved_tracks FROM artists LIMIT 1000000;')
		if order_by_dt_added: artists = deepcopy(artists.sort_values('dt_crawled').reset_index(drop = True))
		if to_retrieve_tracks: artists = deepcopy(artists[artists.retrieved_tracks == 0])
		if limit: artists = deepcopy(artists[:limit])
		return {permalink: user_id for permalink, user_id in
				zip(artists.permalink, artists.user_id)}


	@collect_garbage
	def get_artist_ids_from_seed_list(self, incl_already_scraped = False, limit = None):
		'''To make sure that all of the profiles that are one degree away from "zacheism" or "deeperdreamer" are
		scraped, load the list directly from a .pkl file (order was changed after a VACUUM)'''
		seeds = self.open_pkl('/root/sc_scraper/sc_scraper/seed_list.pkl')
		if not incl_already_scraped:
			scraped = self._query_db('SELECT user_id, permalink FROM artists WHERE retrieved_tracks=1;')
			seeds = {permalink: user_id for permalink, user_id in seeds.items() if permalink not in scraped.permalink.tolist()}
			print(f'\n\nRemaining Seeds: {len(seeds)}\n\n')
		if limit:
			seeds = {k:v for k,v in seeds.items() if k in list(seeds.keys())[:limit]}
		return seeds


	def get_min_scraped_profile_urls(self, following_mult = 0.95, follower_mult = 0.50):
		'''Because the profile being scraped is abandoned after any server/api errors, we can safeguard
		against data loss by requiring a minimum amount of scraped proifle urls from a given profile. The
		defaults set are what I think is optimal (most users are listeners, and will have a higher amount
		of followings with a low chance of overlap in followers) -- though this has not been tested.'''
		artists = self.get_data_from_db('artists', ['followers', 'following'])
		artists['min_scrape'] = ((artists.followers * follower_mult) + (artists.following * following_mult)
								).astype('int')
		return {permalink: n_scrape for permalink, n_scrape in zip(artists.permalink, artists.min_scrape)}


	def toggle_profile_url_scraped(self, permalink, n_scraped):
		'''Add timestamp and number of profiles scraped'''
		self._connect_to_db()
		ts = self.get_timestamp()

		# update the seed url with the datetime and number of profile URLs that were scraped
		update_query = '''
		UPDATE profile_urls SET profiles_scraped = {}, dt_crawled = {}
		WHERE permalink = "{}";
		'''.format(n_scraped, ts, permalink)

		self._execute_query(update_query)
		self.conn.close()
		print('Updated "profile_urls" table...')


	@collect_garbage
	def toggle_user_scraped(self, user_id):
		'''Mark a user/artist as scraped in the db so that it is not crawled again'''
		self._connect_to_db()
		update_query = 'UPDATE artists SET retrieved_tracks = 1 WHERE user_id = {};'.format(user_id)
		self._execute_query(update_query)
		self.conn.close()
		print('Updated "artists" table...')


	def join_ids(self, data, col):
		return ','.join(pd.DataFrame(data)[col].astype('str').tolist())


	def parse_permalink_from_url(self, url):
		return str(url).split('soundcloud.com')[1].replace('/', '')


	def get_url(self, url):
		r = requests.get(url)
		print('URL: {}\nSTATUS: {}\n'.format(url, r.status_code))
		return r


	def get_internal_api_calls_on_load(self, url, event_filter = 'api-v2.soundcloud.com'):
		'''Get all of the internal calls to "api-v2.soundcloud.com" that are made on page load.'''

		# get all relevant items in log via headless Chrome (Selenium)
		options = Options()
		options.headless = True
		cap = DesiredCapabilities.CHROME
		cap['loggingPrefs'] = {'performance': 'ALL'}
		driver = webdriver.Chrome(desired_capabilities = cap, options = options)
		driver.get(url)
		log = [item for item in [item for item in driver.get_log('performance')]
			   if event_filter in str(item)]
		driver.close()

		# find urls and parse out api calls
		find_urls_regex = '(http|ftp|https)(:\/\/)([\w_-]+(?:(?:\.[\w_-]+)+))' + \
						  '([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?'

		return [''.join(r) for r in re.findall(find_urls_regex, str(log))
				if event_filter in str(r)]


	def get_internal_sc_user_id(self, profile_url):
		'''Get the internal User ID for a given SoundCloud profile URL. This could definitely be
		optimized as it is currently parsing the entire Performance Log and returning the most common
		user_id it finds (which is presumably the correct one -- another optimization opportunity)'''
		return int(pd.Series([c.split('https://api-v2.soundcloud.com/users/')[1].split('/')[0]
							  for c in self.get_internal_api_calls_on_load(profile_url)
							  if 'https://api-v2.soundcloud.com/users/' in c
							 ]).value_counts().idxmax())


	def get_start_urls(self, spider, url_limit = None, follower_max = None):

		# get all permalinks which have not been scraped.
		# NOTE: it's okay to have "profiles_scraped = 0" condition because a
		#       profile cannot be scraped until it's been added to "artists" table
		permalinks = self._query_db(
			'SELECT permalink FROM profile_urls WHERE profiles_scraped = 0;'
		).permalink.tolist()

		# to avoid unnecesary scraping, remove any artists that have already been added to db
		if spider == 'artists':
			artists_in_db = self._query_db('SELECT permalink FROM artists;').permalink.tolist()
			# hacky, but O(n) way of removing non-scraped permalinks that have already been added
			# to artists table while preserving order (originally was using list comprehension)
			permalinks = pd.Series(permalinks + artists_in_db + artists_in_db
								  ).drop_duplicates(keep = False).tolist()

		# use first %url_limit% permalinks if applicable
		if url_limit: permalinks = permalinks[:url_limit]

		# if this is the profile_urls spider, then using a follower maximum is advised
		if spider == 'profile_urls' and follower_max:
			followers = self.get_data_from_db('artists', ['followers'])
			permalinks = followers[(followers.permalink.isin(permalinks)) &
								   (followers.followers <= follower_max)
								  ].permalink.tolist()

		return ['https://soundcloud.com/' + permalink for permalink in permalinks]


	def standard_v2_call(self, url, iter_all = True, call_limit = None):
		'''Base API call to SoundCloud API (v2)'''
		data = []
		calls, data_len, prev_data_len, no_new_data = 0, 0, 0, 0

		while True:
			r = self.get_url(url)
			calls += 1
			if r.status_code == 200:
				data += r.json()['collection']
				try:
					# dedup data to avoid inf loops
					data = list(pd.DataFrame(data).drop_duplicates('id').to_dict('records'))
				except KeyError as e:
					print(f'KeyError: {e}')
					print('Logging error and closing spider...')
					raise CloseSpider('Fatal Error: Missing ID column in data returned from standard_v2_call(). Please debug.')
				next_url = r.json()['next_href']
				if next_url == None or not iter_all: break
				if 'client_id' not in next_url:
					url = '{}&client_id={}'.format(next_url, self.client_id)
				else: url = str(next_url)
				self._rand_sleep()
			
			elif r.status_code == 500:
				print('\nServer Error (Status Code: {}). Sleeping for a bit...'.format(r.status_code))
				sleep(10 * (randint(80, 120) / 100)) # sleep between 8 - 12 seconds
			
			# this error is usually caused by too many requests, sleeping for 10-20 seconds usually fixes it
			elif r.status_code == 502:
				print('\nBad Gateway (Status Code: {}). Sleeping for a bit...'.format(r.status_code))
				sleep(20 * (randint(50, 150) / 100)) # sleep between 10 - 30 seconds
			
			else:
				# if there is an unknown error the spider will get shutdown as the data object will be unknown
				logging.fatal('\n\nThere is an unknown error ({}). Deleting object and stopping process.\nURL: {}\n\n'.format(r.status_code, url))
				raise CloseSpider('Unknown & Fatal Error (Status Code: {}). Debug before continuing.'.format(r.status_code))
				return None
			
			# check if calls exceeds call limit
			if call_limit and calls >= call_limit: break

			# there is a bug with SC's API that sometimes causes an infinite call loop
			# break out of this if there is no new data after a few times in a row
			if not call_limit:
				data_len = len(data)
				if data_len == prev_data_len: no_new_data += 1
				else: no_new_data = 0
				if no_new_data >= 10: break
				prev_data_len = len(data)
		
		return data


	def get_user(self, internal_id, db_data_only = False):
		"""Get all associated user data via a profile's internal Soundcloud ID using SC's API (public v1)."""

		url = '{}/users/{}?client_id={}'.format(self.base_v1, internal_id, self.client_id)
		user = pd.DataFrame([self.get_url(url).json()])\
				   .drop('likes_count', axis = 1)\
				   .rename(columns = {'id': 'user_id', 'public_favorites_count': 'likes_count'})

		if db_data_only:
			user = user[[
				'user_id', 'permalink', 'username', 'last_modified', 'country', 'city', 'track_count',
				'playlist_count', 'plan', 'followers_count', 'followings_count', 'subscriptions',
				'likes_count', 'reposts_count', 'comments_count'
			]]
			user.subscriptions = user.subscriptions.apply(lambda x: str(x[0]['product']['id']) if len(x) > 0 else None)

		return user.to_dict('records')[0]


	def get_user_follow(self, internal_id, follow_type):
		'''Get full API output for all followers for given user (max followers per request: 200)
		follow_type: "followers" or "followings"'''

		assert follow_type == 'followers' or follow_type == 'followings'
		url = '{}/users/{}/{}?client_id={}&limit=200'.format(self.base_v1, internal_id,
															 follow_type, self.client_id)
		follow = []
		while url:
			r = self.get_url(url)
			follow += r.json()['collection']
			url = r.json()['next_href']
			self._rand_sleep()

		return follow


	def _fill_missing_engagement(self, track_df):
		'''Fill missing engagement data with 0s'''
		engage_counts = ['playback_count', 'likes_count', 'reposts_count', 'comment_count', 'download_count']
		for e in engage_counts:
			track_df[e] = track_df[e].fillna(0)
		return track_df


	@collect_garbage
	def get_user_tracks(self, internal_id, from_type, db_data_only = False, max_server_errors = 25):
		'''Get list of tracks for given user. There doesnt seem to be a limit but this uses API v2
		   which is not publicly facing so its TBD on how well/long it will work. The limit is (for
		   now) hard-coded into each of the requests because the limiit is arbitrary for "tracks" and
		   you cant increase it past 1000 without getting too many errors for "stream".'''

		db_track_cols = track_cols = [
			'comment_count', 'commentable', 'created_at', 'display_date', 'download_count', 'downloadable',
			'duration', 'embeddable_by', 'full_duration', 'genre', 'has_downloads_left', 'id', 'label_name',
			'last_modified', 'license', 'likes_count', 'monetization_model', 'permalink', 'playback_count',
			'policy', 'public', 'release_date', 'reposts_count', 'sharing', 'state', 'streamable', 'tag_list',
			'title', 'user_id'
		]

		server_errors = 0

		if from_type == 'tracks':
			url = '{}/users/{}/tracks?client_id={}&limit=100000'.format(self.base_v2, internal_id, self.client_id)
			r = self.get_url(url)
			if db_data_only:
				return self._fill_missing_engagement(
					pd.DataFrame(r.json()['collection'])[db_track_cols].rename(columns = {'id': 'track_id'})
				).to_dict('records')
			else: return r.json()['collection']

		elif from_type == 'stream':
			url = '{}/stream/users/{}?client_id={}&limit=1000'.format(self.base_v2, internal_id, self.client_id)
			tracks = []

			while True:
				r = self.get_url(url)
				if r.status_code == 200:
					data = r.json()
					server_errors = 0
					if len(data['collection']) == 0:
						print('There are no more tracks in the Stream, returning results...')
						tdf = pd.DataFrame(tracks)
						if 'track' in tdf.columns:
							init_tracks = self._fill_missing_engagement(
								pd.DataFrame(tdf.track.dropna().tolist())
							).to_dict('records')
						else: init_tracks = []
						if 'playlist' in tdf.columns:
							pl_tracks = pd.DataFrame([t for track in
													  pd.DataFrame(tdf.playlist.dropna().tolist()).tracks.dropna().tolist()
													  for t in track])
							if len(pl_tracks) > 0 and 'permalink_url' in pl_tracks.columns.tolist():
								pl_tracks = self._fill_missing_engagement(
									pl_tracks[~pl_tracks.permalink_url.isnull()]
									).to_dict('records')
							else: pl_tracks = []
						else: pl_tracks = []
						if db_data_only:
							try:
								return pd.DataFrame(init_tracks + pl_tracks)[db_track_cols]\
									   .rename(columns = {'id': 'track_id'}).to_dict('records')
							except KeyError as e:
								return init_tracks + pl_tracks
						else: return init_tracks + pl_tracks
					else:
						tracks += data['collection']
						url = '{}&client_id={}'.format(r.json()['next_href'], self.client_id)
						self._rand_sleep()
				elif r.status_code == 500:
					print(f'Server Errors: {server_errors}')
					print('Sleeping for a few seconds...')
					max_server_errors += 1
					if server_errors % max_server_errors == 0: return None
					sleep(10)
				else:
					print('Unexpected Status Code:', r.status_code)
					return None
		else:
			print('Please choose a valid "from_type": "tracks" or "stream"')
			return None


	def get_user_likes(self, internal_id):
		'''Get all likes / favorites for a given user. The API has a max return of 200 likes per request.'''

		url = '{}/users/{}/likes?client_id={}&limit=200'.format(self.base_v2, internal_id, self.client_id)
		likes = self.standard_v2_call(url)

		if len(likes) > 0:
			return pd.DataFrame(likes).track.dropna().tolist()
		else: return []


	def get_user_comments(self, internal_id, return_data = 'comments'):
		'''Get all comments made by a user. Data can be returned in two different formats: "comments",
		which contain the content of the actual contents (and the resp track and user IDs), and "tracks",
		which is the standard SC track payload of each track the user commented on (not deduped).'''

		url = '{}/users/{}/comments?client_id={}&limit=1000'.format(self.base_v2, internal_id, self.client_id)
		comments = []

		while True:
			r = self.get_url(url)
			if r.status_code == 200:
				data = r.json()
				if len(data['collection']) == 0 and len(comments) == 0: return []
				elif len(data['collection']) == 0:
					tdf = pd.DataFrame(comments)
					if return_data == 'comments':
						return tdf[
							['body', 'created_at', 'id', 'timestamp', 'track_id', 'user_id']
						].to_dict('records')
					elif return_data == 'tracks':
						if 'tracks' in tdf.columns: return tdf.track.dropna().tolist()
						else: return tdf
					else:
						print('Please choose valid (return) data type: "comments" or "tracks"')
						return None
				else:
					comments += data['collection']
					# get next URL and sleep
					url = '{}&client_id={}'.format(r.json()['next_href'], self.client_id)
					self._rand_sleep()

			elif r.status_code == 500:
				print('Server Error. Sleeping for a few seconds...')
				sleep(5)
			else:
				print('Unexpected Status Code:', r.status_code)
				return None


	def get_uniq_usernames_from_sc_payload(self, obj):
		'''Returns a unique list of usernames from a payload. Handles every payload except
		for the `comments` type from get_user_comments() -- which has been pre-processed.
		Use the `tracks` type from get_user_comments() for the users a user commented on.'''
		if 'permalink_url' in pd.DataFrame(obj).columns:
			return list(set(
				[url.split('soundcloud.com')[1].split('/')[1] for url in
				 pd.DataFrame(obj).permalink_url.dropna().tolist()]
			))
		else: return []


	def scrape_profile_urls_from_user(self, internal_id, inc_data):
		'''Wrapper to get all associated profiles from a given user:
		followers, following, stream (tracks & playlists), likes, and comments.'''

		data = {
			'followers': self.get_user_follow(internal_id, 'followers') if 'followers' in inc_data else None,
			'following': self.get_user_follow(internal_id, 'followings') if 'following' in inc_data else None,
			'stream'   : self.get_user_tracks(internal_id, 'stream') if 'stream' in inc_data else None,
			'likes'    : self.get_user_likes(internal_id) if 'likes' in inc_data else None,
			'comments' : self.get_user_comments(internal_id, 'tracks') if 'comments' in inc_data else None,
		}
		data = deepcopy({k: data[k] for k in inc_data})

		if len([v for v in data.values() if v is not None and len(v) > 0]) > 0:
			return list(set([u for user in
							 [self.get_uniq_usernames_from_sc_payload(data[d]) for d in inc_data]
							 for u in user]))
		else: return []


	@collect_garbage
	def get_track_engagement(self, track_id, engagement_type, db_data_only = False, expected_engagement = None):
		'''Get list of track engagements by type, given an internal (SC) id.
		Engagement Types: "reposters", "likers", or "comments"'''

		# build API call based on engagement type
		if   engagement_type == 'reposters':
			url = '{}/tracks/{}/reposters?client_id={}&limit=200'.format(self.base_v2, track_id, self.client_id)
		elif engagement_type == 'likers':
			url = '{}/tracks/{}/likers?client_id={}&limit=200'.format(self.base_v2, track_id, self.client_id)
		elif engagement_type == 'comments':
			comment_params = '&threaded=1&filter_replies=0'
			url = '{}/tracks/{}/comments?client_id={}&limit=200{}'.format(self.base_v2, track_id,
																		  self.client_id, comment_params)
		else:
			print('Please use a valid engagement_type: "reposters", "likers", or "comments"')
			return None

		''' DEBUG: There was one instance where comments got stuck in an infinite loop but I wasn't able
		to reproduce the bug after it crashed the memory. It might've had something to do with there being
		no expected engagement (I couldn't find a comment count on the track that caused the problem but
		I'm not entirely sure.'''

		# call API
		if expected_engagement:
			call_limit = int(ceil(expected_engagement / 200) * 1.5)

			# TEMP DEBUG : FIGURE OUT A MORE EFFICIENT WAY TO STOP COMMENT INF LOOP
			if engagement_type == 'comments' and call_limit == None:
				call_limit = 100
			
			data = pd.DataFrame(self.standard_v2_call(url, call_limit = call_limit))
		else:
			data = pd.DataFrame(self.standard_v2_call(url))

		# set col lists / naming for filtering and consistency
		db_user_cols = [
			'id', 'permalink', 'username', 'track_count', 'followers_count', 'followings_count',
			'likes_count', 'playlist_count', 'reposts_count', 'comments_count', 'country_code',
			'city', 'last_modified', 'creator_subscriptions'
		]
		rename_user_cols = {'id': 'user_id', 'country_code': 'country', 'creator_subscriptions': 'subscriptions'}
		comment_cols = ['body', 'created_at', 'id', 'timestamp', 'track_id', 'user_id']

		# return data
		if len(data) > 0:

			# all data from comment object is added to db (besides users in this case, which is missing
			# most of the data from the standard SC User object so it is ommitted in this case)
			if engagement_type == 'comments':
				return data[comment_cols].rename(columns = {'id': 'comment_id'}).to_dict('records')

			# filter out unnecesary data for User objects returned from /reposters or /likers
			if db_data_only:
				data = data[db_user_cols].copy()
				data['creator_subscriptions'] = data.creator_subscriptions.apply(
					lambda x: str(x[0]['product']['id']) if len(x) > 0 else None
				)
				data['plan'] = None
				return data.rename(columns = rename_user_cols).to_dict('records')
			else: return data.to_dict('records')

		return []


	@collect_garbage
	def get_multi_track_engagement(self, track_id, engagement_types, db_data_only = False, expected_engagement = {}):
		'''engagement_types: "reposters", "likers", "comments" '''
		return {engage_type: self.get_track_engagement(track_id, engage_type, db_data_only,
													   expected_engagement.get(engage_type))
				for engage_type in engagement_types}








