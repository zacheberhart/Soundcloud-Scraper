from scrapy import signals
from scrapy.conf import settings
import os
import sqlite3


class Database(object):

	db = settings.get('DB_FNAME')

	def connect(self):
		#self.conn = apsw.Connection(self.db)   # apsw
		self.conn = sqlite3.connect(self.db)    # sqlite3
		self.curs = self.conn.cursor()

	def close_connection(self):
		print('Closing connection...')
		self.conn.close()

	def create_table(self, table_name, cols):
		print('Creating new table: {}'.format(table_name))
		create_table_query = 'CREATE TABLE IF NOT EXISTS {} ('.format(table_name)
		for col in cols:
			create_table_query += '{}, '.format(col)
		create_table_query = create_table_query[:-2] + ');'
		
		print(create_table_query)
		self.curs.execute(create_table_query)

	def drop_table(self, table_name):
		print('Dropping old table: {}'.format(table_name))
		self.curs.execute('DROP TABLE IF EXISTS {};'.format(table_name))
		self.conn.commit()

	def recreate_table(self, table_name, cols):
	    self.drop_table(table_name)
	    self.create_table(table_name, cols)

	def insert_item(self, item, item_fields):
		insert_query = 'INSERT OR IGNORE INTO {} VALUES ({})'.format(
			self.table_name, ','.join(['?'] * (len(item) - 1))
		)
		self.curs.execute(insert_query, [item[field] for field in item_fields])
		self.conn.commit()

	def sanity_print(self, item):
		print('\n\n\n\n===== PIPELINE: PROCESS ITEM ======\n\n')
		print('\n\n\n{} {}\n\n'.format('ITEM KEYS:', item.keys()))
		print('\n\n\n{} {}\n\n\n\n'.format('ITEM VALS:', item.values()))


class ArtistPipeline(Database):

	def __init__(self):
		super().__init__()
		self.table_name = 'artists'
		self.table_fields = [
			'user_id', 'dt_crawled', 'retrieved_tracks', 'permalink', 'username', 'track_count',
			'followers_count', 'followings_count', 'likes_count', 'playlist_count', 'reposts_count',
			'comments_count', 'country', 'city', 'last_modified', 'plan', 'subscriptions'
		]
		self.connect()

	def process_item(self, item, spider):
		#self.sanity_print(item)
		if item['item_type'] == 'artist':
			self.insert_item(item, self.table_fields)
		return item


class UpdateArtistPipeline(Database):

	def __init__(self):
		super().__init__()
		self.table_name = 'updated_artists'
		self.table_fields = [
			'user_id', 'dt_updated', 'permalink', 'username', 'track_count', 'followers_count',
			'followings_count', 'likes_count', 'playlist_count', 'reposts_count', 'comments_count',
			'country', 'city', 'last_modified', 'plan', 'subscriptions'
		]
		self.connect()

	def process_item(self, item, spider):
		#self.sanity_print(item)
		if item['item_type'] == 'updated_artist':
			self.insert_item(item, self.table_fields)
		return item


class TrackPipeline(Database):

	def __init__(self):
		super().__init__()
		self.table_name = 'tracks'
		self.table_fields = [
			'track_id', 'dt_crawled', 'user_id', 'title', 'permalink', 'genre', 'tag_list',
			'public', 'sharing', 'state', 'policy', 'label_name', 'license', 'monetization_model',
			'commentable', 'streamable', 'downloadable', 'has_downloads_left', 'embeddable_by',
			'created_at', 'display_date', 'release_date', 'last_modified', 'duration',
			'full_duration', 'playback_count', 'likes_count', 'reposts_count', 'comment_count',
			'download_count'
		]
		self.connect()

	def process_item(self, item, spider):
		#self.sanity_print(item)
		if item['item_type'] == 'track':
			self.insert_item(item, self.table_fields)
		return item


class ProfileUrlPipeline(Database):

	def __init__(self):
		super().__init__()
		self.table_name = 'profile_urls'
		self.table_fields = ['permalink', 'dt_crawled', 'profiles_scraped']
		self.connect()

	def process_item(self, item, spider):
		if item['item_type'] == 'profile_url':
			self.insert_item(item, self.table_fields)
		return item


class LikersPipeline(Database):

	def __init__(self):
		super().__init__()
		self.table_name = 'track_likers'
		self.table_fields = ['track_id', 'likers']
		self.connect()

	def process_item(self, item, spider):
		if item['item_type'] == 'likers':
			self.insert_item(item, self.table_fields)
		return item


class RepostersPipeline(Database):

	def __init__(self):
		super().__init__()
		self.table_name = 'track_reposters'
		self.table_fields = ['track_id', 'reposters']
		self.connect()

	def process_item(self, item, spider):
		if item['item_type'] == 'reposters':
			self.insert_item(item, self.table_fields)
		return item


class CommentPipeline(Database):

	def __init__(self):
		super().__init__()
		self.table_name = 'comments'
		self.table_fields = [
			'comment_id', 'track_id', 'user_id', 'created_at', 'timestamp', 'body'
		]
		self.connect()

	def process_item(self, item, spider):
		if item['item_type'] == 'comment':
			self.insert_item(item, self.table_fields)
		return item


class ToggleScrapedPipeline(Database):

	def __init__(self):
		super().__init__()
		self.connect()

	def process_item(self, item, spider):
		if item['item_type'] == 'toggle_scraped':
			q = f'UPDATE artists SET retrieved_tracks = 1 WHERE user_id = {item["user_id"]};'
			self.curs.execute(q)
			self.conn.commit()
			print(f'\n\n\n\n\nToggled Artist Scraped...\n\n\n\n\n')
		return item
		








