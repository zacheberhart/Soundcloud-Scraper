import scrapy
from scrapy.item import Item, Field

class Artist(Item):
	item_type           = Field()
	user_id             = Field() # PK
	dt_crawled          = Field()
	retrieved_tracks    = Field()
	permalink           = Field()
	username            = Field()
	track_count         = Field()
	followers_count     = Field()
	followings_count    = Field()
	likes_count         = Field()
	playlist_count      = Field()
	reposts_count       = Field()
	comments_count      = Field()
	country             = Field()
	city                = Field()
	last_modified       = Field()
	plan                = Field()
	subscriptions       = Field()


class UpdatedArtist(Item):
	item_type           = Field()
	user_id             = Field() # PK
	dt_updated          = Field()
	permalink           = Field()
	username            = Field()
	track_count         = Field()
	followers_count     = Field()
	followings_count    = Field()
	likes_count         = Field()
	playlist_count      = Field()
	reposts_count       = Field()
	comments_count      = Field()
	country             = Field()
	city                = Field()
	last_modified       = Field()
	plan                = Field()
	subscriptions       = Field()


class ProfileUrl(Item):
	item_type           = Field()
	permalink           = Field() # PK
	dt_crawled          = Field()
	profiles_scraped    = Field()


class Track(Item):
	item_type           = Field()
	track_id            = Field() # PK
	dt_crawled          = Field()
	user_id             = Field()
	title               = Field()
	permalink           = Field()
	genre               = Field()
	tag_list            = Field()
	public              = Field()
	sharing             = Field()
	state               = Field()
	policy              = Field()
	label_name          = Field()
	license             = Field()
	monetization_model  = Field()
	commentable         = Field()
	streamable          = Field()
	downloadable        = Field()
	has_downloads_left  = Field()
	embeddable_by       = Field()
	created_at          = Field()
	display_date        = Field()
	release_date        = Field()
	last_modified       = Field()
	duration            = Field()
	full_duration       = Field()
	playback_count      = Field()
	likes_count         = Field()
	reposts_count       = Field()
	comment_count       = Field()
	download_count      = Field()


class Comment(Item):
	item_type           = Field()
	comment_id          = Field() # PK
	track_id            = Field()
	user_id             = Field()
	created_at          = Field()
	timestamp           = Field()
	body                = Field()


class Reposters(Item):
	item_type           = Field()
	track_id            = Field() # PK
	reposters           = Field()


class Likers(Item):
	item_type           = Field()
	track_id            = Field() # PK
	likers              = Field()


class ToggleScraped(Item):
	item_type           = Field()
	user_id             = Field()


