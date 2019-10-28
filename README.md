# SoundCloud Scraper

A Scrapy spider to scrape user and track information from SoundCloud.

**Steps to get started**:

 1) Create a SQLite database with the desired data (see `items.py` or [SoundCloud docs](https://developers.soundcloud.com/docs/api/reference) for full list).
 2) Build a seed list of accounts to begin scraping (the spider will also scrape all related to the accounts in this list so the results returned can grow exponentially).
 3) Modify `settings.py` to your desired settings.
 4) Run the `artists` spider to retrieve artist data (you will need to do this first as the `tracks` spider relies on SoundCloud's internal Artist ID).
 5) Run the `profile_urls` spider to retrieve all users associated with the seed artists.
 6) And finally (if desired), run the `tracks` spider to retreive all track data associated with scraped artists.
 7) Repeat as needed.
