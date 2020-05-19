# Twitter Crawler

## Description
This python script provides an interface to Twitter.
It is able to gather tweets based on a certain keyword and save it to a file per day.

## Requirements
To install via pip:
 - Tweepy
 - Pandas
 - GetOldTweets3
## Usage
1. Edit the configuration file (keywords_config.json) with your search queries, an example is provided.
2. Run the fetch_tweets.py script

Note: Depending on the number of Tweets in your search query, the script can take a long time due to Twitter rate limiting.