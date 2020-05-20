# Twitter Stream

## Description
This python script provides an interface to Twitter.
It is able to open a stream to Twitter and receive all tweets matchin certain search parameters

## Requirements
To install via pip:
 - Tweepy
 - Pandas
 
## Usage
1. Edit the configuration file (stream_config.json) with your search queries, an example is provided.
2. Run the twitter_stream.py script
3. The script will start to listen for tweets, print them in console and write them to a file per day
