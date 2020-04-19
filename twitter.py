import tweepy
import csv
import pandas as pd
import json


class twitter_client(object):

    def __init__(self, config="twitter_config.json"):
        with open(config, 'r') as f:
            cf = json.load(f)
            auth = tweepy.OAuthHandler(cf['consumer_key'], cf['consumer_secret'])
            auth.set_access_token(cf['access_token'], cf['access_token_secret'])
            self.api = tweepy.API(auth)

    def get_tweets_by_tag(self, tag, since, until, count=100, lang="nl"):
        rows = []
        for tweet in tweepy.Cursor(self.api.search, q=tag, count=count,
                                   lang=lang,
                                   since=since,
                                   until=until).items():
            rows.append(
                [tweet.id_str, tweet.created_at, tweet.user.name, tweet.text.replace('\n', ' '), tweet.user.location,
                 "|".join([x['text'] for x in tweet.entities['hashtags']])])

        return pd.DataFrame(rows, columns=['tweet_id', 'created_at', 'name', 'text', 'location', 'tags'])

    def get_tweets_by_handle(self, handle, since=None, until=None, count=100):
        rows = []
        for tweet in self.api.user_timeline(screen_name=handle, count=count, include_rts=False, tweet_mode="extended",
                                            since=since,
                                            until=until):
            rows.append(
                [tweet.id_str, tweet.created_at, tweet.user.name, tweet.full_text.replace('\n', ' '), tweet.user.location,
                 "|".join([x['text'] for x in tweet.entities['hashtags']])])

        return pd.DataFrame(rows, columns=['tweet_id', 'created_at', 'name', 'text', 'location', 'tags'])


if __name__ == '__main__':
    twitter = twitter_client()
    df = twitter.get_tweets_by_handle('@Bart_DeWever')

    df.to_csv('tweets_BDWV.csv')
