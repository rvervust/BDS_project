import tweepy
import pandas as pd
import json
import GetOldTweets3 as got


class oldtweets_client(object):
    def get_tweets_by_tag(self, tag, since=None, until=None, count=100, lang="nl", geocode=None):
        """
        Retrieve tweets by a given hashtag
        :param tag: the hashtag (including #)
        :param since: start date in yyyy-mm-dd string format (inclusive)
        :param until: end date in yyyy-mm-dd string format (inclusive)
        :param count: the number of tweets to retrieve (it is specified in the API, but the tweepy library seems to ignore this and return more tweets)
        :param lang: the tweet language to be filtered on
        :return: DataFrame containing al tweets (a tweet per row)
        """
        tweetCriteria = got.manager.TweetCriteria().setQuerySearch(tag) \
            .setSince(since) \
            .setUntil(until) \
            .setMaxTweets(count)
        rows = []
        for tweet in got.manager.TweetManager.getTweets(tweetCriteria):
            rows.append([tweet.id, tweet.date, tweet.username, tweet.text.replace('\n', ' '), tweet.geo, tweet.hashtags])

        return pd.DataFrame(rows, columns=['tweet_id', 'created_at', 'name', 'text', 'location', 'tags'])


class twitter_client(object):
    """
    This class handles the connection to the Twitter API, it provides a high level access to tweet retrieval
    """

    def __init__(self, config="../twitter_config.json"):
        """
        Initiate the client, using the api keys in a specified config file
        :param config: json file with Twitter credentials
        """
        with open(config, 'r') as f:
            cf = json.load(f)
            auth = tweepy.OAuthHandler(cf['consumer_key'], cf['consumer_secret'])
            auth.set_access_token(cf['access_token'], cf['access_token_secret'])
            self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def get_tweets_by_tag(self, tag, since=None, until=None, count=100, lang="nl", geocode=None):
        """
        Retrieve tweets by a given hashtag
        :param tag: the hashtag (including #)
        :param since: start date in yyyy-mm-dd string format (inclusive)
        :param until: end date in yyyy-mm-dd string format (inclusive)
        :param count: the number of tweets to retrieve (it is specified in the API, but the tweepy library seems to ignore this and return more tweets)
        :param lang: the tweet language to be filtered on
        :return: DataFrame containing al tweets (a tweet per row)
        """
        print(tag, since, until, count, lang)
        rows = []
        for tweet in tweepy.Cursor(self.api.search, q=tag, count=count,
                                   lang=lang,
                                   since=since,
                                   until=until,
                                   geocode=geocode).items():
            rows.append(
                [tweet.id_str, tweet.created_at, tweet.user.name, tweet.text.replace('\n', ' '), tweet.user.location,
                 "|".join([x['text'] for x in tweet.entities['hashtags']])])

        return pd.DataFrame(rows, columns=['tweet_id', 'created_at', 'name', 'text', 'location', 'tags'])

    def get_tweets_by_handle(self, handle, since=None, until=None, count=100, lang="nl"):
        """
        Retrieve tweets by a given user handle
        :param handle: The user handle (including @)
        :param since: start date in yyyy-mm-dd string format (inclusive)
        :param until: end date in yyyy-mm-dd string format (inclusive)
        :param count: the number of tweets to retrieve (it is specified in the API, but the tweepy library seems to ignore this and return more tweets)
        :return: DataFrame containing al tweets (a tweet per row)
        """
        rows = []
        for tweet in self.api.user_timeline(screen_name=handle, count=count, include_rts=False, tweet_mode="extended",
                                            lang=lang,
                                            since=since,
                                            until=until,
                                            place_country='BE'):
            rows.append(
                [tweet.id_str, tweet.created_at, tweet.user.name, tweet.full_text.replace('\n', ' '),
                 tweet.user.location,
                 "|".join([x['text'] for x in tweet.entities['hashtags']])])

        return pd.DataFrame(rows, columns=['tweet_id', 'created_at', 'name', 'text', 'location', 'tags'])


if __name__ == '__main__':
    twitter = twitter_client()
    df = twitter.get_tweets_by_tag('#blijfinuwkot', since='2020-04-19', until='2020-04-20', lang='nl')
    print(df.head())
