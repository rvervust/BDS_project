import tweepy
import pandas as pd
import json
import datetime
import os


class twitter_client(object):
    """
    This class handles the connection to the Twitter API, it provides a high level access to tweet retrieval
    """

    def __init__(self, twitter_config="../twitter_config.json", stream_config="./stream_config.json"):
        """
        Initiate the client, using the api keys in a specified config file
        :param config: json file with Twitter credentials
        """
        with open(twitter_config, 'r') as f:
            with open(stream_config, 'r') as f2:
                cf = json.load(f)
                cf2 = json.load(f2)
                print(cf2)
                auth = tweepy.OAuthHandler(cf['consumer_key'], cf['consumer_secret'])
                auth.set_access_token(cf['access_token'], cf['access_token_secret'])
                self.myStreamListener = listener(out=cf2["output"], limit = cf2["limit"])
                self.myStream = tweepy.Stream(auth=auth, listener=self.myStreamListener, tweet_mode='extended')
                self.myStream.filter(track=cf2["keywords"], is_async=False)


class csv_dumper(object):
    def __init__(self, outfolder, limit=20):
        self.rows = []
        self.limit = limit
        self.outfolder = outfolder
        self.check_dirs()

    def append(self, tweet):

        if hasattr(tweet, "retweeted_status"):  # Check if Retweet
            try:
                text = tweet.retweeted_status.extended_tweet["full_text"]
            except AttributeError:
                text = tweet.retweeted_status.text
        else:
            try:
                text = tweet.extended_tweet["full_text"]
            except AttributeError:
                text = tweet.text

        print(text)
        self.rows.append(
            [tweet.id_str, tweet.created_at, tweet.user.name, text.replace('\n', ' '), tweet.user.location,
             "|".join([x['text'] for x in tweet.entities['hashtags']])])
        if len(self.rows) >= self.limit:
            self.dump()

    def dump(self):
        df = pd.DataFrame(self.rows, columns=['tweet_id', 'created_at', 'name', 'text', 'location', 'tags'])
        self.rows = []
        date = datetime.datetime.now().strftime('%Y-%m-%d')
        i = 0
        target = os.path.join(self.outfolder, "{}.csv".format(date))
        if os.path.exists(target):
            df.to_csv(target, index=False,  mode='a', header=False)
        else:
            df.to_csv(target, index=False)

        print(f"Dumped {len(df)} tweets to file {target}")

    def check_dirs(self):
        if not os.path.exists(self.outfolder):
            os.makedirs(self.outfolder)
            print("Created directory {}".format(self.outfolder))


class listener(tweepy.StreamListener):
    def __init__(self, out="./twitter_data", limit=20):
        super().__init__()
        self.dumper = csv_dumper(out, limit=20)

    def on_status(self, tweet):
        self.dumper.append(tweet)

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            print("Twitter rate limit reached, reconnecting stream with backoff")
            return True
        else:
            print(f"Statuscode: {status_code}")
            return False


if __name__ == '__main__':
    twitter = twitter_client()
