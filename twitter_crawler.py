import twitter
import json
import datetime
import os
import time


class Crawler(object):
    def __init__(self, twitter_client, config='crawler_config.json'):
        self.twitter = twitter_client
        with open(config, 'r') as f:
            cf = json.load(f)

            self.tag_queue = []
            tags = cf['searchByTags']['tags']
            dates = cf['searchByTags']['dates']
            # If no dates specified, add each day of the last week
            if len(dates) == 0:
                for i in range(7):
                    from_date = datetime.datetime.now() - datetime.timedelta(days=i)
                    until_date = from_date + datetime.timedelta(days=1)
                    dates.append({'from': from_date.strftime('%Y-%m-%d'), 'until': until_date.strftime('%Y-%m-%d')})
            languages = cf['searchByTags']['languages']

            # Create grid search queue
            for tag in tags:
                for date in dates:
                    for lang in languages:
                        params = {'tag': tag, 'since': date['from'], 'until': date['until'], 'lang': lang}
                        self.tag_queue.append(params)

            self.tag_output = cf['searchByTags']['output_dir']

            self.handle_queue = []
            handles = cf['searchByHandles']['handles']
            dates = cf['searchByHandles']['dates']
            # If no dates specified, add each day of the last week
            if len(dates) == 0:
                for i in range(7):
                    from_date = datetime.datetime.now() - datetime.timedelta(days=i)
                    until_date = from_date + datetime.timedelta(days=1)
                    dates.append({'from': from_date.strftime('%Y-%m-%d'), 'until': until_date.strftime('%Y-%m-%d')})
            languages = cf['searchByHandles']['languages']

            # Create grid search queue
            for handle in handles:
                for date in dates:
                    for lang in languages:
                        params = {'handle': handle, 'since': date['from'], 'until': date['until'], 'lang': lang}
                        self.handle_queue.append(params)

            self.handle_output = cf['searchByHandles']['output_dir']

            print("Created crawler with {} tasks".format(len(self.tag_queue) + len(self.handle_queue)))

            self.check_dirs()

    def check_dirs(self):
        if not os.path.exists(self.tag_output):
            os.makedirs(self.tag_output)
            print("Created directory {}".format(self.tag_output))
        if not os.path.exists(self.handle_output):
            os.makedirs(self.handle_output)
            print("Created directory {}".format(self.handle_output))

    def run_tag_jobs(self):
        print("Starting tag task..")
        start = time.perf_counter()
        for job in self.tag_queue:
            df = self.twitter.get_tweets_by_tag(**job)
            df.to_csv(os.path.join(self.tag_output, "{}_{}_{}.csv".format(job['tag'], job['since'], job['lang'])))
        stop = time.perf_counter()
        print(f"Finished task in {stop - start:0.4f} seconds")

    def run_handle_jobs(self):
        print("Starting handle task..")
        start = time.perf_counter()
        for job in self.handle_queue:
            df = self.twitter.get_tweets_by_handle(**job)
            df.to_csv(os.path.join(self.handle_output, "{}_{}_{}.csv".format(job['handle'], job['since'], job['lang'])))
        stop = time.perf_counter()
        print(f"Finished task in {stop - start:0.4f} seconds")


if __name__ == '__main__':
    twitter = twitter.twitter_client()
    crawler = Crawler(twitter)
    crawler.run_tag_jobs()
