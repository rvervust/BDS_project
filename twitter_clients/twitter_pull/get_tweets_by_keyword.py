from twitter import oldtweets_client
import argparse
import json
import sys
from os import mkdir
from os.path import join, exists
from datetime import date, timedelta, datetime
parser = argparse.ArgumentParser(description='Get tweets by a given keyword and date')
parser.add_argument('--config',
                   help='sum the integers (default: find the max)')


if __name__ == '__main__':
    args = parser.parse_args()
    client = oldtweets_client()
    with open(args.config,'r') as configfile:
        cf = json.load(configfile)

        for task in cf:
            out_folder = task['output']
            try:
                mkdir(out_folder)
            except FileExistsError:
                print(f"{out_folder} already exists")
            from_data = datetime.strptime(task['from'], '%Y-%m-%d')
            to_date = datetime.strptime(task['until'], '%Y-%m-%d')
            delta = to_date - from_data  # as timedelta
            dates = [(from_data + timedelta(days=i)).strftime('%Y-%m-%d''') for i in range(delta.days + 1)]
            for i in range(len(dates) - 1):
                fd = dates[i]
                td = dates[i + 1]
                outfile = join(out_folder,f"{fd}_{td}.csv")
                if exists(outfile):
                    continue
                else:
                    print("Getting tweets: ", task['keyword'], fd, td)
                    try:
                        df = client.get_tweets_by_tag(task['keyword'], since=fd , until=td, count=task['max_tweets'])
                        df.to_csv(outfile, index=False)
                    except Exception as e:
                        print("exception",e)
    sys.exit(7896)