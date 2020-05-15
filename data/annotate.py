import pandas as pd
from os import listdir
from os.path import isfile, join
import random
import numpy as np


def load_tweets(folder):
    onlyfiles = [f for f in listdir(folder) if isfile(join(folder, f))]
    dfs = []
    for file in onlyfiles:
        p = join(folder, file)
        data = pd.read_csv(p)
        dfs.append(data)
    return pd.concat(dfs)


def add_labels(output_file, tweets, columns=["tweet_id","created_at","name","text","location","tags","label"]):
    total_tweets = len(tweets)
    labeled = {}
    while 1:
        ids = random.randint(0, total_tweets)
        tweet = tweets.iloc[ids]
        while tweet["tweet_id"] in labeled.keys():
            ids = random.randint(0, total_tweets)
            tweet = tweets.iloc[ids]
        print(f"----------------------------------------\nTweet: \n{tweet['text']}\n"
              f"----------------------------------------\n")
        input1 = input("Input 1 for positive tweet or 0 for negative tweet.\nPress s to skip.\nPress q to exit\n")
        if input1 in ['0', '1']:
            labeled[tweet['tweet_id']] = np.concatenate((tweet, [int(input1)]))
        elif input1 == 'q':
            break
        elif input1 == 's':
            print("Skipping tweet")
        else:
            print("Invalid input..")
    labels = pd.DataFrame(list(labeled.values()), columns=columns)

    if not isfile(output_file):
        labels.to_csv(output_file, header=columns, index=False)
    else:  # else it exists so append without writing the header
        labels.to_csv(output_file, mode='a', header=False,  index=False)

if __name__ == '__main__':
    tweets = load_tweets('./tweets')
    add_labels("test_data.csv", tweets)
