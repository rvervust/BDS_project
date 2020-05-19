import os
import time
"""
This is simply a workaround to use the oldtweets3 package, the library does not provide exception handling.
Instead it prints a message and calls system.exit()
"""


if __name__ == '__main__':
    backoff = 60 # 1 minute
    ret = os.system("python get_tweets_by_keyword.py --config keywords_config.json")
    while ret != 896:
        print(f"Script timed out, sleeping for {backoff/60} minutes")
        time.sleep(backoff)
        ret = os.system("python get_tweets_by_keyword.py --config keywords_config.json")
