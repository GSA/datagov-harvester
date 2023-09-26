import requests
from airflow.decorators import task

# import snscrape.modules.twitter as sntwitter
# import pandas as pd
# from transform import transform_data


# Creating list to append tweet data to
def extract_data():
    # scrape tweets and append to a list
    for i, tweet in enumerate(
        sntwitter.TwitterSearchScraper("Chatham House since:2023-01-14").get_items()
    ):
        if i > 1000:
            break
        tweets_list.append(
            [
                tweet.date,
                tweet.user.username,
                tweet.rawContent,
                tweet.sourceLabel,
                tweet.user.location,
            ]
        )

        # convert tweets into a dataframe
    tweets_df = pd.DataFrame(
        tweets_list, columns=["datetime", "username", "text", "source", "location"]
    )

    # save tweets as csv file

    transform_data(tweets_df)
