import math
import numpy as np
import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt

from collections import defaultdict
from scipy.stats.stats import pearsonr


def _average_tweet_len():  # v1
    df_users = pd.read_csv('./dataset/users.csv', sep=',', index_col=0)  # load users
    df_tweets = pd.read_csv('./dataset/tweets.csv', sep=',', index_col=0)  # load tweets
    # create a df containing the avg_tweet_len, after that it will be joined with the user df
    # the df is created using the same ids of the users
    df_avg_tweet_len = pd.DataFrame(index=df_users.index, columns=['avg_tweet_len'])

    # for each user
    for user_id in df_avg_tweet_len.index:
        user_average_tweet_length = 0
        # get their tweets
        user_tweets = df_tweets[df_tweets['user_id'] == str(user_id)]  # just a view
        if len(user_tweets) > 0:  # if they have more than 0 tweets
            for tweet_text in user_tweets['text']:
                # 'str' is used for avoiding errors if any non string data is encountered
                user_average_tweet_length += len(str(tweet_text)) / len(user_tweets)
        # using df.at to save the value related to the user
        df_avg_tweet_len.at[user_id, 'avg_tweet_len'] = user_average_tweet_length
    # saving into a csv for future merging
    df_avg_tweet_len.to_csv('./dataset/users_avg_tweet_len.csv')


def average_tweet_len():  # v2
    df_tweets = pd.read_csv('./dataset/tweets.csv', sep=',', index_col=0)  # load tweets
    # create a dataframe for storing tweet counts and average tweet length per user
    df_avg_tweet_len = pd.DataFrame(columns=['tweet_count', 'avg_tweet_len'])
    # setting the index column name to 'id'
    df_avg_tweet_len.index.names = ['id']

    # iterating on tweets
    for id_tweet, tweet in df_tweets.iterrows():
        user_id = tweet['user_id']

        # if a user published a tweet and is not into the dataframe
        if user_id not in df_avg_tweet_len.index:
            avg_tweet_len = len(str(tweet['text']))
            # tweet count is set to 1 and the average length is the length of the sole tweet published
            df_avg_tweet_len.loc[user_id] = 1, avg_tweet_len

        # if a user published a tweet and is into the dataframe
        else:
            previous_tweet_count = df_avg_tweet_len.at[user_id, 'tweet_count']
            previous_avg = df_avg_tweet_len.at[user_id, 'avg_tweet_len']
            # summing the previous average multiplied for n/n+1 with the current tweet length multiplied for 1/n+1 gives
            # us the current average where n is the previous tweet count
            avg_tweet_len = previous_avg * (previous_tweet_count / (previous_tweet_count + 1)) \
                            + len(str(tweet['text'])) * (1 / (previous_tweet_count + 1))

            df_avg_tweet_len.at[user_id, 'tweet_count'] += 1
            df_avg_tweet_len.at[user_id, 'avg_tweet_len'] = avg_tweet_len

    df_avg_tweet_len.to_csv('./dataset/users_avg_tweet_len.csv')


if __name__ == '__main__':
    average_tweet_len()
