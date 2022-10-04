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

def get_indicators_csv():  # v3
    df_tweets = pd.read_csv('./dataset/tweets.csv', sep=',', index_col=0)  # load tweets
    # create a dataframe for storing indicators
    # current indicators: total number of tweets, average tweet length, total number of likes, like ratio per tweet
    df_indicators = pd.DataFrame(columns=['tweet_count', 'avg_tweet_len', 'total_num_of_likes', 'like_ratio_per_tweet'])
    # setting the index column name to 'id'
    df_indicators.index.names = ['id']

    # iterating on tweets
    for id_tweet, tweet in df_tweets.iterrows():
        user_id = tweet['user_id']

        #Analisi/Pulizia dei dati prima dell'estrazione degli indicatori, per tenere traccia dei dati non validi
        if str.isdigit(str(user_id)):
            user_id = int(user_id)

            # if a user published a tweet and is not into the dataframe
            if user_id not in df_indicators.index:
                avg_tweet_len = len(str(tweet['text']))
                number_of_likes = tweet['favorite_count']
                # tweet count is set to 1 and the average length is the length of the sole tweet published

                df_indicators.at[user_id, 'tweet_count'] = 1
                df_indicators.at[user_id, 'avg_tweet_len'] = avg_tweet_len

                if str.isdigit(str(number_of_likes)):  # Vedi analisi sopra
                    df_indicators.at[user_id, 'total_num_of_likes'] = int(number_of_likes)
                else:
                    df_indicators.at[user_id, 'total_num_of_likes'] = 0

            # if a user published a tweet and is into the dataframe
            else:
                previous_tweet_count = df_indicators.at[user_id, 'tweet_count']
                previous_avg = df_indicators.at[user_id, 'avg_tweet_len']

                # summing the previous average multiplied for n/n+1 with the current tweet length multiplied for
                # 1/n+1 gives us the current average where n is the previous tweet count
                avg_tweet_len = previous_avg * (previous_tweet_count / (previous_tweet_count + 1)) \
                                + len(str(tweet['text'])) * (1 / (previous_tweet_count + 1))

                number_of_likes = tweet['favorite_count']

                df_indicators.at[user_id, 'tweet_count'] += 1
                df_indicators.at[user_id, 'avg_tweet_len'] = avg_tweet_len

                if str.isdigit(str(number_of_likes)):
                    df_indicators.at[user_id, 'total_num_of_likes'] += int(number_of_likes)

    # ratios
    for id_user, user in df_indicators.iterrows():
        # tweet count is always >0, so there is no risk of a zero division
        user['like_ratio_per_tweet'] = user['total_num_of_likes'] / user['tweet_count']

    df_indicators.to_csv('./dataset/users_avg_tweet_len.csv')

def testing():
    #Testinggggggggggg
    #df_tweets1 = df_tweets.head(5)
    #new_df_tweets = df_tweets1.drop(df_tweets1[pd.to_numeric(df_tweets1['user_id'], errors='coerce').isnull()].index) # Tweets where the user id is not a number
    #new_df_tweets['user_id'] = pd.to_numeric(new_df_tweets['user_id'], errors='coerce')
    #print(new_df_tweets.convert_dtypes().dtypes)
    #new_df_tweets.info()
    #df_tweets['text'].isnull()
    #df_tweets.info()
    #print(type(df_tweets.at['461498835362013185','user_id'])) # check the type of a cell
    #df_tweets.astype({'user_id':int64},errors='ignore').convert_dtypes().info()#[df_tweets['user_id'].apply(lambda x: isinstance(x, int64))]
    pass


if __name__ == '__main__':
    df_indicators = pd.DataFrame(columns=['ciao'])
    df_indicators.at[1, 'ciao'] = 'aaaaaaaaaaaa bbbbbbbbbbbbbb cccccccccccccc'

    df_indicators.info()
    #get_indicators_csv()
