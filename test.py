import math
import numpy as np
import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt

from collections import defaultdict
from scipy.stats.stats import pearsonr

min_date = np.datetime64('2006-07-15 00:00:00')
max_date = np.datetime64('2020-12-31 23:59:59')

def get_indicators_csv():
    df_tweets = pd.read_csv('./dataset/tweets.csv', sep=',', index_col=0)  # load tweets
    # create a dataframe for storing indicators
    # current indicators: total number of tweets, average tweet length, total number of likes, like ratio per tweet
    df_indicators = pd.DataFrame(columns=['tweet_count', 'avg_tweet_len', 'total_num_of_likes', 'like_ratio_per_tweet', 'years_not_valid'])
    
    # setting the index column name to 'id'
    df_indicators.index.names = ['user_id']

    # iterating on tweets
    for id_tweet, tweet in df_tweets.iterrows():
        user_id = np.int64(tweet['user_id'])
        
    # if a user published a tweet and is not into the dataframe
        if user_id not in df_indicators.index:
            avg_tweet_len = len(str(tweet['text']))
            number_of_likes = tweet['favorite_count']
            # tweet count is set to 1 and the average length is the length of the sole tweet published

            #Assign to the indicator dataframe
            df_indicators.at[user_id, 'tweet_count'] = 1
            df_indicators.at[user_id, 'avg_tweet_len'] = avg_tweet_len
            df_indicators.at[user_id, 'total_num_of_likes'] = np.int64(number_of_likes)
            df_indicators.at[user_id, 'years_not_valid'] = 0

            if df_tweets.created_at < min_date or df_tweets.created_at > max_date:
                df_indicators.at[user_id, 'years_not_valid'] = 1
             
            

        # if a user published a tweet and is into the dataframe
        else:
            previous_tweet_count = df_indicators.at[user_id, 'tweet_count']
            previous_avg = df_indicators.at[user_id, 'avg_tweet_len']

            # summing the previous average multiplied for n/n+1 with the current tweet length multiplied for
            # 1/n+1 gives us the current average where n is the previous tweet count
            avg_tweet_len = previous_avg * (previous_tweet_count / (previous_tweet_count + 1)) \
                            + len(str(tweet['text'])) * (1 / (previous_tweet_count + 1))

            number_of_likes = tweet['favorite_count']

            #Update to the indicator dataframe
            df_indicators.at[user_id, 'tweet_count'] += 1
            df_indicators.at[user_id, 'avg_tweet_len'] = avg_tweet_len
            df_indicators.at[user_id, 'total_num_of_likes'] += np.int64(number_of_likes)

            if df_tweets.created_at < min_date or df_tweets.created_at > max_date:
                df_indicators.at[user_id, 'years_not_valid'] += 1


    # ratios
    for id_user, user in df_indicators.iterrows():
        # tweet count is always >0, so there is no risk of a zero division
        user['like_ratio_per_tweet'] = user['total_num_of_likes'] / user['tweet_count']

    df_indicators.to_csv('./dataset/users_avg_tweet_len.csv')

def get_entropy_from_timedeltas(list_of_timedeltas,user_id):
    #The probability of a timedelta to appear is the number of times the unique timedelta has appeared over total number of times timedeltas appeared
    total_number_of_timedeltas = sum(list_of_timedeltas.values()) #the total number of times timedeltas appeared
    entropy = 0. #entropy set to 0.
    for timedelta in list_of_timedeltas:
        number_of_timedelta = list_of_timedeltas[timedelta] #the number of times the unique timedelta has appeared
        entropy -= number_of_timedelta/total_number_of_timedeltas * np.log2(number_of_timedelta/total_number_of_timedeltas) #shannon's entropy

    return entropy

def get_entropy_over_time():
    #Subtract from each datetime the previous datetime, obtaining the timedeltas.
    #Calculate the entropy on those timedeltas (if the timedeltas are the same, the entropy will be lower)
    #The threshold is needed to consider a meaningful number of tweets for applying the entropy
    entropy_threshold = 2

    last_tweet_encountered = dict() #a dict where the key is user_id and the value is the last post datetime64. It is needed to get the timedelta between posts
    tweet_timedeltas = dict() #a dict where the key is user_id and the value is a dict containing the timedeltas:number_of_times_timedelta_has_shown
    user_entropy_list = dict() # The return of the function; a dict where the key is user_id and the value is the entropy of the user

    df_tweets = pd.read_csv('./dataset/tweets_sample.csv', sep=',', index_col=0)  # load tweets
    df_tweets.created_at = pd.to_datetime(df_tweets.created_at, errors='coerce') # convert created_at to datetime
    df_tweets.sort_values(by="created_at",inplace=True) # is needed to be sorted by date in order to be able to subtract the previous date from the current

    for id_tweet, tweet in df_tweets.iterrows(): #iterating on rows
        try: #if user_id cannot be casted into int64 it skips the tweet
            user_id = np.int64(tweet['user_id'])
        except:
            continue

        if user_id not in tweet_timedeltas.keys(): # if user is not into the timedeltas
            last_tweet_encountered[user_id] = tweet.created_at #the first datetime is saved
            tweet_timedeltas[user_id] = dict() #the dict containing timedelta:number_of_times_timedelta_has_shown is created
        else:
            timedelta = tweet.created_at - last_tweet_encountered[user_id] #subrtacting the previous datetime64 to the current datetime64 gives the timedelta between the 2
            last_tweet_encountered[user_id] = tweet.created_at # the last datetime64 is saved
            if timedelta not in tweet_timedeltas[user_id]: # if the timedelta is not in the dict containing timedelta:number_of_times_timedelta_has_shown
                tweet_timedeltas[user_id][timedelta] = 1 # it is the first timedelta, so it has appeared only 1 time
            else:
                tweet_timedeltas[user_id][timedelta] += 1 # it has already appeared, so the number of times encountered increases by 1
    
    for user_id in tweet_timedeltas:
        if len(tweet_timedeltas[user_id]) < entropy_threshold: #if the number of tweets encountered is below the threshold
            user_entropy_list[user_id] = 0. #the entropy is set to 0 (can be also set to None)
        else:
            user_entropy_list[user_id] = get_entropy_from_timedeltas(tweet_timedeltas[user_id],user_id) #the entropy is calculated

    return user_entropy_list

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
    dict_of_entropies = get_entropy_over_time()
    #print("User 403782011 entropy = ",dict_of_entropies[403782011])
    #for user_id in dict_of_entropies:
     #   if dict_of_entropies[user_id] is not None:
      #      if dict_of_entropies[user_id] < 2:
       #         print("User:",user_id,"entropy:",dict_of_entropies[user_id])