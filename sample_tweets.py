import pandas as pd

tweets_csv_pth = './dataset/tweets.csv'

df_tweets = pd.read_csv(tweets_csv_pth, index_col=0)#.set_index('id')

print(df_tweets.head(10))
print('\n')
print(df_tweets.tail(10))

# Generate a sample with 1% of random data
df_sample = df_tweets.sample(frac=0.01, random_state=9)
df_sample.to_csv('./dataset/tweets_sample.csv')