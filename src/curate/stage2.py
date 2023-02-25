# %% [markdown]
## Data Curation
#   Summary Overview
#   - Merge Parsed Tweets
#   - Cutoff tweets older than 2017
#   - Normalize and clean data
#   - Create dictionaries from words spoken
#   - Create probabilities of words used per user
#   - pivot tweets to show users on same days
#   - merge tweets from weekend to monday

# %% [markdown]
# - Import Libraries
import os, nltk, pandas as pd, numpy as np
from nltk.util import ngrams,everygrams,skipgrams

# %% [markdown]
# - Change Directory to top level folder
top_level_folder = 'twitter_app'
if(os.getcwd().split(os.sep)[-1] != top_level_folder):
    try:
        os.chdir('../..')
        print(f"cwd: {os.getcwd()}", sep = '\n')
    except Exception as e:
        print(f"{e}\n:start current working directory from {top_level_folder}")
        
# %% [markdown]
# - Load tools
from src.curate.curate_tools import clean_text, ngram_probability, merge_files, merge_all, df_to_csv

# %% [markdown]
# - Read in groups of twitter user's to merge
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)
user_df
    
# %% [markdown]
# - Merge Tweets
for group in groups:
    merge_files(group, display = 0)
df_all = merge_all(display = 0)

# %% [markdown]
# - top 5 oldest tweets by user
df_all.groupby('user')['created_at'].min().sort_values(ascending= True).head(5)

# %% [markdown]
# - Bounds to drop old tweets if neccesary
threshold = '2017-01-01'
df_all_upperbound = df_all[df_all.created_at > threshold]

# %% [markdown]
# - non significant words to remove
stopwords = nltk.corpus.stopwords.words("english") 
nonessential_words = ['twitter', 'birds','lists','list', 'source', 'am', 'pm']
stopwords.extend(nonessential_words) # merge two lists together
words_to_remove = sorted(list( dict.fromkeys(stopwords) )) # remove duplicates

# %% [markdown]
# - Cleaning up Data and Creating a Dictionary words spoken
# - removed websites(html/www)
# - usernames 
# - hashtags 
# - digits
# - extra spaces
# - stopwords
cleaned_text = clean_text(df_all_upperbound.text, words_to_remove)
# %%
def n_gram(cleaned_text, n):
           
    grams = pd.Series([list(ngrams(tweet, n)) for tweet in cleaned_text]) 
    
    return grams, grams.value_counts(), grams.value_counts(normalize=True)

# Unigram Gram
# Create a dictionary of word counts and frequency
all_words, unigram_frequency, unigram_relative_frequency = n_gram(cleaned_text, 1)
bigram_words, bigram_frequency, bigram_relative_frequency = n_gram(cleaned_text, 2)
trigram_words, trigram_frequency, trigram_relative_frequency = n_gram(cleaned_text, 3)

# book keeping output
# df_stats = pd.DataFrame({'unigram_frequency':          unigram_frequency,
#                          'unigram_relative_frequency': unigram_relative_frequency,
#                          'bigram_frequency':           bigram_frequency,
#                          'bigram_relative_frequency':  bigram_relative_frequency,
#                          'trigram_frequency':          trigram_frequency,
#                          'trigram_relative_frequency': trigram_relative_frequency})
# df_to_csv(df = df_stats, 
#           folder = f'./data/merge/all_twitter_users', 
#           file = f'/stats.csv')
# %%
# Probabilities of twitter words spoken compared to other tweets

# N Gram
# Unigram
unigram = ngram_probability(unigram_relative_frequency, cleaned_text)
print(f'sum of probability column = {sum(unigram)}')
bigram = ngram_probability(bigram_relative_frequency, cleaned_text)
print(f'sum of probability column = {sum(unigram)}')
trigram = ngram_probability(trigram_relative_frequency, cleaned_text)
print(f'sum of probability column = {sum(unigram)}')

# Bigram
# Trigram

# %%
# Adding probability and frequency to the dataframe
df_all_prob = df_all_upperbound.reset_index()
df_all_prob['unigram_probability'] = unigram_probability
# removing empty tweets ( links removed )
df_all_prob = df_all_prob.dropna()
# Converting timestamp (HH:MM:SS) to Year-month-day to combine users on the same day
df_all_prob.insert(loc = 0, column = 'date', value = pd.to_datetime(df_all_prob['created_at']).apply(lambda x: x.strftime('%Y-%m-%d')))
df_all_prob.date = pd.to_datetime(df_all_prob['date'], format='%Y-%m-%d')
df_all_prob = df_all_prob.sort_values(by=['date'], ascending=False).drop(columns=['index'])

# %%
df_all_prob.head(2)

# %%
print(sum(df_all_prob.unigram_probability / sum(df_all_prob.unigram_probability)))

# %% Merge Users on same dates
df_wide1 = df_all_prob.pivot_table(index='date', values=['favorite_count','retweet_count'], aggfunc='sum',fill_value=0 ).sort_values(by='date',ascending=False)
df_wide2 = df_all_prob.pivot_table(index='date', columns=['user'], values=['unigram_probability'], aggfunc='sum',fill_value=0 ).sort_values(by='date',ascending=False).droplevel(0, axis=1) 
df_wide_merge = pd.merge(df_wide1, df_wide2, how='inner', on='date')

# %% [markdown]
# - Merging Sat/Sun Tweets to Monday and re-merging to data

# %%
# Drop Saturday-Monday And replace with Monday
# week_end_mask = df_wide_merge.reset_index().date.dt.day_name().isin(['Saturday', 'Sunday', 'Monday'])
# week_end = df_wide_merge.reset_index().loc[week_end_mask, :]
# monday_group = week_end.groupby([pd.Grouper(key='date', freq='W-MON')])[df_wide_merge.columns].sum().reset_index('date')
# # Apply the stripped mask
# df_wide_stripped = df_wide_merge.reset_index().loc[~ week_end_mask, :]
# df_wide = pd.merge(df_wide_stripped, monday_group, how='outer').set_index('date')

# %%
df_to_csv(df = df_wide_merge, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_pivot.csv')
df_wide_merge.head(5)
