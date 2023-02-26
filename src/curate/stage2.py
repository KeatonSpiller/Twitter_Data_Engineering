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
import os, nltk, pandas as pd, numpy as np, timeit

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
from src.curate.curate_tools import clean_text, merge_files, merge_all, df_to_csv, n_gram, unigram_probability, bigram_probability

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
df_to_csv(df = cleaned_text, 
          folder = f'./data/merge/all_twitter_users/stats', 
          file = f'/cleaned_text.csv')


# %%
# N gram, frequency, and relative frequency
unigram_sentence, unigram_frequency, unigram_relative_frequency = n_gram(cleaned_text, 1)
bigram_sentence, bigram_frequency, bigram_relative_frequency = n_gram(cleaned_text, 2)
trigram_sentence, trigram_frequency, trigram_relative_frequency = n_gram(cleaned_text, 3)

# %%
# N Gram probability
unigram_prob = unigram_probability(cleaned_text, unigram_relative_frequency)
bigram_prob = bigram_probability(bigram_sentence, unigram_frequency, bigram_frequency)
# %%
# unigram_prob.reset_index(drop=True)
bigram_prob
# %%
# Adding probability and frequency to the dataframe
df_all_prob = df_all_upperbound.reset_index()
df_all_prob['unigram_probability'] = unigram_prob.reset_index(drop=True)
df_all_prob['bigram_probability'] = bigram_prob
# removing empty tweets ( links removed )
df_all_prob = df_all_prob.dropna()
# Converting timestamp (HH:MM:SS) to Year-month-day to combine users on the same day
df_all_prob.insert(loc = 0, column = 'date', value = pd.to_datetime(df_all_prob['created_at']).apply(lambda x: x.strftime('%Y-%m-%d')))
df_all_prob.date = pd.to_datetime(df_all_prob['date'], format='%Y-%m-%d')
df_all_prob = df_all_prob.sort_values(by=['date'], ascending=False).drop(columns=['index'])

# %%
df_all_prob.head(2)

# %% Merge Users on same dates
df_wide1 = df_all_prob.pivot_table(index='date', values=['favorite_count','retweet_count'], aggfunc='sum',fill_value=0 ).sort_values(by='date',ascending=False)
df_wide2 = df_all_prob.pivot_table(index='date', columns=['user'], values=['unigram_probability','bigram_probability'], aggfunc='sum',fill_value=0 ).sort_values(by='date',ascending=False).droplevel(0, axis=1) 
df_wide = pd.merge(df_wide1, df_wide2, how='inner', on='date')

# %%
df_to_csv(df = df_wide, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_pivot.csv')
df_wide.head(5)

# # %% [markdown]
# # - To combine Sat/Sun Tweets with Monday
# week_end_mask = df_wide.reset_index().date.dt.day_name().isin(['Saturday', 'Sunday', 'Monday'])
# week_end = df_wide.reset_index().loc[week_end_mask, :]
# monday_group = week_end.groupby([pd.Grouper(key='date', freq='W-MON')])[df_wide.columns].sum().reset_index('date')
# # Apply the stripped mask
# df_wide_stripped = df_wide.reset_index().loc[~ week_end_mask, :]
# df_wide_wknd_merge = pd.merge(df_wide_stripped, monday_group, how='outer').set_index('date')

# # %%
# df_to_csv(df = df_wide_wknd_merge, 
#           folder = f'./data/merge/all_twitter_users', 
#           file = f'/all_twitter_users_pivot_wkd_merge.csv')
# df_wide_wknd_merge.head(5)
# # %%