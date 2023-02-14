# %% [markdown]
## Data Curation
#       Summary Overview
#   -
#   -
#   -


# %% [markdown]
## Import Libraries
import os,sys,tweepy,nltk,pandas as pd,numpy as np, yfinance as yf
from datetime import date
np.random.seed(0)

# %% [markdown]
## Change Directory to root
file = os.getcwd().split(os.sep)
while(file[-1] != 'twitter_app'): # Check the working directory
    os.chdir('..')
    file = os.getcwd().split(os.sep)
    sys.path.append(os.path.abspath(os.getcwd()))
print(f"root directory: {os.getcwd()}", sep = '\n')

## Load Custom Functions
from src.tools.twitter_tools import strip_all_words, sentence_word_probability, download_todays_test,normalize_columns,merge_files, merge_all

# %%
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)
user_df
    
# %% [markdown]
# ## Merge Tweets

# %%
merge = []
for group in groups:
    merge.append(merge_files(group, display = 0))
df_all = merge_all('merge/merged_twitter_users', display = 0)
# %%
df_all.head(2)

# %%
df_all.info(verbose = True, null_counts = None, show_counts=None)

# %% [markdown]
# - Some users have infrequent tweets and span the 3600 limit over 10 years

# %%
df_all.groupby('user')['created_at'].min().sort_values(ascending= True).head(5)

# %% [markdown]
# ## Drop Old Tweets
# - Keep 2017 - 2023

# %%
threshold = '2017-01-01'
df_all_upperbound = df_all[df_all.created_at > threshold]
df_all_upperbound.tail(5)

# %%
# Adding nonessential twitter words to remove
stop = nltk.corpus.stopwords.words("english") 
twitter_nonessential_words = ['twitter', 'birds','lists','list', 'source','just','am','pm'\
                              'a','b','c','d','e','f','g','h','i','j','k','l','m','n',\
                              'n','o','p','q','r','s','t','u','v','w','x','y','z']
stop.extend(twitter_nonessential_words) # merge two lists together
stop = sorted(list( dict.fromkeys(stop) )) # remove duplicates

# %% [markdown]
# ### Create dictionarys of words 
# * Remove unnecessary words
# * Generate frequency of words per sentence

# %%
df_all_words = strip_all_words(df_all_upperbound, stop)
df_all_words_count = df_all_words.explode().replace("", np.nan, regex=True).dropna() # drop NAN's and empty words
all_count = df_all_words_count.value_counts()

# %%
print(f"Tweets of Dictionaries: {len(df_all_words)}")
print(f"all words: {len(df_all_words_count)}")
print(f"Dictionary of all words: {len(all_count)}")

# %% [markdown]
# * Nan are tweets w/ images
# * ',' are words removed with special cases

# %%
print(f"All the words in each individual Sentence:\n{df_all_words[0:5]}")

# %%
print(f"5 words from dictionary of all words:\n{all_count[0:5]}", end='\n\n')

# %%
print(all_count.isna().value_counts())


# %% [markdown]
# ## Probability of individual tweets

# %%
# Probabilities
sentence_list, total_probability, individual_probability = sentence_word_probability(all_count, df_all_words)
print(f'sum of probability column = {sum(total_probability)}')

# %%
df_all_prob = df_all_upperbound.reset_index()
df_all_prob['frequency'] = sentence_list
df_all_prob['probability'] = total_probability
df_all_prob = df_all_prob.dropna()
df_all_prob.insert(loc = 0, column = 'date', value = pd.to_datetime(df_all_prob['created_at']).apply(lambda x: x.strftime('%Y-%m-%d')))
df_all_prob.date = pd.to_datetime(df_all_prob['date'], format='%Y-%m-%d')
df_all_prob = df_all_prob.sort_values(by=['date'], ascending=False).drop(columns=['index'])

# %%
df_all_prob.head(2)

# %%
df_wide1 = df_all_prob.pivot_table(index='date', values=['favorite_count','retweet_count'], aggfunc='sum',fill_value=0 ).sort_values(by='date',ascending=False)
df_wide2 = df_all_prob.pivot_table(index='date', columns=['user'], values=['probability'], aggfunc='sum',fill_value=0 ).sort_values(by='date',ascending=False).droplevel(0, axis=1) 
df_wide_merge = pd.merge(df_wide1, df_wide2, how='inner', on='date')

# %% [markdown]
# - Merging Sat/Sun Tweets to Monday and re-merging to data

# %%
# Drop Saturday-Monday And replace with Monday
week_end_mask = df_wide_merge.reset_index().date.dt.day_name().isin(['Saturday', 'Sunday', 'Monday'])
week_end = df_wide_merge.reset_index().loc[week_end_mask, :]
monday_group = week_end.groupby([pd.Grouper(key='date', freq='W-MON')])[df_wide_merge.columns].sum().reset_index('date')

df_wide_stripped = df_wide_merge.reset_index().loc[~ week_end_mask, :]
df_wide = pd.merge(df_wide_stripped, monday_group, how='outer').set_index('date')
df_wide.head(5)

# %%
path_all_merged_twitter_analysts_pivot = f'./data/merge/all_merged_twitter_users' # Create Folders
if not os.path.exists(path_all_merged_twitter_analysts_pivot):
    os.makedirs(path_all_merged_twitter_analysts_pivot)
df_wide.to_csv(path_all_merged_twitter_analysts_pivot +'/all_merged_twitter_users_pivot.csv', index=True) # Export to csv

df_wide.head(5)
