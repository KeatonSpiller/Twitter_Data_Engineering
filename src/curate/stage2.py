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
## Import Libraries
import os,sys,nltk,pandas as pd,numpy as np, texthero
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
from src.curate.curate_tools import clean_text, sentence_word_probability, merge_files, merge_all, df_to_csv, sentence_word_probability_original

# %% [markdown]
# ### Read in groups of twitter user's to merge
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)
user_df
    
# %% [markdown]
# ## Merge Tweets
for group in groups:
    merge_files(group, display = 0)
df_all = merge_all(display = 0)

# %% [markdown]
# - top 5 oldest tweets by user
df_all.groupby('user')['created_at'].min().sort_values(ascending= True).head(5)

# %% [markdown]
# ## Bounds to drop old tweets if neccesary
threshold = '2017-01-01'
df_all_upperbound = df_all[df_all.created_at > threshold]

# %% [markdown]
# ## non significant words to remove
stopwords = nltk.corpus.stopwords.words("english") 
nonessential_words = ['twitter', 'birds','lists','list', 'source', 'am', 'pm']
stopwords.extend(nonessential_words) # merge two lists together
words_to_remove = sorted(list( dict.fromkeys(stopwords) )) # remove duplicates

# %% [markdown]
# - Cleaning up Data and Creating a Dictionary of all words
# - remove website(html/www), username, hashtag, digits, extra spaces, and stopwords
cleaned_text = clean_text(df_all_upperbound.text, words_to_remove)
# flatten all words into one list, drop any empty entries
all_words = cleaned_text.explode().replace("", np.nan, regex=True).dropna() # drop NAN's and empty words
# Create a dictionary of list of all words
unique_words = all_words.value_counts()
relative_frequency = all_words.value_counts(normalize=True)
print(f"Original tweets of length: {len(cleaned_text)}")
print(f"all words: {len(all_words)}")
print(f"all unique words: {len(unique_words)}")
# %%
# Probabilities of twitter words spoken compared to every other tweet
# sentence_list, total_probability, individual_probability = sentence_word_probability(unique_words, cleaned_text)
total_probability = sentence_word_probability(relative_frequency, cleaned_text)
print(total_probability)
total_probability2 = sentence_word_probability_original(unique_words, list(cleaned_text))
print(total_probability2)
print(f'sum of probability column = {sum(total_probability)}')

# %%
# Adding probability and frequency to the dataframe
df_all_prob = df_all_upperbound.reset_index()
df_all_prob['frequency'] = relative_frequency
# df_all_prob['frequency'] = sentence_list
df_all_prob['probability'] = total_probability
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
df_wide2 = df_all_prob.pivot_table(index='date', columns=['user'], values=['probability'], aggfunc='sum',fill_value=0 ).sort_values(by='date',ascending=False).droplevel(0, axis=1) 
df_wide_merge = pd.merge(df_wide1, df_wide2, how='inner', on='date')

# %% [markdown]
# - Merging Sat/Sun Tweets to Monday and re-merging to data

# %%
# Drop Saturday-Monday And replace with Monday
week_end_mask = df_wide_merge.reset_index().date.dt.day_name().isin(['Saturday', 'Sunday', 'Monday'])
week_end = df_wide_merge.reset_index().loc[week_end_mask, :]
monday_group = week_end.groupby([pd.Grouper(key='date', freq='W-MON')])[df_wide_merge.columns].sum().reset_index('date')
# Apply the stripped mask
df_wide_stripped = df_wide_merge.reset_index().loc[~ week_end_mask, :]
df_wide = pd.merge(df_wide_stripped, monday_group, how='outer').set_index('date')

# %%
df_to_csv(df = df_wide, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_pivot.csv')
df_wide.head(5)
