# %% [markdown]
## Data Condition
#       Summary Overview
#   - normalize probabilities
#   - pivot tweets to show users on same days
#   - merge tweets from weekend to monday
#   - merge yahoo finance index funds with tweet probabilities
#   - Min Max normalize index funds and tweet retweet/favorite counts
#   - Download tweets and index prices today

# %% [markdown]
# - Import Libraries
import os,pandas as pd,numpy as np, yfinance as yf
from datetime import date
np.random.seed(0)

# %% [markdown]
# - Change Directory to top level folder
top_level_folder = 'twitter_app'
if(os.getcwd().split(os.sep)[-1] != top_level_folder):
    try:
        os.chdir('../..')
        print(f"cwd: {os.getcwd()}", sep = '\n')
    except Exception as e:
        print(f"{e}\n:start current working directory from {top_level_folder}")
        
# %% 
# - Load Custom Functions
from src.condition.condition_tools import download_todays_test, normalize_columns, df_to_csv

# %%
path_all_merged_twitter_users_ngram = f'./data/merge/all_twitter_users'
df_all_prob = pd.read_csv(path_all_merged_twitter_users_ngram +'/all_twitter_users_ngram.csv').astype({'date':'datetime64[ns]'})

# which user's probabilities
print(df_all_prob.user.value_counts())
print(len(df_all_prob.user.unique()))

# %% [markdown]
# - normalize probability column to 1 
df_all_prob_norm = df_all_prob
df_all_prob_norm.unigram_probability = df_all_prob.unigram_probability / df_all_prob.unigram_probability.sum()
df_all_prob_norm.bigram_probability = df_all_prob.bigram_probability / df_all_prob.bigram_probability.sum()

# removing empty tweets
# df_all_prob_norm = df_all_prob_norm.dropna(subset=['unigram_probability','bigram_probability'],axis=0)
# print(df_all_prob_norm.user.value_counts())

# - Threshold tweets
if False:
    threshold = '2017-01-01'
    df_all_prob_norm = df_all_prob_norm[df_all_prob_norm.created_at > threshold]

# Any Inter quantile data to remove?
# Upper 95% or lower 5%
# Extrema

df_to_csv(df = df_all_prob_norm, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_pivot_norm.csv')
df_all_prob_norm.head(5)

# %% Merge Users on same dates
df_wide1 = df_all_prob_norm.pivot_table(index='date', 
                                   values=['favorite_count','retweet_count'], 
                                   aggfunc='sum',
                                   fill_value=0).sort_values(by='date',
                                                            ascending=False)
df_wide2 = df_all_prob_norm.pivot_table(index='date', 
                                   columns=['user'],
                                   values=['unigram_probability','bigram_probability'], 
                                   aggfunc={'unigram_probability': 'sum',
                                            'bigram_probability': 'sum'},
                                   fill_value=0 ).sort_values(by='date',
                                                              ascending=False)#.droplevel(0, axis=1) 
df_wide2.columns = pd.Series(['_'.join(str(s).strip() for s in col if s) for col in df_wide2.columns]).str.replace("probability_", "", regex=True)
df_wide = pd.merge(df_wide1, df_wide2, how='inner', on='date')
df_to_csv(df = df_wide, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_pivot.csv')
df_wide.head(5)

# %% [markdown]
# - To combine Sat/Sun Tweets with Monday
week_end_mask = df_wide.reset_index().date.dt.day_name().isin(['Saturday', 'Sunday', 'Monday'])
week_end = df_wide.reset_index().loc[week_end_mask, :]
monday_group = week_end.groupby([pd.Grouper(key='date', freq='W-MON')]).sum().reset_index('date')
# Apply the stripped mask
df_wide_stripped = df_wide.reset_index().loc[~ week_end_mask, :]
df_wide_wknd_merge = pd.merge(df_wide_stripped, monday_group, how='outer').set_index('date').sort_index(ascending=False)
df_wide_wknd_merge
df_to_csv(df = df_wide_wknd_merge, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_pivot_wkd_merge.csv')
df_wide_wknd_merge.head(5)

# %% [markdown]
# - Read in Ticker labels
with open(os.path.normpath(os.getcwd() + './user_input/ticker_list.xlsx'), 'rb') as f:
    ticker_df = pd.read_excel(f, sheet_name='ticker_sheet')
    ticker_df = ticker_df.where(pd.notnull(ticker_df), '')
    f.close()
ticker_df.head(10)

# %% [markdown]
# - Downloding stock tickers values
how_far_back = df_wide.index.min().date()
today = date.today()
print(f'{how_far_back} -> {today}')
column_names = dict(zip(ticker_df.ticker_name, ticker_df.ticker_label))
column_names['Date']='date'
stock_list = list(ticker_df.ticker_name)
stock_str = ' '.join( stock_list )
# downloading stock tickers from list
index_funds_df = yf.download(stock_str, how_far_back, today, interval = '1d', progress=False)['Close'].reset_index('Date').rename(columns=column_names).fillna(0)
# converting to float and aligning date to the same data type
convert_dict = dict(zip(ticker_df.ticker_label, ['float64']*len(ticker_df.ticker_label)))
convert_dict['date'] = 'datetime64[ns]'
index_funds_df = index_funds_df.astype(convert_dict)

# %%
# Export index funds
df_to_csv(df = index_funds_df, 
          folder = f'./data/merge/all_merged_index_funds', 
          file = f'/all_merged_index_funds.csv')
index_funds_df.head(5)

# %%
# Merging twitter probabilities and ticker prices
df_merge = pd.merge(index_funds_df, df_wide_wknd_merge, how='inner', on='date').set_index('date').fillna(0)
df_merge_original = df_merge.copy()
# Min Max normalize ticker columns and favorite/retweet counts
columns = list(ticker_df.ticker_label) + ['favorite_count', 'retweet_count']
df_merge = normalize_columns(df_merge.copy(), columns)
df_merge.tail(5)
# Export twitter index fund merge
df_to_csv(df = df_merge, 
          folder = f'./data/merge/combined', 
          file = f'/tickers_and_twitter_users.csv')
index_funds_df.head(5)

# %%
# Latest Data
todays_test = download_todays_test(ticker_df, df_wide_wknd_merge, df_merge_original)
# Export latest test
df_to_csv(df = todays_test, 
          folder = f'./data/merge/combined', 
          file = f'/todays_test.csv')
if(len(todays_test) > 0):
    todays_test.head(5)
# %%
