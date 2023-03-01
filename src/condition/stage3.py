# %% [markdown]
## Data Condition
#       Summary Overview
#   - pivot tweets to show users on same days
#   - merge tweets from weekend to monday
#   - Min Max normalize columns
#   - Download test of today

# %% [markdown]
## Import Libraries
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
## Load Custom Functions
from src.condition.condition_tools import download_todays_test, normalize_columns, df_to_csv

# %%
path_all_merged_twitter_users_ngram = f'./data/merge/all_twitter_users'
df_all_prob = pd.read_csv(path_all_merged_twitter_users_ngram +'/all_twitter_users_ngram.csv').astype({'date':'datetime64[ns]'})
df_all_prob.head()

# %% Merge Users on same dates
df_wide1 = df_all_prob.pivot_table(index='date', 
                                   values=['favorite_count','retweet_count'], 
                                   aggfunc='sum',
                                   fill_value=0).sort_values(by='date',
                                                            ascending=False)
df_wide2 = df_all_prob.pivot_table(index='date', 
                                   columns=['user'],
                                   values=['unigram_probability','bigram_probability'], 
                                   aggfunc={'unigram_probability': 'sum',
                                            'bigram_probability': 'sum'},
                                   fill_value=0 ).sort_values(by='date',
                                                              ascending=False).droplevel(0, axis=1) 
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

# %%
path_all_merged_twitter_users_pivot = f'./data/merge/all_twitter_users'
df_wide = pd.read_csv(path_all_merged_twitter_users_pivot +'/all_twitter_users_pivot.csv').astype({'date':'datetime64[ns]'})
df_wide.head()
# %%
with open(os.path.normpath(os.getcwd() + './user_input/ticker_list.xlsx'), 'rb') as f:
    ticker_df = pd.read_excel(f, sheet_name='ticker_sheet')
    ticker_df = ticker_df.where(pd.notnull(ticker_df), '')
    f.close()
ticker_df.head(10)

# %%
# downloding index fund's or stock tickers  #.resample('D').ffill()
how_far_back = df_wide.index.min().date()
today = date.today()
column_names = dict(zip(ticker_df.ticker_name, ticker_df.ticker_label))
column_names['Date']='date'
stock_list = list(ticker_df.ticker_name)
stock_str = ' '.join( stock_list )

index_funds_df = yf.download(stock_str, how_far_back, today, interval = '1d', progress=False)['Close'].reset_index('Date').rename(columns=column_names)

convert_dict = dict(zip(ticker_df.ticker_label, ['float64']*len(ticker_df.ticker_label)))
convert_dict['date'] = 'datetime64[ns]'
index_funds_df = index_funds_df.astype(convert_dict)

print(f'{how_far_back} -> {today}')

# %%
path_index_funds_merge = f'./data/merge/all_merged_index_funds' # Create Folders
if not os.path.exists(path_index_funds_merge):
    os.makedirs(path_index_funds_merge)
index_funds_df.to_csv(path_index_funds_merge +'/all_merged_index_funds.csv', index=False) # Export to csv
index_funds_df.head(5)

# %%
# Merging the probabilities of words used from twitter and database of index funds
df_merge = pd.merge(index_funds_df, df_wide, how='inner', on='date').set_index('date').fillna(0)
df_merge_original = df_merge.copy()

columns = list(ticker_df.ticker_label) + ['favorite_count', 'retweet_count']
df_merge = normalize_columns(df_merge.copy(), columns)
df_merge.tail(5)

# %%
path_twitter_and_index_fund = f'./data/merge/combined'
if not os.path.exists(path_twitter_and_index_fund):
    os.makedirs(path_twitter_and_index_fund)
df_merge.to_csv(path_twitter_and_index_fund +'/index_funds_and_twitter_analysts.csv') # Export to csv

# %%
path_twitter_and_index_fund = f'./data/merge/combined'
df_merge = pd.read_csv(path_twitter_and_index_fund +'/index_funds_and_twitter_analysts.csv').set_index('date')

# %% [markdown]
# ## Prepare Today's Test

# %%
# Todays Data
todays_test = download_todays_test(ticker_df, df_wide, df_merge_original)
path_todays_test = f'./data/merge/combined'
if not os.path.exists(path_todays_test):
    os.makedirs(path_todays_test)
todays_test.to_csv(path_todays_test +'/todays_test.csv') # Export to csv
# %%
