# %% [markdown]
## Data Condition
#       Summary Overview
#   - Min Max normalize columns
#   - Download test of today
#   -

# %% [markdown]
## Import Libraries
import os,sys,tweepy,nltk,pandas as pd,numpy as np, yfinance as yf
from datetime import date
np.random.seed(0)

## Change Directory to root
file = os.getcwd().split(os.sep)
while(file[-1] != 'twitter_app'): # Check the working directory
    os.chdir('..')
    file = os.getcwd().split(os.sep)
    sys.path.append(os.path.abspath(os.getcwd()))
print(f"root directory: {os.getcwd()}", sep = '\n')

## Load Custom Functions
from src.condition.condition_tools import download_todays_test,normalize_columns

# %% [markdown]
#     Load pivot data

# %%
path_all_merged_twitter_analysts_pivot = f'./data/merge/all_merged_twitter_users'
df_wide = pd.read_csv(path_all_merged_twitter_analysts_pivot +'/all_merged_twitter_users_pivot.csv').astype({'date':'datetime64[ns]'}).set_index('date')

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
