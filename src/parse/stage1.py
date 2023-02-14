# %% [markdown]
## Parse Data
#       Summary Overview
#   - Using Tweepy API to parse data from Twitter API
#   - 200 chunks of tweets for 3600 tweets for each User in user_input
#   - Includes some data conditioning and curating with regex


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

# %% [markdown]
## Load Custom Functions
from src.tools.twitter_tools import user_download, twitter_authentication

# %% [markdown]
# # Twitter API Credentials
# Read in keys from a csv file
autentication_path = os.path.abspath('./user_input/authentication_tokens.csv')
api = twitter_authentication(autentication_path)

# %% [markdown]
# # Load Twitter Usernames
# * Unvarified user's are not a problem, no one user can have the same ID
#     
# | Removed User's | reason | 
# | ------------ | ------------- |
# |DayTradeWarrior|account removed from site|
# |elonmusk|privated account|

# %%
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)
user_df

# %% [markdown]
# ## Download Tweets
#     * Download User tweets into csv spreadsheets 
# 
# - ( Tweepy limit of 3600 tweets per user )
#     
# %%
for group in groups:
    print(f"\n{group}:\n")
    users = list(user_df[group][user_df[group]!= ''])
    user_download(api, users, group)
    print(f"")