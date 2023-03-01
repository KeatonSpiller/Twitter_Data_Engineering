# %% [markdown]
## Parse Data
#       Summary Overview
#   - Using Tweepy API to parse data from Twitter API
#   - 200 chunks of tweets for 3200 tweets for each User in user_input
#   - strips emoji's, hashtags, usernames, and website links into seperate columns

# %% [markdown]
## Import Libraries
import os,sys,pandas as pd,numpy as np, glob, shutil
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
  
# %% [markdown]
## Load Custom Functions
from src.parse.parse_tools import user_download, twitter_authentication

# %% [markdown]
# # Twitter API Credentials
# Read in keys from a csv file
autentication_path = os.path.abspath('./user_input/authentication_tokens.csv')
api = twitter_authentication(autentication_path)

# %% [markdown]
# # Load Twitter Usernames   
# * Accounts may be privated or removed and change ability to download
# * No two users can have the same id

# %%
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)
user_df

# %%
# If group column is removed from excel list remove folder
groups_dir = glob.glob(os.path.normpath(f'./data/users/*'))
if(len(groups_dir) > 0):
    for g in groups_dir:
        if((g.split(os.sep)[-1]) not in groups):
            shutil.rmtree(f'.{os.sep+g}')
            
# %% [markdown]
# ## Download Tweets
#     * Download User tweets into csv spreadsheets
#     * 3200 limit, adds to previously downloaded files

for group in groups:
    print(f"\n{group}:\n")
    # grab all user's from columns with user's
    users = list(user_df[group][user_df[group]!= ''])
    user_download(api, users, group, display = 'full')
    print(f"")
print('Twitter user download complete')
# %%
