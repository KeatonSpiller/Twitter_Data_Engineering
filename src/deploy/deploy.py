# %% [markdown]
## Data Deployment
#   Summary Overview
#   - Parse, Curate, Condition, and Load

# %% [markdown]
## Import Libraries
import os

# %% [markdown]
# - Change Directory to top level folder
top_level_folder = 'twitter_app'
if(os.getcwd().split(os.sep)[-1] != top_level_folder):
    try:
        os.chdir('../..')
        print(f"cwd: {os.getcwd()}", sep = '\n')
    except Exception as e:
        print(f"{e}\n:start current working directory from {top_level_folder}")

from src.parse import stage1
from src.curate import stage2
from src.condition import stage3
from src.load import stage4

# Exporting to executable from Anaconda Prompt with 'pyinstaller -c -F deploy.py'