# %% [markdown]
## Import Libraries
import os,sys

## Change Directory to root
# %%
file = os.getcwd().split(os.sep)
while(file[-1] != 'twitter_app'): # Check the working directory
    os.chdir('..')
    file = os.getcwd().split(os.sep)
    sys.path.append(os.path.abspath(os.getcwd()))

from parse import stage1
from curate import stage2
from condition import stage3
from load import stage4

# Exporting to executable from Anaconda Prompt with 'pyinstaller -c -F deploy.py'