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

from src.parse import stage1
from src.curate import stage2
from src.condition import stage3
from src.load import stage4

# Exporting to executable from Anaconda Prompt with 'pyinstaller -c -F deploy.py'