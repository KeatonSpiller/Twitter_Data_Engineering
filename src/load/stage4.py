# %% [markdown]
## Data Load
#       Summary Overview
#   -
#   -
#   -

# %% [markdown]
## Import Libraries
import os,sys,pandas as pd,numpy as np, pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext 
np.random.seed(0)

## Change Directory to root
# %%
file = os.getcwd().split(os.sep)
while(file[-1] != 'twitter_app'): # Check the working directory
    os.chdir('..')
    file = os.getcwd().split(os.sep)
    sys.path.append(os.path.abspath(os.getcwd()))
print(f"root directory: {os.getcwd()}", sep = '\n')

# %%
path_todays_test = f'./data/merge/combined'
df_merge = pd.read_csv(path_todays_test +'/todays_test.csv', parse_dates=['date']).set_index('date')

# %% [markdown]
## to Spark Database
# %%
spark = SparkSession.builder.appName('twitter_data').getOrCreate()
print(spark)
# %%
# os.environ["SPARK_HOME"] = r"C:\Users\Keaton\bigdata\spark"
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[3] pyspark-shell"
# os.environ["JAVA_HOME"] = r"C:\Java\jre1.8.0_311"