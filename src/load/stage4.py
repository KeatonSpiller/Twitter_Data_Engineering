# %% [markdown]
## Data Load
#       Summary Overview
#   - load data into Spark big data database

# %% [markdown]
## Import Libraries
import os,sys,pandas as pd,numpy as np, findspark, os
from pyspark.sql import SparkSession
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
df_merge = pd.read_csv(path_todays_test +'/index_funds_and_twitter_analysts.csv', parse_dates=['date']).set_index('date')

# %% [markdown]
## to Spark Database
# %%
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-19"
os.environ["SPARK_HOME"] = "C:\spark"

#%%

findspark.init()
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark

#%%
sparkDF = spark.createDataFrame(df_merge)
sparkDF.printSchema()

sparkDF.show(truncate = True)
# %%
