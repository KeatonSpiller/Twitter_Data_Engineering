# %% [markdown]
## Data Load
#       Summary Overview
#   - load data into an amazon database

# %%
%env WR_DATABASE=default
%env WR_CTAS_APPROACH=False
%env WR_MAX_CACHE_SECONDS=900
%env WR_MAX_CACHE_QUERY_INSPECTIONS=500
%env WR_MAX_REMOTE_CACHE_ENTRIES=50
%env WR_MAX_LOCAL_CACHE_ENTRIES=100

# %% [markdown]
## Import Libraries
import os,sys,pandas as pd,numpy as np, findspark, string, random, json, boto3, sqlite3, glob
import awswrangler as wr
from decimal import Decimal
from pyspark.sql import SparkSession

# %%
wr.config

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
## Load Custom functions
from src.load.load_tools import *

# %%
# Create a passcode for DynamoDB Table
if not os.path.exists('.\src\load\key.json'):
    DynamoDB_allowed = string.ascii_letters + string.digits + '-._'
    complex_password = randomize_key(randomize_from = -10, randomize_to = 10, size = 255, specified = DynamoDB_allowed)
    with open(".\src\load\key.json", "w") as fp:
        json.dump(complex_password, fp)
        fp.close()       
with open(".\src\load\key.json", "r") as fp:
     b = json.load(fp)
    #  print(len(b))
     fp.close()

# %%
path_todays_test = f'./data/merge/combined'
df_merge = pd.read_csv(path_todays_test +'/tickers_and_twitter_users.csv')

# %%
# Load AWS access codes
aws_access_df = pd.read_csv('./user_input/aws_access.csv', dtype=str, header=0)

# %%
def boto3_df_schema(df):
    # float, int, bool, datetime64[ns], timedelta[ns], and object
    # currently not converting for datetime or timedelta
    number_attributes = list(df.columns[df.dtypes == 'float64']) + list(df_merge.columns[df_merge.dtypes == 'int64'])
    string_attributes = list(df.columns[df.dtypes == 'object'])
    bool_attributes = list(df.columns[df.dtypes == 'bool'])
    keyschema,attributedefinitions = [],[]
    for key in string_attributes:
        keyschema.append({'AttributeName':str(key), 'KeyType': "HASH"})
        attributedefinitions.append({'AttributeName':str(key), 'AttributeType': "S"})
    for key in number_attributes:
        keyschema.append({'AttributeName':str(key), 'KeyType': "RANGE"})
        attributedefinitions.append({'AttributeName':str(key), 'AttributeType': "N"})
    for key in bool_attributes:
        keyschema.append({'AttributeName':str(key), 'KeyType': "RANGE"})
        attributedefinitions.append({'AttributeName':str(key), 'AttributeType': "B"})
    return keyschema, attributedefinitions

# %%
dynamodb = boto3.client('dynamodb')
keyschema, attributedefinitions = boto3_df_schema(df_merge)
print(list(keyschema))
# [B, N, S] data types
dynamodb.create_table( 
    TableName = "twitter_ticker_merge",
    KeySchema = keyschema,
    AttributeDefinitions = attributedefinitions
)
# wr.dynamodb.put_df(df = pd.DataFrame({'sandwich':1},index=[0]),
#                    table_name='test2')

# %%   
# def df_to_Sqlite3(tablename, df, db_file):
#     """ create a database connection to a database that resides
#         in the memory
#     """
#     try:
#         database_folder = f'./data/database' # Create Folders
#         if not os.path.exists(database_folder):
#             os.makedirs(database_folder)
            
#         conn = sqlite3.connect(database_folder + db_file)
#         cur = conn.cursor()
#         df.to_sql(tablename, conn, if_exists='replace', index=False) # - writes the pd.df to SQLIte DB    
#         conn.commit()
#     except sqlite3.Error as e:
#         print(e)
#     finally:
#         if conn:
#             conn.close()

# %%

with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)

# for group in groups:
#     csv_files = glob.glob(os.path.join('./data/'+group, "*.csv"))
#     df = pd.DataFrame()
#     for user in csv_files:
#         # read the csv file
#         df_temp = pd.read_csv(user)
#         df_to_Sqlite3(user+'_twitter', df_temp, '/twitterdb.db')
        
# test    
# conn = sqlite3.connect('./data/database/twitterdb.db')
# test = pd.read_sql('select * from biancoresearch_twitter', conn)
# print(test) 

# %%   
# Boto3 load into dataframe
# print(pd.io.json.build_table_schema(df_merge)) 
# dynamodb = boto3.resource('dynamodb')

# table = dynamodb.create_table(
#     TableName = 'twitterticker',
#     pd.io.json.build_table_schema(df_merge)
# )

# table.meta.client.get_waiter('table_exists').wait(TableName='twitterticker')
     
# print(table.item_count)
     
# %% [markdown]
## to Spark Database
# %%
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-19"
os.environ["SPARK_HOME"] = "C:\spark"

# %%
findspark.init()
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark

#%%
twitterdf = spark.createDataFrame(df_merge)
# print(df_merge.columns)

# # Create an in-memory DataFrame to query
# twitterdf.createOrReplaceTempView("twitter_ticker")

# # Query
# top_twitter = spark.sql(
# """SELECT count(*)
#     FROM twitter_ticker 
#     WHERE violation_type = 'RED' 
#     GROUP BY name 
#     ORDER BY total_red_violations DESC LIMIT 10""")

# #         # Write the results to the specified output URI
# #         top_red_violation_restaurants.write.option("header", "true").mode("overwrite").csv(output_uri)



# %%
