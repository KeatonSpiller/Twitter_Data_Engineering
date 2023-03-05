# %% [markdown]
## Data Load
#       Summary Overview
#   - load data into an dynamodb

# %%
%env WR_DATABASE=default
%env WR_CTAS_APPROACH=False
%env WR_MAX_CACHE_SECONDS=900
%env WR_MAX_CACHE_QUERY_INSPECTIONS=500
%env WR_MAX_REMOTE_CACHE_ENTRIES=50
%env WR_MAX_LOCAL_CACHE_ENTRIES=100

# %% [markdown]
## Import Libraries
import os,sys,pandas as pd,numpy as np, findspark, string, random, json, boto3, sqlite3, glob, uuid
from decimal import Decimal
from pyspark.sql import SparkSession

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
from src.load.load_tools import randomize_key, df_to_csv

# %%
# Read in twitter User's
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)

# %%
path_todays_test = f'./data/merge/combined'
df_merge = pd.read_csv(path_todays_test +'/tickers_and_twitter_users.csv')

# %%
# Load AWS access codes
aws_access_df = pd.read_csv('./user_input/aws_access.csv', dtype=str, header=0)

# %%
wide_col = list(df_merge.columns[1:])
df_merge.head()

# %%
# dynamodb has a limit of 1024KB per row aggregated
# the wide columns I have the df_merge in are too large
# Therefore melting the wide columns to tall/long
df_tall = pd.melt(df_merge, 
                  id_vars='date', 
                  value_vars=wide_col,
                  var_name = 'twitter_ticker')

# %%
dynamodb = boto3.client('dynamodb')
# [B, N, S] data types
existing_tables = dynamodb.list_tables()['TableNames']
# Create table
try:
    dynamodb.create_table(
        TableName = "twitter_ticker_merge",
        KeySchema =[
                {
                    'AttributeName': 'uuid', # Universally unique identifier
                    'KeyType': 'HASH'
                }
        ],
        AttributeDefinitions = [
                {
                    'AttributeName': 'uuid', # Universally unique identifier
                    'AttributeType': 'S'
                }
        ],
        BillingMode='PROVISIONED',
        ProvisionedThroughput = {
                                    'ReadCapacityUnits': 1,
                                    'WriteCapacityUnits': 1
                                }
    )
except dynamodb.exceptions.ResourceInUseException:
    print(f'Table exists!')
    pass

# %%
# Put table to database
# do we want to keep the previous uuid key's from past dataframes?
# or rewrite with new uuid keys each time
if(os.path.exists(f'./data/merge/combined/tickers_and_twitter_users_tall.csv')):
    df_tall_prev = pd.read_csv(f'./data/merge/combined/tickers_and_twitter_users_tall.csv')
    # If the current df_tall is the same as the previously saved df
    if(len(df_tall_prev) == len(df_tall)):
        df_tall = df_tall_prev
    else:
        # create new uuid for newly added entries
        df_tall['uuid'] = pd.concat([df_tall_prev['uuid'], df_tall[len(df_tall_prev):].index.to_series().map(lambda x: uuid.uuid4())], axis=0)
else:
    df_tall['uuid'] = df_tall.index.to_series().map(lambda x: uuid.uuid4())
    df_to_csv(df=df_tall,
                file=f'tickers_and_twitter_users_tall.csv',
                folder=f'./data/merge/combined/')
for index, row in df_tall.iterrows():
    data = dict(row)
    dynamodb.put_item(  TableName = "twitter_ticker_merge",
                        Item =
                        {
                            "uuid": {'S':str(data['uuid'])},
                            'date': {'S':str(data['date'])},
                            'twitter_ticker':{'S':str(data['twitter_ticker'])},
                            'value': {'N':str(data['value'])}
                        }
                    )

# %%
# sqlite
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
#
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

# %% [markdown]
## Spark
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-19"
os.environ["SPARK_HOME"] = "C:\spark"
findspark.init()
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
twitterdf = spark.createDataFrame(df_merge)

# %%
# Generate encrypted Key
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