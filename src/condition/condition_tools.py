# %% [markdown]
# # Import Libraries

import pandas as pd,numpy as np, yfinance as yf
np.random.seed(0)

def normalize_columns(df, columns):
    """_summary_
    Min Max scaling the numerical data sets
    grab all words from every text file, removing spaces and non nessesary words from stop list
    Args:
        df (_type_): _description_
        columns (_type_): _description_
    _why_
    """
    for c in columns:
        df[c] = (df[c] - df[c].min()) / (df[c].max() - df[c].min())
    return df 

def normalize_columns_target(df, df_original, columns):
    """_summary_
    Min Max Scaling the test dataset based on the original dataset min max values
    grab all words from every text file, removing spaces and non nessesary words from stop list
    _why_
    Args:
        df (_type_): _description_
        df_original (_type_): _description_
        columns (_type_): _description_
    """
    for c in columns:
        df[c] = (df[c] - df_original[c].min()) / (df_original[c].max() - df_original[c].min())
    return df 

def download_todays_test(ticker_df, df_normalized, df_original):
    """_summary_
    Merge the newest matching day for index fund and twitter tweet
    grab all words from every text file, removing spaces and non nessesary words from stop list
    _why_
    Args:
        ticker_df (_type_): _description_
        df_normalized (_type_): _description_
        df_original (_type_): _description_
    """
    # Download today's Index funds, and twitter probabilities
    
    column_names = dict(zip(ticker_df.ticker_name, ticker_df.ticker_label))
    column_names['Datetime']='date'
    stock_list = list(ticker_df.ticker_name)
    stock_str = ' '.join( stock_list )
    current_price = yf.download(stock_str, period='1d', interval = '1m', progress=False)['Close']
    current_price = current_price.loc[[str(current_price.index.max())]].reset_index('Datetime').rename(columns= column_names)
    
    convert_list = dict(zip(ticker_df.ticker_label, ['float64']*len(ticker_df.ticker_label)))
    current_price = current_price.astype(convert_list)
    current_price.date = current_price.date.dt.date
    current_price = current_price.astype({'date':'datetime64[ns]'}).set_index('date')
    
    todays_data = df_normalized.loc[df_normalized.index == str(current_price.index[0]),:]
    
    todays_test = pd.merge(current_price, todays_data, how='inner', on='date')
    columns = list(ticker_df.ticker_label) + ['favorite_count', 'retweet_count']
    todays_test = normalize_columns_target(todays_test.copy(), df_original.copy(), columns)
    
    return todays_test

def create_target(df, day = 5, ticker= "SandP_500"):
    """_summary_
    Create A Target for prediction
    _why_
    Args:
        df (_type_): _description_
        day (_type_): _description_
        ticker (_type_): _description_
    ex day = 5 
    sum(5 days) / 5 compare to each date if current day > last 5 days 
    If True Then 1 Else 0
    """
    day_avg = df[ticker].shift(periods=-1).rolling(day).sum().div(day) # period = -1 to start from the oldest
    conditional = df[ticker] > day_avg

    try:
        df.insert(loc = 0,
                        column = 'y',
                        value = np.where(conditional, 1, 0) )
    except ValueError:
        print(ticker)
        pass
    return df 