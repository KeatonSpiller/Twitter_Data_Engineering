# %% [markdown]
# # Import Libraries

import os,tweepy,re,glob, pandas as pd,numpy as np, yfinance as yf, string, datetime
np.random.seed(0)

def dataframe_astypes():
    """_summary_
    
    cleanly access dataframe conversions
    
    Returns:
        dictionary: column names and pandas dataframe conversions
        
        { 'id': 'int64',
        'created_at': 'datetime64[ns, UTC]',
        'favorite_count': 'int64',
        'retweet_count': 'int64',
        'url': 'object',
        'text': 'object'}
    """
    return { 'id': 'int64',
            'created_at': 'datetime64[ns, UTC]',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'url': 'object',
            'text': 'object'}
    
def user_download_helper(api, userID, group):
    """_summary_
    
    Tweepy API download limit of 200 per chunk, Twitter API limit of 3200
    Looping over individual user using usertimeline to append maximum tweets
    limits on time scraping data, will occasionally wait ~ 15 then continue
    removed usernames, punctuation, website links and special charactersfrom text
    converted time zone from UTC to EST
    _why_
    
    Strip particular user data from Twitter
    Args input:
        api (tweepy class): Handles parsing Twitter API
        userID (list of strings): twitter usernames
        group (string): label of users in list
    Args output:
        csv file in data/{group}/{username}.csv
        {'id': 'int64',
        'created_at': 'datetime64[ns, 'US/Eastern']', # converted to match yahoo finance EST
        'favorite_count': 'int64',
        'retweet_count': 'int64',
        'url': 'object',
        'text': 'object'}
    """
    # Check if there exists previous history of user and group
    
    path = f'./data/{group}'
    if os.path.exists(path):
        try:
            df_history = pd.read_csv(path +'/'+ userID +'_twitter.csv', parse_dates=['created_at']).reset_index(drop=True)
            oldest_id = df_history.id.min()
            since_id = df_history.id.max()
        except Exception:
            print("folder created without previous csv file")
    else:
        oldest_id = None
    all_tweets = []
    
    # If first time running
    #################################################
    if(oldest_id == None):
        tweets = api.user_timeline(screen_name=userID, 
                                count=200,
                                include_rts = False,
                                trim_user = False,
                                tweet_mode = 'extended'
                                )
        all_tweets.extend(tweets)
        
        oldest_id = tweets[-1].id
        while True :
            tweets = api.user_timeline(screen_name=userID,
                                # 200 is the maximum allowed count
                                count=200, 
                                include_rts = False,
                                max_id = oldest_id - 1,
                                trim_user = False,
                                tweet_mode = 'extended'
                                )
            if len(tweets) == 0:
                break
            oldest_id = tweets[-1].id
            all_tweets.extend(tweets)
    #################################################
    else:
        # Start where we ended
        tweets = api.user_timeline(screen_name=userID, 
                                count=200,
                                include_rts = False,
                                since_id = since_id,
                                trim_user = False,
                                tweet_mode = 'extended'
                                )
        # if we haven't downloaded today
        if( len(tweets) != 0 ):
            all_tweets.extend(tweets)
            oldest_id = tweets[-1].id
            since_id = tweets[0].id
             
            while True :
                tweets = api.user_timeline(screen_name=userID,
                                        # 200 is the maximum allowed count
                                        count=200, 
                                        include_rts = False,
                                        since_id = since_id,
                                        max_id = oldest_id - 1,
                                        trim_user = False,
                                        tweet_mode = 'extended'
                                        )
                if len(tweets) == 0:
                    break
                oldest_id = tweets[-1].id
                all_tweets.extend(tweets)
    #################################################
    if(len(all_tweets) != 0):
        regex = "(@[A-Za-z0-9]+)|(\w+:\/\/\S+)"
        outtweets = []
        for tweet in all_tweets:
            # encode decode
            txt = tweet.full_text
            txt = txt.encode("utf-8").decode("utf-8")
            # remove @ and website links
            txt = ' '.join(re.sub(regex, " ", txt).split())
            # remove punctuation
            txt = re.sub(f"[{re.escape(string.punctuation)}]", "", txt)
            # remove non characters
            txt = re.sub(f"([^A-Za-z0-9\s]+)", "", txt)
            # store as a string
            txt = " ".join(txt.split())
            # Pull data from the tweet
            tweet_list = [
                tweet.id_str,
                tweet.created_at,
                tweet.favorite_count, 
                tweet.retweet_count,
                'https://twitter.com/i/web/status/' + tweet.id_str,
                txt 
            ]
            outtweets.append(tweet_list)
        df_temp = pd.DataFrame(outtweets, columns=['id','created_at','favorite_count',\
                                                    'retweet_count','url','text'])
        
        # using dictionary to convert specific columns
        df_temp = df_temp.astype(dataframe_astypes())
        # Convert UTC to US/Eastern
        df_temp.created_at = df_temp.created_at.dt.tz_convert('US/Eastern')
        
        # If no previous history then create file else
        if not os.path.exists(path):
            os.makedirs(path)
            df_temp.to_csv(path +'/'+ userID +'_twitter.csv',index=False)
        else:
            df_merge = pd.concat([df_temp, df_history],
                                axis = 0,
                                join = 'outer',
                                names=['id',
                                        'created_at',
                                        'user',
                                        'favorite_count',
                                        'retweet_count',
                                        'url',
                                        'text'],
                                ignore_index=True)
            print(len(df_merge.id))
            df_merge.to_csv(path +'/'+ userID +'_twitter.csv',index=False)
    #################################################
    else:  
        print(f"no recent tweets")  
             
    
def user_download(api, user_list, group):
    """_summary_
    
    Download users within Excel list of usernames and save in a csv under data
    _why_
    
    runs user_download_helper for each user to download tweets 
    Args:
        Args input:
        api (tweepy class): Handles parsing Twitter API
        userID (list): twitter usernames
        group (string): label of users in list
    Args output:
        csv file in data/{group}/{username}.csv
        {'id': 'int64',
        'created_at': 'datetime64[ns, 'US/Eastern']', # converted to match yahoo finance EST
        'favorite_count': 'int64',
        'retweet_count': 'int64',
        'url': 'object',
        'text': 'object'}
    """
    
    for userID in user_list:
        try:
            print(userID, end=' ')
            user_download_helper(api, userID, group)
        except Exception as e:
            print (str(e))

def merge_files(group, display):
    """_summary_
    Merge Individual groups of Twitter user's and save merge files as csv
    _why_
    Args:
        group (_type_): _description_
        display (_type_): _description_
    """
    csv_files = glob.glob(os.path.join('./data'+"/"+group, "*.csv"))
    df = pd.DataFrame()
    for f in csv_files:
        # read the csv file
        df_temp = pd.read_csv(f)
        user_row = f.split("\\")[-1].split(".")[0]
        df_temp.insert(2, 'user', user_row)
        df_temp = df_temp.astype(dataframe_astypes())
        if( display > 0):
            display(df_temp.iloc[0:display])
            print(df_temp.shape)
        # Merging columns of groups
        df = pd.concat([df_temp,df], axis = 0, join = 'outer', names=['id','created_at','user','favorite_count',\
                                                                      'retweet_count','url','text']).astype(dataframe_astypes())
        
    print(f"size of merged data sets of {group}: {df.shape}")
    
    # Creating path and saving to csv
    path_group_merge = f'./data/{group}/merge/'
    path_merge = f'./data/merge/merged_twitter_users/'
    if not os.path.exists(path_group_merge):
        os.makedirs(path_group_merge)
    if not os.path.exists(path_merge):
        os.makedirs(path_merge)
    df.to_csv(path_group_merge +'/merged_'+ group +'.csv',index=False)
    df.to_csv(path_merge +'/merged_'+ group +'.csv',index=False)

    return df 

def merge_all(group, display):
    """_summary_
    Merge all groups of Twitter user's and save merge files as csv
    _why_
    Args:
        api (_type_): _description_
        userID (_type_): _description_
        group (_type_): _description_
    """
    csv_files = glob.glob(os.path.join('./data'+"/"+group, "*.csv"))
    df = pd.DataFrame()
    for f in csv_files:
        # read the csv file
        df_temp = pd.read_csv(f).astype(dataframe_astypes())
        # using dictionary to convert specific columns
        if( display > 0):
            display(df_temp.iloc[0:display])
            print(df_temp.shape)
        # Merging columns of everything
        df = pd.concat([df_temp,df], axis = 0, join = 'outer',names=['id','created_at','user','favorite_count',\
                                                                     'retweet_count','url','text']).astype(dataframe_astypes())
         
    print(f"size of merged data sets of {group.split('/')[1]}: {df.shape}")
    
    # Creating path and saving to csv
    path_merge = f'./data/merge/all_merged_twitter_users'
    if not os.path.exists(path_merge):
        os.makedirs(path_merge)
    df.to_csv(path_merge +'/all_merged_twitter_users.csv',index=False)
    
    return df

def strip_all_words(df, stop):
    """_summary_
    grab all words from every text file, removing spaces and non nessesary words from stop list
    _why_
    Args:
        df (_type_): _description_
        stop (_type_): _description_
    """
    s = df.text
    # lowercase
    s = s.str.lower()
    # drop digit
    s = s.replace('[\d]+', '',regex=True)
    # remove stop words
    for i in stop :
        s = s.replace(r'\b%s\b'%i, '',regex=True)
    # remove multiple spaces
    s = s.replace('[\s]{2,}', ' ', regex=True)
    s = s.str.split(' ')
    return s

# navigating the all merged text each twitter message for each word and comparing to frequency of word used
def sentence_word_probability(all_word_count, series_text):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        all_word_count (_type_): _description_
        series_text (_type_): _description_
    """
    d = all_word_count.to_dict()
    keys, values = d.keys(), d.values()
    sentence_list, total_probability, individual_probability = [], [], []
    N = float(len(keys)) # N is the length of every word in the dictionary of all words used
    
    for i, sentence in enumerate(series_text):
        word_freq, freq_dict, prob_dict, probability_value = {}, [], {}, 0.0
        if( type(sentence) == list ):
            for word in sentence:
                if( sentence != ''):
                    if word in keys:
                        total_words = d[word]
                        v = 1/total_words * 100
                        if(word in word_freq):
                            word_freq[word] = word_freq[word] + v
                        else:
                            word_freq[word] = v
                            
                        freq_dict.append(word_freq)
                        
                        if word in prob_dict:
                            prob_dict[word] = prob_dict[word] + (v/N)
                        else:
                            prob_dict[word] = v/N
                        probability_value += v
                else:
                    print(word)
        # p = word / count(individual word) * 100 / len(# of all words)
        sentence_list.append(freq_dict)
        individual_probability.append(prob_dict)
        total_probability.append(probability_value / N)
        
    return sentence_list, total_probability, individual_probability

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

def twitter_authentication(autentication_path):
    """_summary_
    Read in twitter api credentials stored on csv file under user_input
    _why_
    Args:
        autentication_path (_type_): _description_
    """
    
    readin_authentication = pd.read_csv(autentication_path, header=0, sep=',')
    consumer_key = readin_authentication['consumer_key'][0]
    consumer_secret = readin_authentication['consumer_secret'][0]
    access_token = readin_authentication['access_token'][0]
    access_token_secret = readin_authentication['access_token_secret'][0]
    bearer_token = readin_authentication['beaker_token'][0]

    # connect to twitter application 
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit = True)
    return api