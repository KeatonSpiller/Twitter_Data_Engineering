import os, re, pandas as pd, numpy as np, string, tweepy
np.random.seed(0)
  
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