import os, pandas as pd, numpy as np, tweepy, glob, re, advertools
np.random.seed(0)

def user_download_helper(api, userID, group, display='full'):
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
        display (string): 'full' -> prints user and size of tweets added or [no recent tweets]
                          'minimal' -> prints user
                           [any other keystroke] -> prints only the group downloading
                           Default: 'full'
    Args output:
        csv file in data/users/{group}/{username}.csv
        { 'id': 'int64',
            'created_at': 'datetime64[ns, US/Eastern]',
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}
    """
    file = f'./data/users/{group}/{userID}.csv'
    folder = f'./data/users/{group}'
    
    # Check if there exists previous history of user and group
    if os.path.exists(file):
        try:
            df_history = pd.read_csv(file, parse_dates=['created_at']).reset_index(drop=True)
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
    if(len(all_tweets) > 0):
        outtweets = []
        for tweet in all_tweets:
            
            # encode decode
            txt = tweet.full_text
            txt = txt.encode("utf-8").decode("utf-8")
            
            # Pull data from the tweet
            tweet_list = [
                tweet.id_str,
                tweet.created_at,
                'https://twitter.com/i/web/status/' + tweet.id_str,
                tweet.favorite_count, 
                tweet.retweet_count,
                txt 
            ]
            outtweets.append(tweet_list)
        df_temp = pd.DataFrame(outtweets, 
                               columns=['id',
                                        'created_at',
                                        'url',
                                        'favorite_count',
                                        'retweet_count',
                                        'text'])
        
        # Pulling specific txt into their own seperate columns
        s = pd.Series(df_temp.text)
        website = r'http\S+|www\S+'
        username = r'@[\w]+'
        hashtag = r'#[\w]+'
        
        hashtags = s.str.findall(hashtag, flags=re.IGNORECASE).str.join(" ")
        usernames = s.str.findall(username, flags=re.IGNORECASE).str.join(" ")
        links = s.str.findall(website, flags=re.IGNORECASE).str.join(" ")
        
        emoji_extraction = advertools.extract_emoji(s)
        emojis = pd.Series(emoji_extraction['emoji']).str.join(" ")
        emoji_text = pd.Series(emoji_extraction['emoji_text']).str.join(" ")
        
        df_temp.insert(5, 'hashtags', hashtags)
        df_temp.insert(6, "emojis", emojis)
        df_temp.insert(7, "emoji_text", emoji_text)
        df_temp.insert(8, "usernames", usernames)
        df_temp.insert(9, "links", links)
        
        # using dictionary to convert specific columns
        df_temp = df_temp.astype(dataframe_astypes())
        # Convert UTC to US/Eastern
        df_temp.created_at = df_temp.created_at.dt.tz_convert('US/Eastern')
        
        # If no previous folder then create folder
        if not os.path.exists(folder):
            os.makedirs(folder)
        # If no previous history then create file
        if not os.path.exists(file):
            df_temp.to_csv(file,index=False)
        # else merge previous history
        else:
            df_merge = pd.concat([df_temp, df_history],
                                axis = 0,
                                join = 'outer',
                                names=['id','created_at','url','user','favorite_count',
                                        'retweet_count','url','hashtags','emojis','emoji_text',
                                        'usernames','links','text'],
                                ignore_index=True)
            df_merge.to_csv(file,index=False)
            
        # print size of download
        if(display == 'full'):
            # if first time downloading
            if(oldest_id == None):
                print(f'-> {len(df_merge)} tweets downloaded', end='\n')
            # else added tweets are not empty
            else: 
                print(f'-> {len(df_temp)} tweets downloaded, {len(df_merge)} total tweets', end='\n') 
    #################################################
    else:
        # len() of downloaded file was 0
        if(display == 'full'):
            print(f"-> [no recent tweets]",end='\n')       
    
def user_download(api, user_list, group, display='full'):
    """_summary_
    
    Download users within Excel list of usernames and save in a csv under data
    _why_
    
    runs user_download_helper for each user to download tweets
    removes csv files from user_list
    Args:
        Args input:
        api (tweepy class): Handles parsing Twitter API
        userID (list): twitter usernames
        group (string): label of users in list
    Args output:
        csv file in data/{group}/{username}.csv
        { 'id': 'int64',
            'created_at': 'datetime64[ns, UTC]',
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}
    """
    # Download every User from Group of User's
    for userID in user_list:
        try:
            if(display == 'minimal' or display == 'full'):
                print(userID, end=' ')
            user_download_helper(api, userID, group, display)
        except Exception as e:
            if(display != 'minimal' and display != 'full'):
                print(userID, end = ' ')
            print(f"exception: {str(e)}",
                  f"Scenarios could include",
                  f"- Mispelled User ID",
                  f"- Account Removed",
                  f"- Privated account",
                  f"*****", sep='\n', end='\n\n')
    
    # if group folder exists
    if(os.path.exists(os.path.normpath(f'./data/users/{group}'))):
        # remove file if removed from excel spreadsheet
        csv_files = glob.glob(os.path.join(f'./data/users/{group}', "*.csv"))
        if(len(csv_files) > 0):
            for file in csv_files:
                if((str(file.split(os.sep)[-1].split(".")[0])) not in user_list):
                    os.remove(f'{file}')
                    
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
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}
    """
    return { 'id': 'int64',
            'created_at': 'datetime64[ns, UTC]',
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}