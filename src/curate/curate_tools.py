# %% [markdown]
# # Import Libraries

import os, glob, pandas as pd, numpy as np, re, texthero, collections, itertools
from nltk.util import ngrams,everygrams,skipgrams

def dataframe_astypes_curate():
    """_summary_
    
    cleanly access dataframe conversions
    
    Returns:
        dictionary: column names and pandas dataframe conversions
        
        { 'id': 'int64',
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
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}
    
def df_to_csv(df, folder, file):
    """_summary_
        Save Dataframe as a CSV in a particular folder with specified file name
    Args:
        df (pandas): any pandas dataframe
        folder (string): folder location from source
        file (string): file to name CSV file
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
    df.to_csv(folder+file, index=False)
    return

def merge_files(group, display):
    """_summary_
    Merge Individual groups of Twitter user's and save merge files as csv
    _why_
    Args:
        group (_type_): _description_
        display (_type_): _description_
    """
    csv_files = glob.glob(os.path.join(f'./data/users/{group}', "*.csv"))
    df = pd.DataFrame()
    for f in csv_files:
        # read the csv file
        df_temp = pd.read_csv(f)
        user_row = f.split("\\")[-1].split(".")[0]
        df_temp.insert(2, 'user', user_row)
        df_temp = df_temp.astype(dataframe_astypes_curate())
        if( display > 0):
            display(df_temp.iloc[0:display])
            print(df_temp.shape)
        # Merging columns of groups
        df = pd.concat([df_temp,df], 
                        axis = 0, 
                        join = 'outer', 
                        names=['id','created_at','url','user','favorite_count',
                               'retweet_count','url','hashtags','emojis','emoji_text',
                               'usernames','links','text']).astype(dataframe_astypes_curate())
    if(len(df) > 0): 
    # Always prints the complete merged size
        print(f"{len(df)} {group} tweets")
        # Creating folder and saving to csv
        df_to_csv(df = df, 
                folder = f'./data/merge/twitter_groups', 
                file = '/merged_'+ group +'.csv')

    return df 

def merge_all(display):
    """_summary_
    Merge all groups of Twitter user's and save merge files as csv
    _why_
    Args:
        api (_type_): _description_
        userID (_type_): _description_
        group (_type_): _description_
    """
    csv_files = glob.glob(os.path.join('./data/merge/twitter_groups', "*.csv"))
    df = pd.DataFrame()
    for f in csv_files:
        # read the csv file
        df_temp = pd.read_csv(f).astype(dataframe_astypes_curate())
        # using dictionary to convert specific columns
        if( display > 0):
            display(df_temp.iloc[0:display])
            print(df_temp.shape)
            
        # Merging columns of everything
        df = pd.concat([df_temp,df], 
                    axis = 0, 
                    join = 'outer',
                    names=['id','created_at','url','user','favorite_count',
                            'retweet_count','url','hashtags','emojis','emoji_text',
                            'usernames','links','text'])
         
    print(f"= {len(df)} total tweets")
    if(len(df) > 0):
        # Creating folder and saving to csv
        df_to_csv(df = df, 
                folder = f'./data/merge/all_twitter_users', 
                file = '/all_twitter_users.csv')
    return df

def clean_text(s, words_to_remove):
    """_summary_
    grab all words from every text file, removing spaces and non nessesary words from stop list
    _why_
    Args:
        s (Pandas Series): Series of strings to clean
        words_to_remove (list): list of words to remove
    """
    
    # normalize to lowercase
    s = s.str.lower()
    
    # remove website(html/www) username hashtag decimal extra spaces
    regex = r'http\S+|www\S+|@[\w]+|#[\w]+|[\d]+|[\s]{2,}'
    s = s.str.replace(regex, "", regex=True)
    
    # remove stop words
    s = s.str.replace(r'(?<!\w)(?:' +'|'.join(words_to_remove) + r')(?!\w)', "", regex=True)
    
    # remove punctuation and library touch up
    s = texthero.clean(s)
    
    # touch up remaining non characters and str split to remove leading/trailing spaces
    s = s.str.replace(r'[^\w\s]+', "", regex=True).str.split()
    
    return s

def relative_probability(relative_frequency, cleaned_text):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        relative_frequency (Series): _description_
        cleaned_text (Series): _description_
        
    example
    cleaned_text =  [cat dog cat]            (length of tweet words) = 3
                    [shark cat]              (length of tweet words) = 2
                    [dog lamb]               (length of tweet words) = 2
                    
    relative_frequency = cat   : 3 / 7 = ~.43
                         shark : 1 / 7 = ~.14
                         dog   : 2 / 7 = ~.29
                         lamb  : 1 / 7 = ~.14
                                       = 1
    tweet_frequency =   [3/7 2/7 3/7]
                        [1/7 3/7]
                        [2/7 1/7]
                        
    tweet_probability = [3/7 + 2/7 + 3/7] / 3 (length of tweet words)
                        [1/7 + 3/7] / 2       (length of tweet words)
                        [2/7 + 1/7] / 2       (length of tweet words)
                        
    sum(tweet_probability) =  ~.38
                              ~.28
                              ~.21
                            = ~.88
    tweet_probability / sum(tweet_probability) = ~.38 / ~.88
                                                 ~.28 / ~.88
                                                 ~.21 / ~.88
                                                 
                                               = ~.43
                                                 ~.32
                                                 ~.24
                                               = 1     
    """
    
    tweet_frequency = [list(map(relative_frequency.get, tweet)) for tweet in cleaned_text]
    tweet_probability = [sum(tweet)/len(tweet) if(len(tweet) > 0) else 0.0 for tweet in tweet_frequency ]

    return tweet_probability / sum(tweet_probability)

def n_gram(cleaned_text, n):
    """_summary_

    Args:
        cleaned_text (Pandas Series): _description_
        n (integar): number of grams wanted
    Returns:
        grams, frequency and relative frequency Pandas Series
        ouputs csv files to to stats folder 
    """
    grams = pd.Series(cleaned_text.apply(lambda tweet: list(ngrams(tweet, n))))
    frequency = pd.Series(collections.Counter(list(itertools.chain.from_iterable(grams))))
    relative_frequency = frequency / len(frequency)
    
    # book keeping output
    df_to_csv(df = grams, 
            folder = f'./data/merge/all_twitter_users/stats', 
            file = f'/{n}_grams.csv')
    df_to_csv(df = frequency.reset_index(), 
            folder = f'./data/merge/all_twitter_users/stats', 
            file = f'/{n}_gram_frequency.csv')
    df_to_csv(df = relative_frequency.reset_index(), 
            folder = f'./data/merge/all_twitter_users/stats', 
            file = f'/{n}_gram_relative_frequency.csv')
    
    return grams, frequency, relative_frequency

# Probabilities of N gram twitter words spoken compared to other tweets
def bigram_probability(bigram_sentence, unigram_frequency, bigram_frequency):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        relative_frequency (Series): _description_
        cleaned_text (Series): _description_
        
    P(students are from vallore)
    Bigram = P(are|students)*P(from|are)*P(vallore|from)
    P(are|students) = count(students|are)/count(students)
    """
    # start = timeit.default_timer()
    # stop = timeit.default_timer()
    # print('Time: ', stop - start)  
     
    total_probability = [(np.prod([bigram_frequency[tup]/unigram_frequency[tup[0]] for tup in sentence])) for sentence in bigram_sentence]
    
    # total_probability = cleaned_text.apply(lambda tweet: np.prod(list(map(relative_frequency.get, tweet))))
    return total_probability

# Probabilities of N gram twitter words spoken compared to other tweets
def unigram_probability(cleaned_text, unigram_relative_frequency):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        relative_frequency (Series): _description_
        cleaned_text (Series): _description_
        
    P(students are from vallore)
    Bigram = P(are|students)*P(from|are)*P(vallore|from)
    P(are|students) = count(students|are)/count(students)
    """
    total_probability = cleaned_text.apply(lambda tweet: np.prod(list(map(unigram_relative_frequency.get, tweet))))
   
    return total_probability