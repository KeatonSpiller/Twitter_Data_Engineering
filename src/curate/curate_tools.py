# %% [markdown]
# # Import Libraries

import os, glob, pandas as pd, numpy as np, re, string, timeit, texthero
np.random.seed(0)

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

# navigating the all merged text each twitter message for each word and comparing to frequency of word used
def sentence_word_probability(relative_frequency, cleaned_text):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        relative_frequency (_type_): _description_
        cleaned_text (_type_): _description_
    """
    N = float(len(relative_frequency))
    total_probability = [list(map(relative_frequency.get, l)) for l in cleaned_text]
    total_probability = [sum(i)/N if(len(i) != 0) else 0 for i in total_probability ]
        
    return total_probability / sum(total_probability)

# navigating the all merged text each twitter message for each word and comparing to frequency of word used
def sentence_word_probability_original(all_word_count, series_text):
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