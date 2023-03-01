# %% [markdown]
## Data Curation
#   Summary Overview
#   - Merge Parsed Tweets
#   - Cutoff tweets older than 2017
#   - Normalize and clean data
#   - Create dictionaries from words spoken
#   - Create probabilities of words used per user

# %% [markdown]
# - Import Libraries
import os, nltk, pandas as pd, numpy as np, skfda, string, texthero, emoji, math
from gensim.models import Word2Vec
from nltk.stem.snowball import SnowballStemmer
from easynmt import EasyNMT

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
# - Load tools
from src.curate.curate_tools import clean_text, merge_files, merge_all, df_to_csv, n_gram, unigram_probability, bigram_probability

# %% [markdown]
# - Read in groups of twitter user's to merge
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)
user_df
    
# %% [markdown]
# - Merge Tweets
for group in groups:
    merge_files(group, display = 0)
df_all = merge_all(display = 0)

# %% [markdown]
# - top 5 oldest tweets by user
df_all.groupby('user')['created_at'].min().sort_values(ascending= True).head(5)

# %% [markdown]
# - Drop old tweets below threshold
threshold = '2017-01-01'
df_all_upperbound = df_all[df_all.created_at > threshold]

# %% [markdown]
# - non significant words to remove
nonessential_words = ['twitter', 'birds','lists','list', 'source', 'am', 'pm', 'nan'] + list(string.ascii_lowercase) + list(string.ascii_uppercase)
stopwords = nltk.corpus.stopwords.words("english") + nonessential_words
words_to_remove = sorted(list( dict.fromkeys(stopwords) )) # remove duplicates

# %% [markdown]
# - Cleaning up tweet sentences
# - websites(html/www)
# - usernames | hashtags | digits
# - extra spaces | stopwords | emoji
# - punctuation (texthero)
# - translate to english from 186 languages Helsinki-NLP
# - stemming similar words -> 'like' 'liked' 'liking' to stem:'lik'
stem_text, nonstem_text = clean_text(df_all_upperbound.text[0:10000], words_to_remove)

df_to_csv(df = stem_text, 
          folder = f'./data/merge/all_twitter_users/stats', 
          file = f'/cleaned_text_stem.csv')
df_to_csv(df = nonstem_text, 
          folder = f'./data/merge/all_twitter_users/stats', 
          file = f'/cleaned_text_nonstem.csv')

# %% [markdown] 
# - Combine similar words to reduce Probability computation
# - Vectorize with Word2vec | BERT | Tf-Idf
# cluster similar words with word_to_vec and unsupervised clustering
# word_vector = Word2Vec(cleaned_text,size=100, min_count=1)

# %% [markdown]
# - cluster and combine similar word vectors and replace original words
# - kmeans | PCA | fuzzycmeans | tsne
# emb_df = (
#     pd.DataFrame(
#         [word_vector.wv.get_vector(str(n)) for n in word_vector.wv.key_to_index],
#         index = word_vector.wv.key_to_index
#     )
# )
# print(emb_df.shape)
# fuzzy_c = skfda.ml.clustering.FuzzyCMeans(random_state=0)
# fuzzy_c.fit(emb_df)

# %%
# N gram, frequency, and relative frequency bag of words
unigram_sentence, unigram_frequency, unigram_relative_frequency = n_gram(stem_text, 1)
bigram_sentence, bigram_frequency, bigram_relative_frequency = n_gram(stem_text, 2)
trigram_sentence, trigram_frequency, trigram_relative_frequency = n_gram(stem_text, 3)

# %%
# probability matrix
# column = list(unigram_frequency.index)
# prob_matrix = np.diag(unigram_frequency.to_numpy(dtype='int32'))
# Add Binomials to matrix
# print(prob_matrix)
  
# %% [markdown]
# N Gram probability
# $$ P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{k-1}) $$
# $$ P(W_{n}|W_{n-1}) =  \dfrac{C(W_{n-1}W{n})}{C(W{n-1})} $$
unigram_prob = unigram_probability(stem_text, unigram_relative_frequency)
bigram_prob = bigram_probability(stem_text, bigram_sentence, unigram_frequency, bigram_frequency)

# %%
# Adding ngram probability to the dataframe
df_all_prob = df_all_upperbound.reset_index()
df_all_prob['unigram_probability'] = unigram_prob.reset_index(drop=True)
df_all_prob['bigram_probability'] = bigram_prob
# removing empty tweets
df_all_prob = df_all_prob.dropna()
# Converting timestamp (HH:MM:SS) to Year-month-day to combine users on the same day
df_all_prob.insert(loc = 0, column = 'date', value = pd.to_datetime(df_all_prob['created_at']).apply(lambda x: x.strftime('%Y-%m-%d')))
df_all_prob.date = pd.to_datetime(df_all_prob['date'], format='%Y-%m-%d')
df_all_prob = df_all_prob.sort_values(by=['date'], ascending=False).drop(columns=['index'])
df_to_csv(df = df_all_prob, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_ngram.csv')
# %%
df_all_prob.head(2)