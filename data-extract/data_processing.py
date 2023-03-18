# Import libraries
import pandas as pd
import pymongo
import yaml
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import WordNetLemmatizer
import string
import psycopg2

# Stop words from corpus
corpus_stop = set(stopwords.words('english'))

# Define punctuation
punct = list(string.punctuation)

# Load credentials for postgres and connect to local instance
with open("/Users/samivanecky/git/running-shoe-app/resources/pg_creds.yaml") as f:
    pg_creds = yaml.safe_load(f)

# Connect to postgres
conn = psycopg2.connect(
    dbname=pg_creds['database'], 
    user=pg_creds['user'], 
    host=pg_creds['host'], 
    port=pg_creds['port'])

# Load data from MongoDB
# Import cred from yaml
with open("/Users/samivanecky/git/running-shoe-app/mongo_creds.yaml") as f:
    creds = yaml.safe_load(f)

# Connect to mongo
connection_str = "mongodb+srv://trackrabbit:" + str(creds['pwd']) + "@running-shoes.ncb9yfc.mongodb.net/?retryWrites=true&w=majority"
# Connect to MongoDB
client = pymongo.MongoClient(connection_str)
# Connvect to database
db = client["running-shoes"]

# Connect to collections
rtr_col = db["roadtrailrun"]
guru_col = db["runningshoesguru"]
rr_col = db["runrepeat"]
rw_col = db["runningwarehouse"]

# Load txt file of stop words
with open("/Users/samivanecky/git/running-shoe-app/resources/stop_words_expanded.txt") as f:
    stop_words = f.read()
# Remove new lines & convert to list
stop_words = re.sub('\n', ' ', stop_words).split()

with open("/Users/samivanecky/git/running-shoe-app/resources/remove_words.txt") as f:
    remove_words = f.read()
# Remove new lines & convert to list
remove_words = re.sub('\n', ' ', remove_words).split()

# Extract data from collections into dataframes
rtr_df = pd.DataFrame(list(rtr_col.find()))
guru_df = pd.DataFrame(list(guru_col.find()))
rr_df = pd.DataFrame(list(rr_col.find()))
rw_df = pd.DataFrame(list(rw_col.find()))

# TEST CODE FOR WORKING WITH A SINGLE REVIEW
# Preprocess text to work with

# Function to clean data
def cleanReviewText(df):
    # Convert data to lower case
    df['review_txt_clean'] = df["review_txt"].apply(lambda t: t.lower())

    # Tokenize text
    df['toks'] = df['review_txt_clean'].apply(lambda t: word_tokenize(t))

    # Sentence tokenize
    df['sent_toks'] = df['review_txt_clean'].apply(lambda t: sent_tokenize(t))

    # Remove stop words
    df['toks'] = df['toks'].apply(lambda t: [w for w in t if w not in corpus_stop])

    # Remove "remove" words
    df['toks'] = df['toks'].apply(lambda t: [w for w in t if w not in remove_words])

    # Remove punctuation from toks
    df['toks'] = df['toks'].apply(lambda t: [w for w in t if w not in punct])

    # Create lemmatizer
    wordnet_lemmatizer = WordNetLemmatizer()

    # Lemmatize the words in toks
    df['lemmas'] = df['toks'].apply(lambda t: [wordnet_lemmatizer.lemmatize(w) for w in t])

    return(df)

# Preprocess the review text in the dataframes
rtr_df = cleanReviewText(rtr_df)
guru_df = cleanReviewText(guru_df)
rr_df = cleanReviewText(rr_df)
rw_df = cleanReviewText(rw_df)

# Write all the data frames to postgres
