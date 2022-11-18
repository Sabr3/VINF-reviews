import os
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer

nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

FILENAME = 'sample.json'
MONTH_DICT = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
              7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}
# Get the current working directory
DIRECTORY = os.getcwd()

STEMMER = PorterStemmer()
LEMMATIZER = WordNetLemmatizer()
stopwords_set = set(stopwords.words('english'))  # Added movie because I guess it's a stopword in my context
stopwords_set.add('movie')
STOPWORDS = stopwords_set

TEST_FILE = 'tf_test.dat'
INDEX_FILE = 'tf_idf_index.dat'
TEST_JSON = 'sample-short.json'
JSON_LINES_FILE = 'final_data.jl'
