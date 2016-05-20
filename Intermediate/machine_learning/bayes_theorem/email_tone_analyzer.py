import io
import os
import json
from sqlite3 import connect
from pandas import DataFrame
from watson_developer_cloud import ToneAnalyzerV3Beta
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from pymongo import MongoClient

MONGO_HTTP_HOST = 'localhost'
MONGO_CLIENT_PORT = 27017
MONGO_DATABASE = 'email'
MONGO_COLLECTION_NAME = 'tone'

SPAM_DATASET_PATH = './emails/spam'
HAM_DATASET_PATH = './emails/ham'

EMAIL_DATASET_FILE = './database.sqlite'

WATSON_USERNAME = '<watson_username>'
WATSON_PASSWORD = '<watson_password>'
WATSON_VERSION = '2016-02-11'

#Initialize mongoDB
db = MongoClient(MONGO_HTTP_HOST, MONGO_CLIENT_PORT)[MONGO_DATABASE]
#Initialize Watson tone analizer
tone_analyzer = ToneAnalyzerV3Beta( username=WATSON_USERNAME, password= WATSON_PASSWORD, version= WATSON_VERSION)
# Create a SQL connection to our SQLite database
sql_connection = connect(EMAIL_DATASET_FILE)


#### CREATING A SIMPLE SPAM/HAM EMAIL FILTER (Using naive bayes) ####
def read_files(path):
    for root, dirnames, filenames in os.walk(path):
        for filename in filenames:
            path = os.path.join(root, filename)
            
            inBody = False
            lines = []
            f = io.open(path, 'r', encoding='latin1')
            for line in f:
                if inBody:
                    lines.append(line)
                elif line == '\n':
                    inBody = True
            f.close()

            message = '\n'.join(lines)
            yield path, message


def data_frame_from_directory(path, classification):
    rows = []
    index = []
    for filename, message in read_files(path):
        rows.append({'message': message, 'class': classification})
        index.append(filename)
    
    return DataFrame(rows, index=index)

data = DataFrame({'message': [], 'class': []})
data = data.append(data_frame_from_directory(SPAM_DATASET_PATH, 'spam'))
data = data.append(data_frame_from_directory(SPAM_DATASET_PATH, 'ham'))

counts = CountVectorizer().fit_transform(data['message'].values)
targets = data['class'].values

classifier = MultinomialNB()
classifier.fit(counts, targets)
#### END OF CREATING A SIMPLE SPAM/HAM EMAIL FILTER (Using naive bayes) ####


#### FILTERING THE EMAIL ####
def get_email_body():
    for id, message in sql_connection.cursor().execute('SELECT id, ExtractedBodyText FROM Emails;'):
        yield id, message.encode('utf-8')

def filter_messages():
    counter = 0
    for id, message in get_email_body():
        filtered = []
        #Filter the content of the email
        for line in message.split('\n'):
            if  ('PM' not in line) \
              & ('AM' not in line) \
              & ('UNCLASSIFIED' not in line) \
              & (not line.startswith( 'U.S.')) \
              & (not line.startswith( 'Sent')) \
              & (not line.startswith( 'To')) \
              & (not line.startswith( 'on:')) \
              & (not line.startswith( 'B1')) \
              & (not line.startswith( 'B5')) \
              & (not line.startswith( 'B6')) \
              & (not line.startswith( 'Case No.')) \
              & (not line.startswith( 'Doc No.')) \
              & (not line.startswith( 'Date')) \
              & (not line.startswith( 'STATE DE')) \
              & (not line.startswith( 'SUBJECT TO')) \
              & (not line.startswith( 'From')) \
              & (not line.startswith( '-')) \
              & ('@' not in line) \
              & (not line.startswith( 'RELEASE')) \
              #Remove some unimportant message like Fyi, Pls Print, etc.
              & (len(line)>10):
                filtered.append(line)
        message = '\n'.join(filtered)
        #Classify the email (spam or ham) using naiveBayes
        predictions = classifier.predict(vectorizer.transform([message]))
        if (len(m) > 15) & ('ham' in str(predictions[0])):
            yield id, message

#Use watson to analyze the emails
def analyze_emails():
    for id, message in filter_messages():
        try:
            analyze_result = json.loads(json.dumps(tone_analyzer.tone(text=message), indent=2))
            analyze_result['id'] = id
            analyze_result['filteredText'] = message
            #Insert the JSON result from watson to MONGODB 
            db[MONGO_COLLECTION_NAME].insert_one(analyze_result).inserted_id
        except Exception:
            pass

analyze_emails();
#Be sure to close the connection.
sql_connection.close()