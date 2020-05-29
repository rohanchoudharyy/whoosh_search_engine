import pandas as pd
import math
import timeit
import mysql.connector
import pandas as pd
from nltk.corpus import stopwords
import string
import re
from whoosh import qparser
from whoosh.lang.porter import stem
from whoosh.lang.morph_en import variations
from whoosh.index import create_in
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import *
from whoosh import index
import os, os.path
from whoosh.qparser import MultifieldParser, OrGroup
from bs4 import BeautifulSoup

# Database connectivity :
db_connection = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="olcademy"
)
my_database = db_connection.cursor()
sql_statement = "SELECT * FROM `courses`"
df = pd.read_sql(sql=sql_statement, con=db_connection)

# Removing unneccesary text
df['course_description'].astype(str)
df['trainer_description'].astype(str)
df['clean_course_description'] = df['course_description'].apply(lambda x : BeautifulSoup(str(x), 'lxml').get_text())
df['clean_trainer_description'] = df['trainer_description'].apply(lambda x : BeautifulSoup(str(x), 'lxml').get_text())

# Selecting required columns :
df=df.loc[:,['course_id','trainer_name','course_title','course_subtitle','course_price','clean_course_description',
            'course_language','level_of_course','course_category','clean_trainer_description',]]


#Schema of each document :
schema = Schema(trainer_name=TEXT(analyzer=StemmingAnalyzer(minsize=3), stored=True),
                course_title=TEXT(spelling=True,field_boost=2.0, stored=True),
                clean_course_description=TEXT(analyzer=StemmingAnalyzer(minsize=0),spelling=True, stored=True),
                clean_trainer_description=TEXT(analyzer=StemmingAnalyzer(minsize=3), stored=True),
                course_language=TEXT(analyzer=StemmingAnalyzer(minsize=3), stored=True),
                level_of_course=TEXT(analyzer=StemmingAnalyzer(minsize=3), stored=True),
                course_category=TEXT(analyzer=StemmingAnalyzer(minsize=3), stored=True),
                course_subtitle=TEXT(field_boost=1.0),
                course_id=ID(stored=True))

# Creating index :
if not os.path.exists("indexdir"):
    os.mkdir("indexdir")
ix = index.create_in("indexdir", schema)
#open an existing index object
ix = index.open_dir("indexdir")
#create a writer object to add documents to the index
writer = ix.writer()

#Writing the document locally
for i in range(len(df)):
    x2=df.trainer_name[i]
    x3=df.course_title[i]
    x4=df.clean_course_description[i]
    x7=df.course_subtitle[i]
    x8=df.course_category[i]
    x1=df.course_id[i]
       
    writer.add_document(
                trainer_name=x2,
                course_title=x3,
                clean_course_description=x4,
                course_subtitle=x7,
                course_category=x8,
                course_id=x1
                        )
               
writer.commit()

#Parser to parse the results :
qp = MultifieldParser(["course_title",
                       "trainer_name",
                       "clean_course_description",
                       "course_subtitle"
                      ],                        # all selected fields
                        schema=ix.schema,       # with my schema
                        group=OrGroup)          # OR instead AND


#Function to ask query and provide search results
stop_words_eng = stopwords.words("english")
def ask(user_query):
    #user_query = str(input("Enter your query:"))
    #user_query = "java"
    #print('\n')
    #start = timeit.default_timer()
    user_query = user_query.lower()
    user_query = ' '.join([word for word in user_query.split() if word not in stop_words_eng])
    print("this is your query: " + user_query+'\n\n')

    q = qp.parse(user_query)  

    res_list=[]
    with ix.searcher() as searcher:
        results = searcher.search(q)
        for hit in results:
            res_list.append((hit['course_id']))
        return (res_list)  
          