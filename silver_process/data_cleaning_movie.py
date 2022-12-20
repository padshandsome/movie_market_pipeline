import mysql.connector
import pandas as pd
from tqdm import tqdm

user_name = "root"
password = "*"

db_connection = mysql.connector.connect(user=user_name, password=password)
db_cursor = db_connection.cursor()

# read data from bronze database
query = """SELECT * FROM final_project_db_bronze.movie_bronze"""
movie_dirty_df = pd.read_sql(query,db_connection)

# clean out the quotation marks and replace nan with None
movie_cleaned = pd.DataFrame()
for (col_name, col) in movie_dirty_df.iteritems():
    col_cleaned = col.apply(lambda x:x.replace("'","")).apply(lambda x:x.replace('"','')).apply(lambda x: None if x == 'nan' else x)
    movie_cleaned[col_name] = col_cleaned

# convert duration into numerical values
import re
movie_cleaned['duration'] = movie_cleaned['duration'].apply(lambda x: re.findall("[0-9]*", x)[0] if x and 'min' in x else None)

# generate new index for movie 
for i, row in enumerate(movie_cleaned['show_id']):
    movie_cleaned['show_id'][i] = "Movie"+str(i)

# split multiple country values into different rows
newly_added = pd.DataFrame()
drop_index = []
for i, row in movie_cleaned.iterrows():
    if row['country'] is not None and "," in row['country']:
        country_list = row['country'].split(",")
        movie_cleaned['country'][i] = 'delete'
        
        for j,country in enumerate(country_list):
            new_row = {'show_id':row['show_id']+"_"+str(j), 'type':row['type'], \
                'title':row['title'], 'director':row['director'], \
                    'cast':row['cast'], 'country':country.strip(),'date_added':row['date_added'], \
                        'release_year': row['release_year'], 'rating': row['rating'], 'duration': row['duration'],\
                            'listed_in': row['listed_in'], 'description':row['description'], 'platform':row['platform']}
            newly_added = pd.concat([newly_added, pd.DataFrame([new_row])])
        

movie_cleaned.drop(movie_cleaned[movie_cleaned['country'] == 'delete'].index,inplace = True)

movie_cleaned = pd.concat([newly_added, movie_cleaned])

print("Sucessfully clean movie country")

# create silver database
db_cursor.execute("CREATE DATABASE IF NOT EXISTS final_project_db_silver;")
db_cursor.execute("USE final_project_db_silver;")

# create silver table
db_cursor.execute("""DROP TABLE IF EXISTS movie_silver;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS movie_silver
                        (show_id varchar(255),
                        type varchar(255),
                        title varchar(255),
                        director text,
                        cast text,
                        country varchar(255),
                        date_added varchar(255),
                        release_year varchar(255),
                        rating varchar(255),
                        duration int,
                        listed_in varchar(255),
                        description text,
                        platform varchar(255)
                        );""")

for i,row in tqdm(movie_cleaned.iterrows()):
    insert_sql = """INSERT INTO movie_silver
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s);"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()