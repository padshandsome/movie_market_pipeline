import mysql.connector
import pandas as pd
from tqdm import tqdm

user_name = "root"
password = "*"

db_connection = mysql.connector.connect(user=user_name, password=password)
db_cursor = db_connection.cursor()

# read data from bronze database
query = """SELECT * FROM final_project_db_bronze.tv_bronze"""
tv_show_dirty_df = pd.read_sql(query,db_connection)

# clean out the quotation marks and replace nan with None
tv_show_cleaned = pd.DataFrame()
for (col_name, col) in tv_show_dirty_df.iteritems():
    col_cleaned = col.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
    tv_show_cleaned[col_name] = col_cleaned

# convert duration into numerical values
import re
tv_show_cleaned['duration'] = tv_show_cleaned['duration'].apply(lambda x: re.findall("[0-9]*", x)[0] if x and 'min' in x else None)

# generate new index for movie 
for i, row in enumerate(tv_show_cleaned['show_id']):
    tv_show_cleaned['show_id'][i] = "TVshow"+str(i)

# create silver database
db_cursor.execute("CREATE DATABASE IF NOT EXISTS final_project_db_silver;")
db_cursor.execute("USE final_project_db_silver;")

# create silver table
db_cursor.execute("""DROP TABLE IF EXISTS tv_silver;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS tv_silver
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

for i,row in tqdm(tv_show_cleaned.iterrows()):
    insert_sql = """INSERT INTO tv_silver
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s);"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()