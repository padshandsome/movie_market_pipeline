import mysql.connector
import pandas as pd
from tqdm import tqdm

user_name = "root"
password = "*"

db_connection = mysql.connector.connect(user=user_name, password=password)
db_cursor = db_connection.cursor()

# read data from bronze database
query = """SELECT * FROM final_project_db_bronze.movie_revenue_bronze"""
revenue_dirty_df = pd.read_sql(query,db_connection)

movie_id = revenue_dirty_df['movie_id']
title = revenue_dirty_df['title']
platform = revenue_dirty_df['platform']
budget = revenue_dirty_df['budget']
box_office = revenue_dirty_df['box_office']

# clean out all the quotation marks and nan
movie_id_cleaned = movie_id.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
title_cleaned = title.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
platform_cleaned = platform.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
budget_cleaned = budget.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
box_office_cleaned = box_office.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)

# convert budget and box_office into numerical value
import re
for i,row in enumerate(budget_cleaned):
    if row == None:
        continue
    try:
        number = float(re.findall("[0-9][0-9.]*[0-9]",row)[0])
    except:
        try:
            number = float(re.findall("[0-9]",row)[0])
        except:
            number = None
    if "million" in row:
        number = 1000000 * number
    if 'billion' in row:
        number = 1000000000 * number
    budget_cleaned[i] = number 

for i,row in enumerate(box_office_cleaned):
    if row == None:
        continue
    try:
        number = float(re.findall("[0-9][0-9.]*[0-9]",row)[0])
    except:
        try:
            number = float(re.findall("[0-9]",row)[0])
        except:
            number = None
    if "million" in row:
        number = 1000000 * number
    if 'billion' in row:
        number = 1000000000 * number
    box_office_cleaned[i] = number 


movie_revenue_cleaned = pd.DataFrame()
movie_revenue_cleaned['movie_id'] = movie_id_cleaned
movie_revenue_cleaned['title'] = title_cleaned
movie_revenue_cleaned['platform'] = platform_cleaned
movie_revenue_cleaned['budget'] = budget_cleaned
movie_revenue_cleaned['box_office'] = box_office_cleaned

# create silver database
db_cursor.execute("CREATE DATABASE IF NOT EXISTS final_project_db_silver;")
db_cursor.execute("USE final_project_db_silver;")

# create silver table
db_cursor.execute("""DROP TABLE IF EXISTS movie_revenue_silver;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS movie_revenue_silver
                        (movie_id varchar(255),
                        title text,
                        platform varchar(255),
                        budget double,
                        box_office double
                        );""")

for i,row in tqdm(movie_revenue_cleaned.iterrows()):
    insert_sql = """INSERT INTO movie_revenue_silver
                VALUES(%s, %s, %s, %s, %s);"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()