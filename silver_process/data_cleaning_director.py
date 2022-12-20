import mysql.connector
import pandas as pd
from tqdm import tqdm

user_name = "root"
password = "*"

db_connection = mysql.connector.connect(user=user_name, password=password)
db_cursor = db_connection.cursor()

# read data from bronze database
query = """SELECT * FROM final_project_db_bronze.director_bronze"""
director_dirty_df = pd.read_sql(query,db_connection)

director_id = director_dirty_df['director_id']
director_name = director_dirty_df['name']
director_bod = director_dirty_df['bod']
director_birthplace = director_dirty_df['birthplace']

# clean out all the quotation marks and nan
director_id_cleaned = director_id.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
director_name_cleaned = director_name.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
director_bod_cleaned = director_bod.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)
director_birthplace_cleaned = director_birthplace.apply(lambda x:x.replace("'","")).apply(lambda x: None if x == 'nan' else x)

director_cleaned = pd.DataFrame()
director_cleaned['director_id'] = director_id_cleaned
director_cleaned['name'] = director_name_cleaned
director_cleaned['bod'] = director_bod_cleaned
director_cleaned['birthplace'] = director_birthplace_cleaned


# create silver database
db_cursor.execute("CREATE DATABASE IF NOT EXISTS final_project_db_silver;")
db_cursor.execute("USE final_project_db_silver;")

# create silver table
db_cursor.execute("""DROP TABLE IF EXISTS director_silver;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS director_silver
                        (director_id varchar(255),
                        name text,
                        bod DATE,
                        birthplace text
                        );""")

for i,row in tqdm(director_cleaned.iterrows()):
    insert_sql = """INSERT INTO director_silver
                VALUES(%s, %s, %s, %s);"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()