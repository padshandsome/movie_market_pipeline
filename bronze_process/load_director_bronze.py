import mysql.connector
import pandas as pd
from tqdm import tqdm

user_name = "root"
password = "*"

db_connection = mysql.connector.connect(user=user_name, password=password)
db_cursor = db_connection.cursor()

# create database
db_cursor.execute("CREATE DATABASE IF NOT EXISTS final_project_db_bronze;")
db_cursor.execute("USE final_project_db_bronze;")

# read raw data into the bronze database
file_path = "./bronze_process/director_df.csv"
director_df = pd.read_csv(file_path)
director_df = director_df[["directorID","name","bod","birthplace"]]

# create table director_Bronze
db_cursor.execute("""DROP TABLE IF EXISTS director_bronze;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS director_bronze
                        (director_id varchar(255),
                        name text,
                        bod varchar(255),
                        birthplace text
                        );""")

for i,row in tqdm(director_df.iterrows()):
    insert_sql = """INSERT INTO director_bronze
                VALUES("%s", "%s", "%s","%s");"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()

