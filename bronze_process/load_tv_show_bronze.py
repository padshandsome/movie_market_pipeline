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
file_path = "**"
amazon_prime_titles = pd.read_csv(file_path+"amazon_prime_titles.csv")
disney_plus_titles = pd.read_csv(file_path + "disney_plus_titles.csv")
hulu_titles = pd.read_csv(file_path + "hulu_titles.csv")
netflix_titles = pd.read_csv(file_path + "netflix_titles.csv")

amazon_tv = amazon_prime_titles[amazon_prime_titles['type']=="TV Show"]
amazon_tv["platform"] = 'amazon'
disney_plus_tv = disney_plus_titles[disney_plus_titles['type']=="TV Show"]
disney_plus_tv["platform"] = 'disney_plus'
hulu_tv = hulu_titles[hulu_titles['type']=="TV Show"]
hulu_tv["platform"] = 'hulu'
netflix_tv = netflix_titles[netflix_titles['type']=="TV Show"]
netflix_tv["platform"] = 'netflix'

# create table movie_bronze
db_cursor.execute("""DROP TABLE IF EXISTS tv_bronze;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS tv_bronze
                        (show_id varchar(255),
                        type varchar(255),
                        title varchar(255),
                        director text,
                        cast text,
                        country varchar(255),
                        date_added varchar(255),
                        release_year varchar(255),
                        rating varchar(255),
                        duration varchar(255),
                        listed_in varchar(255),
                        description text,
                        platform varchar(255)
                        );""")

for i,row in tqdm(amazon_tv.iterrows()):
    insert_sql = """INSERT INTO tv_bronze
                VALUES("%s", "%s", "%s","%s","%s","%s","%s", "%s", "%s","%s","%s","%s","%s");"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()


for i,row in tqdm(disney_plus_tv.iterrows()):
    insert_sql = """INSERT INTO tv_bronze
                VALUES("%s", "%s", "%s","%s","%s","%s","%s", "%s", "%s","%s","%s","%s","%s");"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()


for i,row in tqdm(hulu_tv.iterrows()):
    insert_sql = """INSERT INTO tv_bronze
                VALUES("%s", "%s", "%s","%s","%s","%s","%s", "%s", "%s","%s","%s","%s","%s");"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()

for i,row in tqdm(netflix_tv.iterrows()):
    insert_sql = """INSERT INTO tv_bronze
                VALUES("%s", "%s", "%s","%s","%s","%s","%s", "%s", "%s","%s","%s","%s","%s");"""
    db_cursor.execute(insert_sql, tuple(row))
db_connection.commit()