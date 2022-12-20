import mysql.connector
import pandas as pd
from tqdm import tqdm

user_name = "root"
password = "*"

db_connection = mysql.connector.connect(user=user_name, password=password)
db_cursor = db_connection.cursor()

# create gold database
db_cursor.execute("CREATE DATABASE IF NOT EXISTS final_project_db_gold;")
db_cursor.execute("USE final_project_db_gold;")

# create gold table
db_cursor.execute("""DROP TABLE IF EXISTS director_gold;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS director_gold
                        (director_id varchar(255),
                        name text,
                        bod DATE,
                        birthplace text
                        );""")

db_cursor.execute("""INSERT INTO director_gold (director_id,
                                                name,
                                                bod,
                                                birthplace) 
                    SELECT director_id,
                            name,
                            bod,
                            birthplace
                     FROM final_project_db_silver.director_silver;""")

# create movie_revenue gold table
db_cursor.execute("""DROP TABLE IF EXISTS movie_revenue_gold;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS movie_revenue_gold
                        (movie_id varchar(255),
                        title text,
                        platform varchar(255),
                        budget double,
                        box_office double
                        );""")

db_cursor.execute("""INSERT INTO movie_revenue_gold (movie_id,
                                                title,
                                                platform,
                                                budget,
                                                box_office) 
                    SELECT movie_id,
                            title,
                            platform,
                            budget,
                            box_office
                     FROM final_project_db_silver.movie_revenue_silver;""")


# create movie gold table
db_cursor.execute("""DROP TABLE IF EXISTS movie_gold;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS movie_gold
                        (movie_id varchar(255),
                        type varchar(255),
                        title varchar(255),
                        director text,
                        director_bod DATE,
                        director_birthplace text,
                        cast text,
                        country varchar(255),
                        date_added varchar(255),
                        release_year varchar(255),
                        rating varchar(255),
                        duration int,
                        listed_in varchar(255),
                        description text,
                        platform varchar(255),
                        budget double,
                        box_office double,
                        platform_box_office_rank int,
                        total_box_office_rank int 
                        );""")
db_cursor.execute("""INSERT INTO movie_gold (movie_id,
                                                type,
                                                title,
                                                director,
                                                director_bod,
                                                director_birthplace,
                                                cast,
                                                country,
                                                date_added,
                                                release_year,
                                                rating,
                                                duration,
                                                listed_in,
                                                description,
                                                platform,
                                                budget,
                                                box_office,
                                                platform_box_office_rank,
                                                total_box_office_rank
                                                ) 
                    SELECT a.show_id,
                            a.type,
                            a.title,
                            a.director,
                            c.bod,
                            c.birthplace,
                            a.cast,
                            a.country,
                            a.date_added,
                            a.release_year,
                            a.rating,
                            a.duration,
                            a.listed_in,
                            a.description,
                            a.platform,
                            b.budget,
                            b.box_office,
                            rank() over(partition by a.platform order by b.box_office DESC)  as platform_box_office_rank,
                            rank() over(order by b.box_office DESC ) as total_box_office_rank
                     FROM final_project_db_silver.movie_silver a 
                     left join final_project_db_silver.movie_revenue_silver b ON regexp_substr(a.show_id,"Movie[0-9]+") = b.movie_id
                     left join final_project_db_silver.director_silver c ON a.director = c.name;""")


# create tvshow gold table
db_cursor.execute("""DROP TABLE IF EXISTS tv_gold;""")
db_cursor.execute("""CREATE TABLE IF NOT EXISTS tv_gold
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
db_cursor.execute("""INSERT INTO tv_gold(show_id,
                                            type,
                                            title,
                                            director,
                                            cast,
                                            country,
                                            date_added,
                                            release_year,
                                            rating,
                                            duration,
                                            listed_in,
                                            description,
                                            platform)
                        SELECT * FROM final_project_db_silver.tv_silver""")

db_connection.commit()