import pandas as pd 
import numpy as np
import re 
from tqdm import tqdm
import requests

file_path = "**"
amazon_prime_titles = pd.read_csv(file_path+"amazon_prime_titles.csv")
disney_plus_titles = pd.read_csv(file_path + "disney_plus_titles.csv")
hulu_titles = pd.read_csv(file_path + "hulu_titles.csv")
netflix_titles = pd.read_csv(file_path + "netflix_titles.csv")

amazon_director = amazon_prime_titles['director'].unique()
disney_plus_director = disney_plus_titles['director'].unique()
hulu_director = hulu_titles['director'].unique()
netflix_director = netflix_titles['director'].unique()

director_unique_list = []
director_unique_list.extend(amazon_director)
director_unique_list.extend(disney_plus_director)
director_unique_list.extend(hulu_director)
director_unique_list.extend(netflix_director)


director_df = pd.DataFrame()
for i in tqdm(range(len(director_unique_list))):
    try:
        search_query = director_unique_list[i]
        URL = "https://en.wikipedia.org/w/api.php?action=parse&page="+search_query + "&format=json"

        response = requests.get(url=URL)
        response = response.json()
        save = response['parse']['text']

        for key in save.keys():
            save = save[key]
        save = str(save)

        try:
            bday_match = '<span class="bday">.*<\/span>\)'
            bday_match_result = re.findall(bday_match, save)[0]
            bday = re.findall("[0-9]*-[0-9]*-[0-9]*",bday_match_result)
        except:
            bday = None

        try:
            birthplace_match = '<div style="display:inline" class="birthplace"><a href="\/wiki\/.*?" title=".*?">.*?<\/a>'
            birthplace_match_result = re.findall(birthplace_match, save)[0]
            birthplace = re.findall(">[A-Z][a-z, ]*.*?<",birthplace_match_result)[0][1:-1]
        except: 
            birthplace = None 
        
        schema = {"directorID":"director"+str(i),"name":search_query, "bod": bday, "birthplace":birthplace}
        director_df = pd.concat([director_df, pd.DataFrame(schema)])

    except:
        schema = {"directorID":"director"+str(i),"name":director_unique_list[i], "bod": None, "birthplace":None}
        director_df = pd.concat([director_df, pd.DataFrame([schema])])

director_df.to_csv("./director_df.csv")
