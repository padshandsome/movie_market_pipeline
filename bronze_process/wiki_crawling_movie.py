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

amazon_movies = amazon_prime_titles[amazon_prime_titles['type']=="Movie"][['title']]
disney_plus_movies = disney_plus_titles[disney_plus_titles['type']=="Movie"][['title']]
hulu_movies = hulu_titles[hulu_titles['type']=="Movie"][['title']]
netflix_movies = netflix_titles[netflix_titles['type']=="Movie"][['title']]

movie_titles = []
movie_titles.extend(amazon_movies['title'])
movie_titles.extend(disney_plus_movies['title'])
movie_titles.extend(hulu_movies['title'])
movie_titles.extend(netflix_movies['title'])

movie_count = [len(amazon_movies),len(disney_plus_movies),len(hulu_movies),len(netflix_movies)]


movie_df = pd.DataFrame()
print("----------------Start Web Crawling Wikipedia-----------------")
for i in tqdm(range(len(movie_titles))):
    try:
        search_query = movie_titles[i]
        URL = "https://en.wikipedia.org/w/api.php?action=parse&page=" + search_query + "&format=json"

        response = requests.get(url=URL)
        response = response.json()
        save = response['parse']['text']

        for key in save.keys():
            save = save[key]
        save = str(save)

        try:
            budget_match = 'Budget<\/th><td class="infobox-data">.*?<'
            budget_match_result = re.findall(budget_match, save)[0]
            budget = re.findall("\$.*<",budget_match_result)[0]
            budget = budget[:-1]
        except:
            budget = None

        try:
            box_office = 'Box office<\/th><td class="infobox-data">.*?<'
            box_office_match_result = re.findall(box_office, save)[0]
            box_office = re.findall("\$.*<",box_office_match_result)[0]
            box_office = box_office[:-1]
        except:
            box_office = None 

        if i<movie_count[0]:
            platform = "amazon"
        elif i >= movie_count[0] and i<movie_count[1]:
            platform = "disney_plus"
        elif i >= movie_count[1] and i<movie_count[2]:
            platform = "hulu"
        else:
            platform = "netflix"

        
        schema = {"movieID":"Movie"+str(i),"title":search_query,"platform":platform,"budget":budget,"box_office":box_office}    
        movie_df = pd.concat([movie_df,pd.DataFrame([schema])],ignore_index = True)

    except:
        if i<movie_count[0]:
            platform = "amazon"
        elif i >= movie_count[0] and i<movie_count[1]:
            platform = "disney_plus"
        elif i >= movie_count[1] and i<movie_count[2]:
            platform = "hulu"
        else:
            platform = "netflix"

        schema = {"movieID":"Movie"+str(i),"title":movie_titles[i],"platform":platform,"budget":None,"box_office":None}    
        movie_df = pd.concat([movie_df,pd.DataFrame([schema])],ignore_index = True)

movie_df.to_csv("movie_revenue.csv")