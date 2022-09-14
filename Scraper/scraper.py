import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import mysql.connector

#==============================================================
#===============  Load credentials and settings ===============
#==============================================================

load_dotenv()

PERSONAL_USE_SCRIPT = os.getenv('PERSONAL_USE_SCRIPT')
SECRET = os.getenv('SECRET')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME')
PASS = os.getenv('PASS')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASS = os.getenv('DB_PASS')
DB_IP = os.getenv('DB_IP')
DB_NAME = os.getenv('DB_NAME')

settingsJson = open('Scraper/settings.json')
settings = json.load(settingsJson)

SUBREDDIT = settings['Subreddit']
SORTBY = settings['SortBy']
VIDEOSCOUNT = settings['VideosCount']
MAXPOSTSREQ = settings['MaxPostsReq']

#==============================================================

def df_from_response(res):
    lastPost = None
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()

    posts = res.json()['data']['children']
    if len(posts)>0 :
        lastPost = posts[len(posts)-1]['data']['name']
    else:
        print(res)
    
    # loop through each post pulled from res and append to df
    for post in posts:
        if post['data']['is_video']:
            global VIDEOSCOUNT
            VIDEOSCOUNT -= 1

            df = df.append({
                'name': post['data']['name'],
                'created_utc': post['data']['created_utc'],
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'author': post['data']['author'],
                'permalink': post['data']['permalink'],
                'vid_downloaded': False
            }, ignore_index=True)

    return df, lastPost

auth = requests.auth.HTTPBasicAuth(PERSONAL_USE_SCRIPT, SECRET)

login_data = {
    'grant_type' : 'password',
    'username' : REDDIT_USERNAME,
    'password' : PASS
}

headers = {'User-Agent': 'HotVideosScrapper/0.0.1'}

response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=login_data, headers=headers)

token = response.json()['access_token']
headers = {**headers, **{'Authorization': f'bearer {token}'}}

videoPosts = pd.DataFrame()
params = {'limit': MAXPOSTSREQ}

while VIDEOSCOUNT > 0:

    # make request
    if SORTBY == 'top':
        res = requests.get("https://oauth.reddit.com/r/" + str(SUBREDDIT) + "/top/?t=all",
                       headers=headers,
                       params=params)
    else:
        res = requests.get("https://oauth.reddit.com/r/" + str(SUBREDDIT) + "/" + str(SORTBY),
                       headers=headers,
                       params=params)

    # get dataframe from response
    new_df, oldestPostName = df_from_response(res)
    print(oldestPostName)

    if oldestPostName is None :
        break

    # add/update fullname in params
    params['after'] = oldestPostName
    
    # append new_df to videoPosts
    videoPosts = videoPosts.append(new_df, ignore_index=True)

#print(videoPosts)

mydb = mysql.connector.connect(
  host=DB_IP,
  user=DB_USERNAME,
  password=DB_PASS,
  database=DB_NAME
)

mycursor = mydb.cursor()

sql = "INSERT INTO vids (name, created_utc, subreddit, title, author, permalink, vid_downloaded) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=id"
postsData = list(videoPosts.itertuples(index=False, name=None))

mycursor.executemany(sql, postsData)
mydb.commit()

print(mycursor.rowcount, " rows were inserted.")

