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

# load .env
load_dotenv()

PERSONAL_USE_SCRIPT = os.getenv('PERSONAL_USE_SCRIPT')
SECRET = os.getenv('SECRET')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME')
PASS = os.getenv('PASS') # reddit pass
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASS = os.getenv('DB_PASS')
DB_IP = os.getenv('DB_IP')
DB_NAME = os.getenv('DB_NAME')

# load settings.json
settingsJson = open('Scraper/settings.json')
settings = json.load(settingsJson)

SUBREDDIT = settings['Subreddit'] # which subreddit to scrape for videos
SORTBY = settings['SortBy'] # sort posts by top, hot, new, rising
VIDEOSCOUNT = settings['VideosCount'] # max number of videos to scrape
MAXPOSTSREQ = settings['MaxPostsReq'] # number of posts in the response from reddit

#==============================================================

# function to create dataframe containing all video posts we got from the response
def df_from_response(res):
    # we use the last post parameter in the next request to tell reddit we need the NEXT x per number of posts after the last post
    lastPost = None
    df = pd.DataFrame()

    # get all posts from the response
    posts = res.json()['data']['children']
    # if we have ANY posts then save the last post
    if len(posts)>0 :
        lastPost = posts[len(posts)-1]['data']['name']
    else:
        print(res)
    
    # loop through each post pulled from response and append to dataframe
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

# set the user agent according to the app, not just some default bs
headers = {'User-Agent': 'HotVideosScrapper/0.0.1'}

# response containing the token
response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=login_data, headers=headers)

token = response.json()['access_token']
headers = {**headers, **{'Authorization': f'bearer {token}'}}

videoPosts = pd.DataFrame()
# param which tells reddit how much posts to return
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
    
    # if response has no posts we are done
    if oldestPostName is None :
        break

    # add/update oldest post name in params
    params['after'] = oldestPostName
    
    # append new_df to videoPosts
    videoPosts = videoPosts.append(new_df, ignore_index=True)


# connect to the db
mydb = mysql.connector.connect(
  host=DB_IP,
  user=DB_USERNAME,
  password=DB_PASS,
  database=DB_NAME
)

# insert all collected video posts
mycursor = mydb.cursor()

sql = "INSERT INTO vids (name, created_utc, subreddit, title, author, permalink, vid_downloaded) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id=id"
postsData = list(videoPosts.itertuples(index=False, name=None))

mycursor.executemany(sql, postsData)
mydb.commit()

print()
print(mycursor.rowcount, " rows were inserted.")
