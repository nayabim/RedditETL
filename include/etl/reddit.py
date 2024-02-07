from praw import Reddit
import praw
import sys
import numpy as np
from include.utils.constants import POST_FIELDS
import pandas as pd

def connect_to_reddit(client_id, client_secret, user_agent):
    print("Connecting to Reddit...")
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )

        print("connected to reddit!")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)


def extract_posts(redit_instance: Reddit, subreddit: str, time_filter: str, limit: None):
    subreddit = redit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []
    for post in posts:
        post_dict = vars(post)
        # print(post_dict)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)

    return post_lists


def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df

import os
def load_data_to_csv(data: pd.DataFrame, path: str):
    # print(f"ghf{path}")
    # if not os.path.exists("/usr/local/airflow/data/output1/reddit_20240104.csv"):
    #     os.makedirs("/usr/local/airflow/data/output1/")
    #     print('path found')
    try:
        data.to_csv(path, index=False)
    except Exception as e:
        print(str(e))