from include.etl.reddit import connect_to_reddit, extract_posts, transform_data, load_data_to_csv
from include.utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
import pandas as pd
import os


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    print('Loading reddit data...')

    # connecting to reddit instance
    instance = connect_to_reddit(CLIENT_ID, SECRET, 'USER AGENT')

    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)

    # transformation
    post_df = transform_data(post_df)

    # loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path
