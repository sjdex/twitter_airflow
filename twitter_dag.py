from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import tweepy
import pandas as pd
import json
import s3fs
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

def fetchAndLoadToS3():
    access_key = "" 
    access_secret = ""
    consumer_key = ""
    consumer_secret = ""

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object 
    api = tweepy.API(auth)
    
    # Real Madrid
    madrid_tweets = api.user_timeline(
        screen_name='@realmadrid',
        count=200,
        include_rts=False,
        tweet_mode='extended'
    )

    madrid_tweet_list=[]

    for tweet in madrid_tweets:
        refined_tweet = {'user': tweet.user.screen_name,
                        'id': tweet.id,
                        'lang' : tweet.lang,
                        'source': tweet.source,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        madrid_tweet_list.append(refined_tweet)

    madrid_df = pd.DataFrame(madrid_tweet_list)
    madrid_csv_string = madrid_df.to_csv(index=False)

    # Upload the CSV file to S3
    s3_hook = S3Hook() 
    s3_hook.load_string(madrid_csv_string, key='madrid_tweets.csv', bucket_name='dexter-twitter-data-raw', replace=True)

    # FC Barcelona
    barca_tweets = api.user_timeline(
        screen_name='@FCBarcelona',
        count=200,
        include_rts=False,
        tweet_mode='extended'
    )

    barca_tweet_list=[]

    for tweet in barca_tweets:
        refined_tweet = {'user': tweet.user.screen_name,
                        'id': tweet.id,
                        'lang' : tweet.lang,
                        'source': tweet.source,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        barca_tweet_list.append(refined_tweet)

    barca_df = pd.DataFrame(barca_tweet_list)
    barca_csv_string = barca_df.to_csv(index=False)

    # Upload the CSV file to S3
    s3_hook_2 = S3Hook() 
    s3_hook_2.load_string(barca_csv_string, key='barca_tweets.csv', bucket_name='dexter-twitter-data-raw', replace=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':0
}

with DAG(
        'twitter_dag',
        default_args=default_args,
        description='Twitter Data Pipeline!',
        schedule_interval=None,
        max_active_runs=1
) as dag:

    # Fetch data from Twitter API and load to S3
    fetch_data_and_load_to_S3 = PythonOperator(
        task_id='fetch_data_and_load_to_S3',
        python_callable=fetchAndLoadToS3
    )

    # Process data from S3, transform using spark and store it back to S3
    glue_etl_job = GlueJobOperator(
        task_id='glue_etl_job',
        job_name='twitter-glue-etl',
        aws_conn_id='aws_default',
        region_name='ap-south-1',
        retry_limit=0
    )

    # create_sf_schema = SnowflakeOperator(
    #     task_id='create_sf_schema',
    #     sql='CREATE SCHEMA IF NOT EXISTS twitter_de',
    #     snowflake_conn_id='',
    #     autocommit=True
    # )

    # create_sf_table = SnowflakeOperator(
    #     task_id='create_sf_table',
    #     sql='''CREATE TABLE IF NOT EXISTS twitter_de.tweets(
    #         user VARCHAR(30),
    #         tweet_id BIGINT,
    #         language TEXT(10),
    #         source TEXT(30),
    #         likes INT,
    #         retweets INT,
    #         date DATE
    #     )''',
    #     snowflake_conn_id='',
    #     autocommit=True
    # )

    # load_into_snowflake
    load_into_snowflake = S3ToSnowflakeOperator(
        task_id="load_into_snowflake",
        snowflake_conn_id='snowflake_default',
        s3_keys=['teams.csv'],
        schema='twitter_de',
        table='tweets',
        stage='TWITTER_STAGE',
        file_format='csv_file_format'
    )

    fetch_data_and_load_to_S3 >> glue_etl_job >> load_into_snowflake
