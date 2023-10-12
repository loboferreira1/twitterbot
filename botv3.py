import logging
import tweepy
import time
import datetime
import pandas as pd
from config import api_key, api_key_secret, access_token, access_token_secret, bearer_token


MAX_TWEETS_PER_DAY = 48
MAX_TWEETS_PER_MONTH = 1500
MAX_REST_DURATION_SECONDS = 43400
MAX_INTERVAL = 5.7 * 60 * 60 

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

#Disclaimer: I don't document simple functions, too lazy.
def load_data():
    # Define the filename with double backslashes for Windows
    filename = "F:\\Scripts\\frasesbd.csv"

    # Load the XLSX file into a DataFrame
    df = pd.read_csv(filename)
    return df

def format_tweet(frase, autor):
    # Format the tweet as "frase" -autor
    return f'"{frase}" -{autor}'

'''
def keep_alive():
    try:
        # Create a client instance with your API keys
        client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret, wait_on_rate_limit=True)
        
        # Send a simple request (e.g., fetching your user information)
        client.get_user("loboshoot")
    except Exception as e:
        logger.error(f"Error in keep_alive: {e}")

'''

def tweet_message(client, message):
    try:
        '''
        Use the v2 endpoint to create a tweet. By doing this, you guarantee
        the script won't get a timeout error due to it eventually waiting on 
        time.sleep() for one reason or another, which one could get by calling 
        tweepy.Client() on main().
        '''

        client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret, wait_on_rate_limit=True)
        client.create_tweet(text=message)
        logger.info('Tweeted successfully: {}'.format(message))

    #Handling common (and uncommon!) errors.    
    except tweepy.errors.TweepyException as e:
        if "limit" in str(e):
            logger.info('Tweet limit reached. Sleeping for 3 hours')
            time.sleep(3 * 60 * 60)
        else:
            logger.error(e)
            raise e
    except Exception as e:
        logger.error('An error occurred while tweeting: {}'.format(e))

def check_tweet_limit(daily_tweet_count):
    return daily_tweet_count >= MAX_TWEETS_PER_DAY


def main(interval):
    # Start the script
    logger.info('Starting the script')

    # Create a Tweepy client
    client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)

    df = load_data()

    # Initialize the daily and monthly tweet count
    daily_tweet_count = 0
    monthly_tweet_count = 0
    current_month = datetime.datetime.today().month
    last_month = current_month

    for index, row in df.iterrows():
        frase = row['frase']
        autor = row['autor']
        tweet = format_tweet(frase, autor)

        print(f"Tweeting: {tweet}")  # DEBUG what's being tweeted

        tweet_message(client, tweet)
        
        daily_tweet_count += 1
        monthly_tweet_count += 1
        

        time.sleep(interval)

        if check_tweet_limit(daily_tweet_count):
            logger.info('Tweet limit for the day reached. Resting for {} seconds'.format(MAX_REST_DURATION_SECONDS))
            time.sleep(MAX_REST_DURATION_SECONDS)
            daily_tweet_count = 0

        # Check if it's a new month and reset the monthly count
        current_month = datetime.datetime.today().month
        if current_month != last_month:
            logger.info('New month started. Resetting monthly tweet count.')
            last_month = current_month
            monthly_tweet_count = 0

        if monthly_tweet_count >= MAX_TWEETS_PER_MONTH:
            logger.info('Monthly tweet limit reached. Waiting for the new month.')
            while current_month == last_month:
                time.sleep(60*60)  # Sleep for an hour and check the month again, since, with current settings, it's highly unlikely it will ever hit it

       
    logger.info('Script finished')

if __name__ == '__main__':
    
    interval = MAX_INTERVAL

    main(interval)