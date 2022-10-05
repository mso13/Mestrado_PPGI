import os
import sys
import json
import tweepy
from datetime import datetime, timedelta


def get_twitter_api_access(consumer_key, consumer_secret, access_token, access_token_secret):
    """
    Set up Authentication Instance.
    """

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)
    
    return api

## Display Info Functions

def display_hashtag_info(hashtag_list):
    if len(hashtag_list) == 0:
        print ('\tNo Hashtags mentioned.')
    else:
        for index, hashtag in enumerate(hashtag_list):
            print (f"\t{index+1}. {hashtag['text'].upper()}")

def display_url_info(url_list):
    if len(url_list) == 0:
        print ('\tNo URLs mentioned.')
    else:
        for index, url in enumerate(url_list):
            print (f"\t{index+1}. {url['expanded_url']}")

def display_media_info(entities_json):
    if 'media' not in entities_json.keys():
        print ('\tNo media available.')
    else:
        media_list = entities_json['media']
        for index, media in enumerate(media_list):
            print (f"\t{index+1}. {media['type']} {media['url']}")

def display_tweet_info(tweet):
    # User Specific Info
    user = tweet['user']
    
    print ('--' * 50)
            
    print ('\nTweet information:')
    print (f"  - Tweet ID: {tweet['id']}")
    print (f"  - Created at: {tweet['created_at']}")
    print (f"  - Text: {tweet['text']}")

    print ('\nUser information:')
    print (f"  - User ID: {user['id']}")
    print (f"  - Name: {user['name']}")
    print (f"  - Username: @{user['screen_name']}")

    print ('\nMetadata:')
    print (f"  - Hashtags: ")
    display_hashtag_info(tweet['entities']['hashtags'])
    print (f"  - URLs: ")
    display_url_info(tweet['entities']['urls'])
    print (f"  - Media: ")
    display_media_info(tweet['entities'])

def get_json_object(tweet) -> dict:

    results_dict = dict()

    # User Specific Info
    user = tweet['user']

    # Convert Date Format and UTC
    dt_str = tweet['created_at']
    dt_obj = datetime.strptime(dt_str, '%a %b %d %H:%M:%S +%f %Y') - timedelta(hours=3)
    dt_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S')

    # Store Search Date
    now_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    results_dict['tweet_id'] = str(tweet['id'])
    results_dict['created_at'] = dt_str
    results_dict['search_dt'] = now_dt
    results_dict['text'] = tweet['full_text']
    results_dict['user_id'] = str(user['id'])
    results_dict['screen_name'] = user['screen_name']
    results_dict['rt_count'] = tweet['retweet_count']
    results_dict['favorite_count'] = tweet['favorite_count']

    return results_dict
    
def save_tweets_history(results_list: list, user_name: str) -> None:
    # Save the list of dicts
    with open(f'./results_related/{user_name}.json', 'w', encoding='utf8') as f:
        json.dump(results_list, f, ensure_ascii=False)


## Get Twitter User Timeline
def get_user_timeline(screen_name, user_id):
    """
    This method can only return up to 3,200 of a user's most recent Tweets.
    """

    user_timeline = tweepy.Cursor(api.user_timeline,
                              user_id=user_id,
                              screen_name=screen_name, 
                              count=3200,
                              exclude_replies=True,
                              include_rts=False,
                              tweet_mode='extended').items()

    return user_timeline

if __name__ == '__main__':
    # Keys and Access Tokens
    fpath_credentials: str = os.path.join(
        '../../../', 'resources', 'twitter_credentials.json'
    )

    with open(fpath_credentials, 'r') as file:
        credentials = json.load(file)
        
    # Keys and Access Tokens
    consumer_key = credentials['api_key']
    consumer_secret = credentials['api_key_secret']

    access_token = credentials['access_token']
    access_token_secret = credentials['access_token_secret']

    # The API class provides access to the entire twitter RESTful API methods.
    api = get_twitter_api_access(consumer_key, consumer_secret, access_token, access_token_secret)

    # Get username from argument list
    search_user = sys.argv[1] # Skips first, since it is the filename 'crawler_twitter.py'

    screen_name, user_id = None, None

    # Get user_id from Twitter
    find_users = api.search_users(search_user)

    for user in find_users:
        data = user._json

        if data['screen_name'] == search_user:
            screen_name = data['screen_name']
            user_id = data['id']

    # Verify user_id is not None
    assert user_id is not None

    # Get user Timeline
    user_timeline = get_user_timeline(screen_name, user_id)

    # Store search results
    results_list = list()

    for t in user_timeline:
        tweet = t._json

        results_dict = get_json_object(tweet)
        results_list.append(results_dict)

    print ('Quantidade de Tweets coletados: ', len(results_list))

    # Store results in JSON object
    save_tweets_history(results_list, screen_name)

    