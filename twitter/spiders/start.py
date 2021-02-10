# -*- coding: utf-8 -*-
import json

import tweepy

from twitter import mongo
from twitter.settings import ACCESS_TOKEN, ACCESS_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)


def get_older_tweet(user_id, min_id):
    min = min_id
    for page in tweepy.Cursor(api.user_timeline, id=user_id, max_id=min_id, count=100).pages():
        for tweet in page:
            if tweet.id < min:
                min = tweet.id

            print(min)

        mongo.rec_min(user_id, min)
    mongo.del_rec(user_id, 'min')


def get_newer_tweet(user_id, max_id):
    max = max_id
    for page in tweepy.Cursor(api.user_timeline, id=user_id, since_id=max_id, count=100, include_rts=False,
                              exclude_replies=True).pages():
        for tweet in page:
            if tweet.id > max:
                max = tweet.id

        mongo.rec_max(user_id, max)


def get_all_tweet(user_id):
    old_max = max_id = mongo.get_max_rec(user_id)

    query = {'id': user_id, 'count': 100, 'include_rts': False,
             'exclude_replies': True, 'tweet_mode': 'extended'}
    if max_id:
        query.update(max_id=max_id)
    else:
        old_max = max_id = 0
    for tweet in tweepy.Cursor(api.user_timeline, **query).items(10):
        print(tweet.full_text, tweet.created_at.time())
        if tweet.id > max_id:
            max_id = tweet.id
        print(json.dumps(tweet._json, ensure_ascii=False))
    if max_id > old_max:
        mongo.rec_max(user_id, max_id)


def main():
    uid = 937122453909782528
    get_all_tweet(uid)


if __name__ == '__main__':
    main()
