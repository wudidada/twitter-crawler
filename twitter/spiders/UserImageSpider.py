import logging
import math

import scrapy
import tweepy

from twitter import mongo
from twitter.settings import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET

logger = logging.getLogger(__name__)


class UserImageSpider(scrapy.Spider):
    name = 'userimagespider'

    start_urls = ['https://www.baidu.com/']

    def parse(self, response):

        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        api = tweepy.API(auth)

        for user_id in api.friends_ids():
            logger.info('start crawl %s', user_id)

            old_max = max_id = mongo.get_max_rec(user_id)
            old_min = min_id = mongo.get_min_rec(user_id)

            if max_id and not min_id and mongo.down_recent(max_id, 0):
                continue

            query = {'id': user_id, 'count': 100, 'include_rts': False,
                     'tweet_mode': 'extended'}
            if min_id:
                query.update(max_id=min_id)
            elif max_id:
                query.update(since_id=max_id)

            if not min_id:
                min_id = old_min = math.inf
            if not max_id:
                max_id = old_max = 0

            cnt = 0
            for tweet in tweepy.Cursor(api.user_timeline, **query).items():
                cnt += 1
                if tweet.id > max_id:
                    max_id = tweet.id
                if tweet.id < min_id:
                    min_id = tweet.id
                if cnt % 100 == 0 and min_id != old_min:
                    mongo.rec_min(user_id, min_id)

                yield tweet._json
            if max_id > old_max:
                mongo.rec_max(user_id, max_id)

            mongo.del_rec(user_id, 'min')
            logger.info('end crawl %s', user_id)
