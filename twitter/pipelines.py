# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
import os
from datetime import datetime
from email.utils import parsedate
from urllib.parse import urlparse

import scrapy
from scrapy.pipelines.files import FilesPipeline

from twitter import mongo

logger = logging.getLogger(__name__)


def parse_datetime(string):
    return datetime(*(parsedate(string)[:6]))


class TwitterPipeline:

    def process_item(self, item, spider):
        mongo.save_tweet(item)
        return item


class ImagePipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        if item.get('extended_entities'):
            i = 1
            for m in item.get('extended_entities').get('media'):
                url = m.get('media_url') + '?name=orig'
                logger.info('image %s : %s [%s]', i, url, item.get('id_str'))
                yield scrapy.Request(url=url, meta={'item': item, 'index': i})
                try:
                    video_url = m.get('video_info').get('variants')[0].get('url')
                    yield scrapy.Request(url=video_url, meta={'item': item, 'index': i})
                except:
                    pass

                i += 1

    def file_path(self, request, response=None, info=None):
        item = request.meta['item']
        index = request.meta['index']

        username = item.get('user').get('screen_name')
        tweet_id = item.get('id_str')
        tweet_time = parse_datetime(item.get('created_at')).strftime('%Y%m%d')

        url_path = urlparse(request.url).path
        ext = os.path.splitext(url_path)[1]

        filename = '{}-{}-{}_{}{}'.format(username, tweet_time, tweet_id, index, ext)
        path = username + os.path.sep + filename
        logger.info('file path : %s [%s]', path, request.url)
        return path
