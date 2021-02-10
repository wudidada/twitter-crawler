from datetime import datetime

from pymongo import MongoClient

host = 'localhost:27017'
db_name = 'sns'
msg_col = 'tweet'
rec_col = 'rec'

time_format = "%Y/%m/%d %H:%M:%S"

client = MongoClient(host)
db = client[db_name]


def time_now():
    now = datetime.now()
    return format_date(now)


def format_date(date):
    return date.strftime(time_format)


def rec_min(user_id, min_id):
    if user_id and min_id:
        db[rec_col].insert_one({'u_id': user_id, 'min': min_id, 'time': time_now()})


def del_rec(user_id, param):
    db[rec_col].delete_many({'u_id': user_id, param: {'$exists': True}})


def rec_max(user_id, max_id):
    if user_id and max_id:
        db[rec_col].insert_one({'u_id': user_id, 'max': max_id, 'time': time_now()})


def get_max_rec(user_id):
    if not user_id:
        return

    max_rec = db[rec_col].find({'u_id': user_id, 'max': {'$exists': True}}).sort([('max', -1)]).limit(1)
    for rec in max_rec:
        return rec.get('max')


def get_min_rec(user_id):
    if not user_id:
        return

    min_rec = db[rec_col].find({'u_id': user_id, 'min': {'$exists': True}}).sort([('min', 1)]).limit(1)
    for rec in min_rec:
        return rec.get('min')


def save_tweet(json):
    db[msg_col].insert_one(json)


def down_recent(max_id, day=3):
    rec = db[rec_col].find_one({'max_id': max_id})
    if rec:
        last = datetime.strptime(rec['time'], time_format)
        if datetime.now().day - last.day < day:
            return True

    return False
