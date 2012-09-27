import urllib, urllib2
import json
import base64
import time
from twitter_config import config

def twitter(queries):
    url = 'https://stream.twitter.com/1/statuses/filter.json'
    queries = [q.lower() for q in queries]
    quoted_queries = [urllib.quote(q) for q in queries]
    query_post = 'track=' + ",".join(quoted_queries)
    request = urllib2.Request(url, query_post)
    auth = base64.b64encode('%s:%s' % (config.TWITTER['user'], config.TWITTER['password']))
    request.add_header('Authorization', "basic %s" % auth)
    for item in urllib2.urlopen(request):
        try:
            item = json.loads(item)
        except json.JSONDecodeError, e: #for whatever reason json reading twitters json sometimes raises this
            print(e)
            continue

        if 'text' in item and 'user' in item:
            text = item['text'].lower()
            user_id = item['user']['id_str']
            yield user_id, text


fruits = ['apple','banana','carrot']

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

def follow(data, target): 
    for datum in data: target.send(datum)

@coroutine
def printer(name=''):
    while True:
         line = (yield)
         print name, line

if __name__ == '__main__':
    follow(twitter(fruits), printer())
