#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pymongo import Connection
import time, datetime

connection = Connection('10.12.34.51', 30000)
db = connection['Hawaii']

# 时间记录器
def func_time(func):
        def _wrapper(*args, **kwargs):
                start = time.time()
                func(*args, **kwargs)
                print func.__name__, 'function time last:', (time.time() - start)
                print "insert row num:", args[0]
                print "rows per sec", args[0] / (time.time() - start)
        return _wrapper

@func_time
def insert(num):
        posts = db.userinfo
        for col in range(50):
                map={}
                map[str(col)]=time.time()
        for x in range(num):
                post = {
                        "author": str(x) + "Mike",
                        "text": "My first blog post!",
                        "tags": ["mongodb", "python", "pymongo"],
                        "date": datetime.datetime.utcnow()}
                post.update(map)
                # print post
                # posts.insert(post)
                posts.insert(post, safe=True)

if __name__ == "__main__":
	# 设定循环
    num = 100000
    insert(num)
