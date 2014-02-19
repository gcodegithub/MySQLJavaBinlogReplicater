#!/usr/bin/env python
# -*- coding: utf-8 -*- 


from bson import json_util
import time
import pymongo
import json
import os
from bson.timestamp import Timestamp


connection = pymongo.Connection('192.168.24.1', 27017)
db = connection['test']
tb = db.MY_TEST
#
filename = r'/Users/akui/temp/checkpoint.json'
check_ts = Timestamp(1392728000, 1)
if os.path.exists(filename):
   f = file(filename);
   check_ts = json.load(f)
   check_ts = Timestamp(int(check_ts['t']), int(check_ts['i']))
else:
    f = open(filename, 'w')
    f.write(json.dumps(check_ts, default=json_util.default))
    

#
while(1):
    after_ts = Timestamp(check_ts.time + 5, check_ts.inc)
    rows = tb.find({'dvs_server_ts':{'$gt':check_ts, '$lte':after_ts}}).sort([('dvs_server_ts', pymongo.ASCENDING)])
    if(rows.count() == 0):
        check_ts = after_ts
    for row in rows:
        check_ts = row['dvs_server_ts']
        print row
    if(rows.count() != 0):
        f = open(filename, 'w')
        f.write(json.dumps(check_ts, default=json_util.default))
        f.close()   
    print "-----------------"
    
    
    
    
