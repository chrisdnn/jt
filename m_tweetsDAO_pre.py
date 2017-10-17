from bson import ObjectId
import string
import json
import re
from datetime import datetime

class M_tweetsDAO_pre(object):
#initialize with database and set mongodb collection to use
	def __init__(self, db):
		self.db = db
		#self.db.cln = s_collection


        #insertion of tweets
        def insert_tweet(self, dbcollection, tdata, isprint, printfield):
                 self.db[dbcollection].insert(tdata)
                 if isprint == 1:
                         print('DAO_text:' + json.dumps(tdata[printfield]))

        #update of tweets
        def update_tweet(self, dbcollection, objid, updatefield, updatevalue):
                self.db[dbcollection].update_one({'_id': objid},{
                        '$set': {
                                updatefield: updatevalue
                        }
                }, upsert=False)
                
        #finding of tweets, returns all tweets in the collection
        def find_tweets(self, dbcollection):
		l = []
                for each_item in self.db[dbcollection].find({}):
                        l.append(each_item) #{'_id':each_name['_id'],'text':each_name['text']})
                        #print('one found')
                return l

#finding of tweets, returns list of JSON objects, including all fields
	def find_tweets_by_objectid(self, dbcollection, lookupfield, objid):
		l = []
                query = {lookupfield: objid}
                cursor = self.db[dbcollection].find(query)
                for each_item in cursor:
                        l.append(each_item)
                return l

#finding of tweets, returns list of JSON objects, including all fields
	def find_tweets_by_objectid_str(self, dbcollection, lookupfield, objid):
		l = []
                query = {lookupfield: ObjectId(objid)}
                cursor = self.db[dbcollection].find(query)
                for each_item in cursor:
                        l.append(each_item)
                return l
#finding tweets by range of dates
        def find_tweets_by_daterange(self, dbcollection, datefield, datestart, dateend):
                l = []
                query = { datefield: {
                        '$gte': datestart,#ISODate(datestart),
                        '$lt': dateend#ISODate(dateend)
                }}
                cursor = self.db[dbcollection].find(query)
                for each_item in cursor:
                        l.append(each_item) #{'_id':each_name['_id'],'text':each_name['text']})
                return l
        
#finding of tweets with certain REGEX criteria
# returns list of JSON objects (tweets) with all fields
	def find_tweets_regx_l(self, dbcollection, lookup_field, list_of_regex):
                lfind = []
                if len(list_of_regex) == 0:
                        return None
                str_regex = '|'.join(list_of_regex)
		l = []
                query = {lookup_field:{'$regex': str_regex, '$options': '-i'}}
                cursor = self.db[dbcollection].find(query)
                for each_item in cursor:
                        l.append(each_item) #{'_id':each_name['_id'],'text':each_name['text']})
                return l

        
#finding of tweets with certain NOT REGEX criteria
                ##negation: ^((?!badword).)*$
	def find_tweets_regx_not(self, regexval):
                regx = re.compile("/.*"+regexval+".*/", re.IGNORECASE)
		l = []
                for each_name in self.db.cln.find({u'text':{'$not':regx}},{u'text':1}):
                        l.append({'_id':each_name['_id'],'text':each_name['text']})
                return l
