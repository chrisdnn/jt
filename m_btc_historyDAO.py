from bson import ObjectId
import string
import json
import re
from datetime import datetime
import pymongo

class M_btc_historyDAO(object):
#initialize with database and set mongodb collection to use
	def __init__(self, db):
		self.db = db
		#self.db.cln = s_collection

        #insertion of btc_data VALIDATING no duplicates
        def insert_btc_data_no_duplicates(self, dbcollection, tdata, isprint, printfield):
                 self.db[dbcollection].update(tdata,tdata,upsert=True)
                 if isprint == 1:
                         print('DAO_text:' + json.dumps(tdata[printfield]))
                         
        #insertion of btc_data
        def insert_btc_data(self, dbcollection, tdata, isprint, printfield):
                 self.db[dbcollection].insert(tdata)
                 if isprint == 1:
                         print('DAO_text:' + json.dumps(tdata[printfield]))

        #update of btc_data
        def update_btc_data(self, dbcollection, objid, updatefield, updatevalue):
                self.db[dbcollection].update_one({'_id': objid},{
                        '$set': {
                                updatefield: updatevalue
                        }
                }, upsert=False)
                
        #finding of btc_data, returns all btc_data in the collection
        def find_btc_data(self, dbcollection):
		l = []
                for each_item in self.db[dbcollection].find({}):
                        l.append(each_item) #{'_id':each_name['_id'],'text':each_name['text']})
                        #print('one found')
                return l

                #finding of btc_data, returns all btc_data in the collection
        def find_btc_data_maxmin_value(self, dbcollection, lookupfield, maxmin):
		l = []
                maxminv = pymongo.ASCENDING
                if maxmin == 'max':
                        maxminv = pymongo.DESCENDING
                for each_item in self.db[dbcollection].find({}).sort(lookupfield,maxminv).limit(1):
                        l.append(each_item) #{'_id':each_name['_id'],'text':each_name['text']})
                        #print('one found')
                return l
        
#finding of btc_data, returns list of JSON objects, including all fields
	def find_btc_data_by_objectid(self, dbcollection, lookupfield, objid):
		l = []
                query = {lookupfield: objid}
                cursor = self.db[dbcollection].find(query)
                for each_item in cursor:
                        l.append(each_item)
                return l

#finding of btc_data, returns list of JSON objects, including all fields
	def find_btc_data_by_objectid_str(self, dbcollection, lookupfield, objid):
		l = []
                query = {lookupfield: ObjectId(objid)}
                cursor = self.db[dbcollection].find(query)
                for each_item in cursor:
                        l.append(each_item)
                return l
#finding btc_data by range of dates
        def find_btc_data_by_daterange(self, dbcollection, datefield, datestart, dateend):
                l = []
                query = { datefield: {
                        '$gte': datestart,#ISODate(datestart),
                        '$lt': dateend#ISODate(dateend)
                }}
                cursor = self.db[dbcollection].find(query)
                for each_item in cursor:
                        l.append(each_item) #{'_id':each_name['_id'],'text':each_name['text']})
                return l
        
#finding of btc_data with certain REGEX criteria
# returns list of JSON objects (btc_data) with all fields
	def find_btc_data_regx_l(self, dbcollection, lookup_field, list_of_regex):
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

        
#finding of btc_data with certain NOT REGEX criteria
                ##negation: ^((?!badword).)*$
	def find_btc_data_regx_not(self, regexval):
                regx = re.compile("/.*"+regexval+".*/", re.IGNORECASE)
		l = []
                for each_name in self.db.cln.find({u'text':{'$not':regx}},{u'text':1}):
                        l.append({'_id':each_name['_id'],'text':each_name['text']})
                return l
## returns
## json object{'results':[{'_id','value'}],'timeMillis':N, counts:{'input':N,'emit':N,'reduce':N,'output':N}, 'ok':N}
        def map_reduce_inline(self, dbcollection, smap, sreduce, sreturn, squery, sscope, sout):
                #l = []
                '''
                for each_item in self.db[dbcollection].inline_map_reduce(smap, sreduce, full_response=sreturn, query=squery, scope=sscope, out=sout):
                        l.append(each_item)
                        print('l...')
                '''
                l = self.db[dbcollection].inline_map_reduce(smap, sreduce, full_response=sreturn, query=squery, scope=sscope, out=sout)
                return l

## writes collection
## json object{'results':[{'_id','value'}],'timeMillis':N, counts:{'input':N,'emit':N,'reduce':N,'output':N}, 'ok':N}
        def map_reduce_to_collection(self, dbcollectionfrom, smap, sreduce, dbcollectionto, sreturn, squery, sscope): #, sout):
                #l = []
                '''
                for each_item in self.db[dbcollection].inline_map_reduce(smap, sreduce, full_response=sreturn, query=squery, scope=sscope, out=sout):
                        l.append(each_item)
                        print('l...')
                '''
                l = self.db[dbcollectionfrom].map_reduce(smap, sreduce, dbcollectionto, full_response=sreturn, query=squery, scope=sscope)#, out=sout)
                return l
