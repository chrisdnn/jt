import json
import pymongo
import data_DAO
from datetime import datetime
import argparse
import time
import requests
from bson.code import Code
import re
import pprint

#first, setup mongodb connection
connection_string = "mongodb://cdunneg01"
connection = pymongo.MongoClient(connection_string)
dbs = connection.btctweets
data_DAO = data_DAO.Data_DAO(dbs)

# defaults
default_since_dt = '2017-01-01T00:00:00'
default_until_dt = time.strftime('%Y-%m-%dT%H:%M:%S')
dbcol_tweet_class_from = 'tweets_raw'
dbcol_tweet_class_dst = 'tweet_class'
dbcol_exchange_class_from = 'bittrex_btcusd_class_test'

#time intervals
tintervals = ['onemin', 'fivemin', 'fifteenmin', 'thirtymin', 'hour', 'day']
tintervals_dt_measure = {'onemin':1, 'fivemin':5, 'fifteenmin':15, 'thirtymin':30, 'hour':1, 'day':1}
tintervals_dt_scope = {'onemin':5, 'fivemin':5, 'fifteenmin':5, 'thirtymin':5, 'hour':4, 'day':3}

## get history data for selected interval
def get_since_dt(lastr, tperiod, dtsince, dtuntil):
        lbtc_history = []
        dtsince_param = dtsince
        dbcollection_exists = False
        if lastr == 'y':
                #dt_field = '_id'
                dt_field = 'tweet_class._id.created_at'
                lmax_date = []
                squery = {}
                lmax_date = data_DAO.find_data_maxmin_value(dbcol_tweet_class_dst, squery, dt_field, 'max')
                print('??? ', tperiod, '  lmax_date: ', lmax_date)
                if len(lmax_date) > 0:
                        dtsince_param = lmax_date[0][dt_field]
                        dbcollection_exists = True
                        print('////Update dtsince: ',dtsince_param)
        json_data = {}
        json_data['dbcol_exist'] = dbcollection_exists
        json_data['dt_since'] = dtsince_param
        return json_data

## Map reduces the operation of calculating the bucket where the tweets land, regarding intervals
def lookup_exec(tperiod, dtsince, dtuntil):
        smatch = {'$match': {'$and': [ {'dt_parsed.'+tperiod:{ '$gte': dtsince, '$lte': dtuntil}} ] }  }
        #print('match', smatch)
        pipeline = [
                smatch,
                {'$project':
                 {
                         'dt_parsed':1,
                         'user.screen_name':1,
                         'user.followers_count':1,
                         'user.listed_count':1,
                         'text':1,
                         'favorite_count':1,
                         'id':1
                 }
                },
                {'$lookup':
                 {
                         'from': dbcol_exchange_class_from,
                         'localField': 'dt_parsed.'+tperiod,
                         'foreignField': '_id.created_at',
                         'as':'tweet_class'
                 }
                },
                {'$project':
                 {
                        'dt_parsed':1,
                        'user.screen_name':1,
                        'user.followers_count':1,
                        'user.listed_count':1,
                        'text':1,
                        'favorite_count':1,
                        'id':1,
                         'tweet_class': {
                                 '$filter': {
                                         'input': '$tweet_class',
                                         'as': 'tweet_c',
                                         'cond': { '$eq': ['$$tweet_c._id.interval',tperiod] }
                                 }
                         }
                 }
                }

        ]
        #print(pipeline)
        #print('db.from:',dbcol_tweet_class_from)
        lookup_agg = data_DAO.aggregate_pipeline(dbcol_tweet_class_from, pipeline)
        pprint.pprint(list(lookup_agg))


                
## classifiy btc/usd evolution for specific evolution history set
def classify_evolution(tperiod, dtsince, dtuntil, dbcol_exist):
        print('extracted tweets, dtsince: ', dtsince, ' - dtuntil: ',dtuntil)
        lookup_exec(tperiod, dtsince, dtuntil)
        
# trigger each interval actions                        
def trigger_interval(args):
        dtsince = datetime.strptime(args['since'], '%Y-%m-%dT%H:%M:%S')
        dtuntil = datetime.strptime(args['until'], '%Y-%m-%dT%H:%M:%S')
        dt_since_data = {}
        dt_since_data['dbcol_exist'] = False
        dt_since_data['dt_since'] = dtsince
        if args['interval'] == 'all':
                print('Request to updated all intervals...')
                for tinter in tintervals:
                        dtsince_data_json = get_since_dt(args['last'], tinter, dtsince, dtuntil)
                        dt_since_data['dbcol_exist'] = dtsince_data_json['dbcol_exist']
                        dt_since_data['dt_since'] = dtsince_data_json['dt_since']
                        classify_evolution(tinter, dt_since_data['dt_since'], dtuntil, dt_since_data['dbcol_exist'])
                        print('Finished interval: ' + tinter)
        else:
                print('SINCE: ', dtsince, '   UNTIL: ', dtuntil)
                dtsince_data_json = get_since_dt(args['last'], args['interval'], dtsince, dtuntil)
                print('DT since: ', dtsince_data_json['dt_since'])
                dt_since_data['dbcol_exist'] = dtsince_data_json['dbcol_exist']
                dt_since_data['dt_since'] = dtsince_data_json['dt_since']
                classify_evolution(args['interval'], dt_since_data['dt_since'], dtuntil, dt_since_data['dbcol_exist'])
                
def parse_args():
        parser = argparse.ArgumentParser(description='Classify BTC based on its variance')
        parser.add_argument('-i','--interval', choices=list(set(['all']).union(set(tintervals))), required=True)
        parser.add_argument('-l','--last', nargs='?', choices=['y','n'], required=True) #if "y" calculates from the max(date) it has calculated. If "n" overwrites past calculations, if any
        parser.add_argument('-s','--since', nargs='?', default=default_since_dt, metavar='YYYY-MM-DDThh:mm:ss') 
        parser.add_argument('-u','--until', nargs='?', default=default_until_dt, metavar='YYYY-MM-DDThh:mm:ss') 
        args = parser.parse_args()
        v_args = vars(args)
        print(args)
        return v_args

def main(args):
        trigger_interval(args)
                
if __name__ == "__main__":
        v_args = parse_args()
        print(time.strftime('%Y-%m-%dT%H:%M:%S'))
        main(v_args)
        
