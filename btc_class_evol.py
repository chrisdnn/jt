from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterRestPager
import json
import pymongo
import m_btc_historyDAO
from datetime import datetime
import argparse
import time
import requests
from bson.code import Code

#first, setup mongodb connection
connection_string = "mongodb://cdunneg01"
connection = pymongo.MongoClient(connection_string)
dbs = connection.btctweets
btc_histo_DAO = m_btc_historyDAO.M_btc_historyDAO(dbs)

# defaults
default_since_dt = '2017-01-01T00:00:00'
default_until_dt = time.strftime('%Y-%m-%dT%H:%M:%S')

#time intervals
tintervals = ['onemin', 'fivemin', 'thirtymin', 'hour', 'day']

## get history data for selected interval
def get_history(lastr, tperiod, dtsince, dtuntil):
        lbtc_history = []
        dtsince_param = dtsince
        if lastr == 'y':
                lmax_date = []
                lmaxdate = btc_histo_DAO.find_btc_data_maxmin_value('btc_usd_h_class_'+tperiod, 'ISOdt','max')
                if len(lmax_date) > 0:
                        dtsince_param = lmax_date[0]['ISOdt']
        lbtc_history = btc_histo_DAO.find_btc_data_by_daterange('btc_usd_history_'+tperiod,'ISOdt', dtsince_param, dtuntil)
        print('collection: ', 'btc_usd_history_'+tperiod, 'dts: ', dtsince_param, ' dtu: ', dtuntil)
        return lbtc_history

def map_reduce_exec(tperiod, dtsince, dtuntil):
        map = Code('function() {' \
                   '  difs = prev - this.C;' \
                   '  emit(this.ISOdt, difs/prev);' \
                   '  prev = this.C;' \
                   '}')
        print(map)
        reduce = Code('function(key, values) {' \
                      '  return Array.sum(values);' \
                      '}')
        print(reduce)
        sreturn_full = True
        sscope = {'difs':0, 'prev':0}
        #squery = {'ISOdt':{'$gte':ISODate('2017-08-28T21:33:00'), '$lte':ISODate('2017-08-28T21:40:00')}}
        #squery = {'ISOdt':{'$gte':datetime(2017,8,28,21,33,00), '$lte': datetime(2017,8,28,21,40,00)}}
        squery = {'ISOdt':{'$gte':dtsince, '$lte': dtuntil}}
        sout = {'inline':1}
        result_null = btc_histo_DAO.map_reduce_to_collection('btc_usd_history_'+tperiod, map, reduce, 'btc_usd_class_'+tperiod, False, squery, sscope)
        result = btc_histo_DAO.map_reduce_inline('btc_usd_history_'+tperiod,map, reduce, sreturn_full, squery, sscope, sout)
        for doc in result['results']:
                print doc

## classifiy btc/usd evolution for specific evolution history set
def classify_evolution(tperiod, dtsince, dtuntil):
        max_date = datetime(2017,1,1,0,0,0)
        lmax_date = []
        if len(lmax_date) > 0:
                max_date = lmax_date[0]['ISOdt']
        '''
        for elem in raw_data:
                date_elem = datetime.strptime(elem['T'], '%Y-%m-%dT%H:%M:%S')
                if date_elem >= max_date:
                        elem['ISOdt'] = date_elem
                        #btc_histo_DAO.insert_btc_data_no_duplicates('btc_usd_history',elem,0,'O')
                        #print(elem)
        '''
        map_reduce_exec(tperiod, dtsince, dtuntil)

# trigger each interval actions                        
def trigger_interval(args):
        dtsince = datetime.strptime(args['since'], '%Y-%m-%dT%H:%M:%S')
        dtuntil = datetime.strptime(args['until'], '%Y-%m-%dT%H:%M:%S')     
        '''
        datetime(time.strftime(vargs['since'],'%Y'),time.strftime(vargs['since'],'%m'),time.strftime(vargs['since'],'%d'),
                           time.strftime(vargs['since'],'%H'),time.strftime(vargs['since'],'%M'),time.strftime(vargs['since'],'%S'))
        datetime(time.strftime(vargs['until'],'%Y'),time.strftime(vargs['until'],'%m'),time.strftime(vargs['until'],'%d'),
        time.strftime(vargs['until'],'%H'),time.strftime(vargs['until'],'%M'),time.strftime(vargs['until'],'%S')))
        '''
        if args == 'all':
                print('Request to updated all intervals...')#insert_new_item(args)
                '''
                for tinter in tintervals:
                        insert_new_items(tinter)
                        print('Finished interval: ' + tinter)
                '''
        else:
                print('interval: ', args) #insert_new_items(args)
                print('SINCE: ', dtsince, '   UNTIL: ', dtuntil)
                l_interval_h = get_history(args['last'], args['interval'], dtsince, dtuntil)
                for e in l_interval_h:
                        print('R: ', e)
        classify_evolution(args['interval'], dtsince, dtuntil)
                
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
        
