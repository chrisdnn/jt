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
def get_since_dt(lastr, tperiod, dtsince, dtuntil):
        lbtc_history = []
        dtsince_param = dtsince
        dbcollection_exists = False
        if lastr == 'y':
                dt_field = '_id'
                print('YES.......')
                lmax_date = []
                lmax_date = btc_histo_DAO.find_btc_data_maxmin_value('btc_usd_class_'+tperiod,dt_field,'max')
#                find_btc_data_maxmin_value('btc_usd_class_'+tperiod, '_id','max')
                print('??? ', tperiod, '  lmax_date: ', lmax_date)
                if len(lmax_date) > 0:
                        dtsince_param = lmax_date[0][dt_field]
                        dbcollection_exists = True
                        print('////Update dtsince: ',dtsince_param)
        json_data = {}
        json_data['dbcol_exist'] = dbcollection_exists
        json_data['dt_since'] = dtsince_param
        #        return dtsince_param
        return json_data

## get history data for selected interval
def get_history(lastr, tperiod, dtsince, dtuntil):
        lbtc_history = []
        dtsince_param = dtsince
        if lastr == 'y':
                dt_field = '_id'
                print('YES.......')
                lmax_date = []
                lmax_date = btc_histo_DAO.find_btc_data_maxmin_value('btc_usd_class_'+tperiod,dt_field,'max')
#                find_btc_data_maxmin_value('btc_usd_class_'+tperiod, '_id','max')
#                print('??? ', tperiod, '  lmax_date: ', lmax_date)
                if len(lmax_date) > 0:
                        dtsince_param = lmax_date[0][dt_field]
                        print('////Update dtsince: ',dtsince_param)
        lbtc_history = btc_histo_DAO.find_btc_data_by_daterange('btc_usd_history_'+tperiod,'ISOdt', dtsince_param, dtuntil)
        print('........collection: ', 'btc_usd_history_'+tperiod, 'dts: ', dtsince_param, ' dtu: ', dtuntil)
        return lbtc_history

def map_reduce_exec_original(tperiod, dtsince, dtuntil, dbcol_exists):
        map = Code('function() {' \
                   '  difs = prev - this.C;' \
                   '  var perc_dif = 0; if (prev != 0) {perc_dif = difs/prev};' \
                   '  emit(this.ISOdt, perc_dif );' \
                   '  prev = this.C;' \
                   '}')
        print(map)
        reduce = Code('function(key, values) {' \
                      '  return Array.sum(values);' \
                      '}')
        print(reduce)
        sreturn_full = True
        sscope = {'difs':0, 'prev':0}
        squery = {'ISOdt':{'$gte':dtsince, '$lte': dtuntil}}
        sout = {'inline':1}
        #sout_collection = {'reduce': 'btc_usd_class'+tperiod}
        #        result_null = btc_histo_DAO.map_reduce_to_collection('btc_usd_history_'+tperiod, map, reduce, 'btc_usd_class_'+tperiod, False, squery, sscope, sout_collection)
        result_null = btc_histo_DAO.map_reduce_to_collection('btc_usd_history_'+tperiod, map, reduce, 'btc_usd_class_'+tperiod, False, squery, sscope, dbcol_exists)
        result = btc_histo_DAO.map_reduce_inline('btc_usd_history_'+tperiod,map, reduce, sreturn_full, squery, sscope, sout)
        for doc in result['results']:
                print doc
                
def map_reduce_exec(tperiod, dtsince, dtuntil, dbcol_exists):
        map = Code('function() {' \
                   '  difs = prev - this.C;' \
                   '  var perc_dif = 0; var val_dif = 0; if (prev != 0) {perc_dif = difs/prev; val_dif = difs;};' \
                   '  var value_m = { perc_d : perc_dif, val_d : val_dif};' \
                   '  emit(this.ISOdt, value_m );' \
                   '  prev = this.C;' \
                   '}')
        print(map)
        reduce = Code('function(key, countObjVals) {' \
                      '  reducedVal = { perc_d: 0, val_d: 0 };' \
                      '  for (var idx = 0; idx < countObjVals.length; idx++) {' \
                      '    reducedVal.perc_d += countObjVals[idx].perc_d; ' \
                      '    reducedVal.val_d += countObjVals[idx].val_d; }' \
                      '  return reducedVal;' \
                      '}')
        print(reduce)
        sreturn_full = True
        sscope = {'difs':0, 'prev':0}
        squery = {'ISOdt':{'$gte':dtsince, '$lte': dtuntil}}
        sout = {'inline':1}
        #sout_collection = {'reduce': 'btc_usd_class'+tperiod}
        #        result_null = btc_histo_DAO.map_reduce_to_collection('btc_usd_history_'+tperiod, map, reduce, 'btc_usd_class_'+tperiod, False, squery, sscope, sout_collection)
        result_null = btc_histo_DAO.map_reduce_to_collection('btc_usd_history_'+tperiod, map, reduce, 'btc_usd_class_'+tperiod, False, squery, sscope, dbcol_exists)
        result = btc_histo_DAO.map_reduce_inline('btc_usd_history_'+tperiod,map, reduce, sreturn_full, squery, sscope, sout)
        for doc in result['results']:
                print doc
                
## classifiy btc/usd evolution for specific evolution history set
def classify_evolution(tperiod, dtsince, dtuntil, dbcol_exist):
        max_date = datetime(2017,1,1,0,0,0)
        lmax_date = []
        if len(lmax_date) > 0:
                max_date = lmax_date[0]['ISOdt']
        map_reduce_exec(tperiod, dtsince, dtuntil, dbcol_exist)

# trigger each interval actions                        
def trigger_interval(args):
        dtsince = datetime.strptime(args['since'], '%Y-%m-%dT%H:%M:%S')
        dtuntil = datetime.strptime(args['until'], '%Y-%m-%dT%H:%M:%S')
        dt_since_data = {}
        dt_since_data['dbcol_exist'] = False
        dt_since_data['dt_since'] = dtsince
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
                #dtsince_updated = get_since_dt(args['last'], args['interval'], dtsince, dtuntil)
                dtsince_data_json = get_since_dt(args['last'], args['interval'], dtsince, dtuntil)
                dt_since_data['dbcol_exist'] = dtsince_data_json['dbcol_exist']
                dt_since_data['dt_since'] = dtsince_data_json['dt_since']
        #classify_evolution(args['interval'], dtsince_updated, dtuntil)
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
        
