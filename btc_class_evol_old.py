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
dbcollection_classname = 'bittrex_btcusd_class'

#time intervals
tintervals = ['onemin', 'fivemin', 'fifteenmin', 'thirtymin', 'hour', 'day']

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
                print('??? ', tperiod, '  lmax_date: ', lmax_date)
                if len(lmax_date) > 0:
                        dtsince_param = lmax_date[0][dt_field]
                        dbcollection_exists = True
                        print('////Update dtsince: ',dtsince_param)
        json_data = {}
        json_data['dbcol_exist'] = dbcollection_exists
        json_data['dt_since'] = dtsince_param
        return json_data


## Map reduces the operation of classifying each interval, based on the differences (absolute and percentage) among incremental records, as well as within the interval itself
## Incremental: considers difference between Close position (fields with prefix "incr_")
## Within record: consders difference between Open and Close positions (fields with prefix "int_")
def map_reduce_exec(tperiod, dtsince, dtuntil, dbcol_exists):
        map = Code('function() {' \
                   '  incr_difs = incr_prev - this.C; var incr_perc_dif = 0; var incr_val_dif = 0; var incr_class_diff = "neutral"; ' \
                   '  int_difs = this.O - this.C; var int_perc_dif = 0; var int_val_dif = 0; var int_class_diff = "neutral"; ' \
                   '  if(incr_prev != 0) { incr_perc_dif = incr_difs/incr_prev; incr_val_dif = incr_difs; }; if(incr_difs < 0){ incr_class_diff = "loss"} else if(incr_difs > 0){ incr_class_diff = "gain"}; ' \
                   '  if(this.O != 0) { int_perc_dif = int_difs/this.O; int_val_dif = int_difs; }; if(int_difs < 0){ int_class_diff = "loss"} else if(int_difs > 0){ int_class_diff = "gain"}; ' \
                   '  var value_m = { incr_perc_d : incr_perc_dif, incr_val_d : incr_val_dif, incr_class_d: incr_class_diff, int_perc_d : int_perc_dif, int_val_d : int_val_dif, int_class_d: int_class_diff};' \
                   '  emit(this.ISOdt, value_m );' \
                   '  incr_prev = this.C;' \
                   '}')
        reduce = Code('function(key, countObjVals) {' \
                      '  reducedVal = { perc_d: 0, val_d: 0 };' \
                      '  for (var idx = 0; idx < countObjVals.length; idx++) {' \
                      '    reducedVal.perc_d += countObjVals[idx].perc_d; ' \
                      '    reducedVal.val_d += countObjVals[idx].val_d; }' \
                      '  return reducedVal;' \
                      '}')
        sreturn_full = True
        sscope = {'incr_difs':0, 'incr_prev':0}
        squery = {'ISOdt':{'$gte':dtsince, '$lte': dtuntil}}
        sout = {'inline':1}
        #sout_collection = {'reduce': 'btc_usd_class'+tperiod}
        result_null = btc_histo_DAO.map_reduce_to_collection('btc_usd_history_'+tperiod, map, reduce, 'btc_usd_class_'+tperiod, False, squery, sscope, dbcol_exists)
        ## uncommment the following line for inline map_reduce
        #result = btc_histo_DAO.map_reduce_inline('btc_usd_history_'+tperiod,map, reduce, sreturn_full, squery, sscope, sout)

                
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
        if args['interval'] == 'all':
                print('Request to updated all intervals...')#insert_new_item(args)
                for tinter in tintervals:
                        dtsince_data_json = get_since_dt(args['last'], tinter, dtsince, dtuntil)
                        dt_since_data['dbcol_exist'] = dtsince_data_json['dbcol_exist']
                        dt_since_data['dt_since'] = dtsince_data_json['dt_since']
                        classify_evolution(tinter, dt_since_data['dt_since'], dtuntil, dt_since_data['dbcol_exist'])
                        print('Finished interval: ' + tinter)
        else:
                print('SINCE: ', dtsince, '   UNTIL: ', dtuntil)
                dtsince_data_json = get_since_dt(args['last'], args['interval'], dtsince, dtuntil)
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
        
