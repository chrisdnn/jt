import json
import pymongo
import data_DAO
from datetime import datetime
import argparse
import time
import requests
from bson.code import Code
import re

#first, setup mongodb connection
connection_string = "mongodb://cdunneg01"
connection = pymongo.MongoClient(connection_string)
dbs = connection.btctweets
data_DAO = data_DAO.Data_DAO(dbs)

# defaults
default_since_dt = '2017-01-01T00:00:00'
default_until_dt = time.strftime('%Y-%m-%dT%H:%M:%S')
dbcol_tweet_class = 'tweet_class_history'

#time intervals
tintervals = ['onemin', 'fivemin', 'thirtymin', 'hour', 'day']
tintervals_dt_measure = {'onemin':1, 'fivemin':5, 'thirtymin':30, 'hour':1, 'day':1}
tintervals_dt_scope = {'onemin':5, 'fivemin':5, 'thirtymin':5, 'hour':4, 'day':3}

## get history data for selected interval
def get_since_dt(lastr, tperiod, dtsince, dtuntil):
        lbtc_history = []
        dtsince_param = dtsince
        dbcollection_exists = False
        if lastr == 'y':
                #dt_field = '_id'
                dt_field = '_id'
                lmax_date = []
                #lmax_date = data_DAO.find_data_maxmin_value('btc_usd_class_'+tperiod,dt_field,'max')
                lmax_date = data_DAO.find_data_maxmin_value(dbcol_tweet_class,dt_field,'max')
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
def map_reduce_exec(tperiod, dtsince, dtuntil, dbcol_exists):
        map = Code('''function() {
                     var u_screen_name = this.user.screen_name; var u_followers_count = this.user.followers_count; var u_listed_count = this.user.listed_count; 
                     var t_id = this.id; var t_text = this.text; var t_favorite_count = this.favorite_count;

                     var ret_id = ""; var ret_count = 0; var ret_fav_count = 0; var ret_text = "";
                     if(this.hasOwnProperty("retweeted_status")){ 
                        ret_id = this.retweeted_status.id; 
                        ret_count = this.retweeted_status.retweet_count ; 
                        var ret_fav_count = this.retweeted_status.favorite_count; 
                        var ret_text = this.retweeted_status.text 
                     };

                     var dt_y = parseInt(this.created_at_dt.getFullYear()); var dt_m = parseInt(this.created_at_dt.getMonth()); var dt_d = parseInt(this.created_at_dt.getDate()); 
                     var dt_H = parseInt(this.created_at_dt.getHours()) + parseInt(this.created_at_dt.getTimezoneOffset()/60); var dt_M = parseInt((this.created_at_dt).getMinutes()); var dt_S = parseInt((this.created_at_dt).getSeconds());
                     var int_onemin = Math.floor(dt_M); var int_fivemin = Math.floor(dt_M/factor_fivemin)*factor_fivemin; var int_thirtymin = Math.floor(dt_M/factor_thirtymin)*factor_thirtymin;
                     var int_hour = dt_H; var int_day = dt_d;

                     var onemin = {interval: int_onemin, class_incr:"", class_inter:""}; 
                     var fivemin = {interval: int_fivemin, class_incr:"", class_inter:""};
                     var thirtymin = {interval: int_thirtymin, class_incr:"", class_inter:""}; 
                     var hour = {interval: int_hour, class_incr:"", class_inter:""}; 
                     var day = {interval: int_day, class_incr:"", class_inter:""};
                     var user = { screen_name: u_screen_name, followers_count: u_followers_count, listed_count: u_listed_count};
                     var tweet = { id: t_id, text: this.text ,favorite_count: t_favorite_count, retweet_id: ret_id, retweet_count: ret_count, retweet_favorite_count: ret_fav_count, retweet_text: ret_text}
                     var value_m = {
                                     tweet, user, onemin, fivemin, thirtymin, hour, day
                                   };
                     emit(this.created_at_dt, value_m );
                   }''')
        reduce = Code('function(key, values){}')
        sreturn_full = True
        sscope = {'factor_fivemin':tintervals_dt_measure['fivemin'], 'factor_thirtymin':tintervals_dt_measure['thirtymin']}
        squery = {'created_at_dt':{'$gte':dtsince, '$lte': dtuntil}}
        sout = {'inline':1}
        #sout_collection = {'reduce': 'btc_usd_class'+tperiod}
        result_null = data_DAO.map_reduce_to_collection('tweets', map, reduce, dbcol_tweet_class, False, squery, sscope, dbcol_exists)

                                                                        
def classify_tweets_since(tperiod, lbtc_class, dtsince, dtuntil, dbcol_exist): #
        #ltweet_data = data_DAO.find_data_by_daterange('tweets','created_at_dt',dtsince,dtuntil)
        if tintervals_dt_scope[tperiod] == 5:
                print('5 interval : ', tperiod, ' date_scope: ', tintervals_dt_scope[tperiod])
        elif tintervals_dt_scope[tperiod] == 4:
                print('4 interval : ', tperiod, ' date_scope: ', tintervals_dt_scope[tperiod])
        elif tintervals_dt_scope[tperiod] == 3:
                print('3 interval : ', tperiod, ' date_scope: ', tintervals_dt_scope[tperiod])
        
## classifiy btc/usd evolution for specific evolution history set
def get_btc_class_since(tperiod, dtsince, dtuntil, dbcol_exist):
        lbtc_class = data_DAO.find_data_by_daterange('btc_usd_class_'+tperiod,'_id',dtsince,dtuntil)
        return lbtc_class
                
## classifiy btc/usd evolution for specific evolution history set
def classify_evolution(tperiod, dtsince, dtuntil, dbcol_exist):
        #map_reduce_exec(tperiod, dtsince, dtuntil, dbcol_exist)
        ltweet_history = data_DAO.find_data_by_daterange('tweets','created_at_dt', dtsince,dtuntil) # datetime(2017, 9, 1, 7, 9), datetime(2017, 9, 1, 12, 33) ) #dtsince,dtuntil)  datetime(2017,9,1,9,10),datetime(2017,9,1,12,37) ) #'tweets','created_at_dt', datetime(2017, 9, 1, 7, 9), datetime(2017, 9, 1, 12, 16) )#dtsince,dtuntil)
        ltweet_extended = []
        print('extracted tweets, dtsince: ', dtsince, ' - dtuntil: ',dtuntil)
        for elemp in ltweet_history:
                json_data = {}
                json_data['tweet'] = elemp['text']
                json_data['created_at_dt'] = elemp['created_at_dt']
                ## datetime parsed per element
                var_day = int(re.sub(r'[^\d-]+', '', elemp['created_at_dt'].strftime('%d')))
                var_hr = int(re.sub(r'[^\d-]+', '', elemp['created_at_dt'].strftime('%H')))
                var_min = int(re.sub(r'[^\d-]+', '', elemp['created_at_dt'].strftime('%M')))
                var_sec = int(re.sub(r'[^\d-]+', '', elemp['created_at_dt'].strftime('%S')))
                json_data['dt_yyyy'] = elemp['created_at_dt'].strftime('%Y')
                json_data['dt_mm'] = elemp['created_at_dt'].strftime('%m')
                json_data['dt_dd'] = elemp['created_at_dt'].strftime('%d')
                json_data['dt_Hhr'] = elemp['created_at_dt'].strftime('%H')
                json_data['dt_Min'] = elemp['created_at_dt'].strftime('%M')
                json_data['dt_Sec'] = elemp['created_at_dt'].strftime('%S')
                ## fields for quantification per interval
                json_data['dt_onemin'] = var_min
                json_data['dt_fivemin'] = (var_min/tintervals_dt_measure['fivemin'])*tintervals_dt_measure['fivemin']
                json_data['dt_thirtymin'] = (var_min/tintervals_dt_measure['thirtymin'])*tintervals_dt_measure['thirtymin']
                json_data['dt_hour'] = var_hr
                json_data['dt_day'] = var_day
                ## fields for classes
                #print(json_data)
        #data_DAO.insert_data_no_duplicates(dbcol_tweet_class, tdata, isprint, printfield):
        #classify_tweets_since(tperiod, [], dtsince, dtuntil, dbcol_exist)
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
        
