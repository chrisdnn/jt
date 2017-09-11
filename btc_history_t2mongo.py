from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterRestPager
import json
import pymongo
import m_btc_historyDAO
from datetime import datetime
import argparse
import time
import requests


#first, setup mongodb connection
connection_string = "mongodb://cdunneg01"
connection = pymongo.MongoClient(connection_string)
dbs = connection.btctweets
btc_histo_DAO = m_btc_historyDAO.M_btc_historyDAO(dbs)

#time intervals
tintervals = ['onemin', 'fivemin', 'thirtymin', 'hour', 'day']

def insert_new_items(tperiod):
#        r = requests.get('https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=USDT-BTC&tickInterval=oneMin')
        r = requests.get('https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=USDT-BTC&tickInterval=' + tperiod)
        if r.json()['success'] == True:
                raw_data = r.json()['result']
                lmax_dat = []
                lmax_date = btc_histo_DAO.find_btc_data_maxmin_value('btc_usd_history_' + tperiod,'T','max')
                max_date = datetime(2017,1,1,0,0,0)
                if len(lmax_date) > 0:
                        max_date = lmax_date[0]['ISOdt']
                for elem in raw_data:
                        date_elem = datetime.strptime(elem['T'], '%Y-%m-%dT%H:%M:%S')
                        #print('date_elem: ', date_elem, ' max_date: ', max_date)
                        if date_elem >= max_date:
                                elem['ISOdt'] = date_elem
                                btc_histo_DAO.insert_btc_data_no_duplicates('btc_usd_history_' + tperiod,elem,0,'O')
                                #print(elem)
def trigger_interval(args):
        if args == 'all':
                print('Request to updated all intervals...')#insert_new_item(args)
                for tinter in tintervals:
                        insert_new_items(tinter)
                        print('Finished interval: ' + tinter)
        else:
                insert_new_items(args)
                
def parse_args():
        parser = argparse.ArgumentParser(description='Get BTC/USD price history, as 1 & 5 min time periods')
        parser.add_argument('-i','--interval', choices=list(set(['all']).union(set(tintervals))), required=True)
        #parser.add_argument('-s','--since', nargs='?', default='2017-01-01T00:00:00', metavar='YYYY-MM-DDThh:mm:ss') 
        #parser.add_argument('-u','--until', nargs='?', default=time.strftime('%Y-%m-%dT%H:%M:%S'), metavar='YYYY-MM-DD') 
        args = parser.parse_args()
        v_args = vars(args)
        return v_args

def main(vargs):
        trigger_interval(vargs['interval'])

                                
if __name__ == "__main__":
        v_args = parse_args()
        main(v_args)

