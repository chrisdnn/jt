from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterRestPager
from TwitterAPI import TwitterError
import json
import pymongo
import data_DAO
from datetime import datetime
import argparse
import time
import pytz
import sys

#first, setup mongodb connection
connection_string = "mongodb://cdunneg01"
connection = pymongo.MongoClient(connection_string)
#dbs = connection.dbtweets ## For MTY uses
dbs = connection.btctweets ## For MTY uses
tweetsDAO = data_DAO.Data_DAO(dbs)
dbcollection_tweets = 'tweets'

#DEFAULTS
consumer_key = '6a8eiydF31fyJrJ3vJFtmpHV9'
consumer_secret = '8tyZ34jIk2AM3WxN18JimbRC58FRyzTUSX83l7lC7Tz54rrO2H'
access_token_key = '80065688-IMt9thIgmN0CM7oNK0tUGAVoC6Nkdr01XNWhl3Eu1'
access_token_secret = 'xUjSwaK5PGAVRhFJj37U1OqJ44iISWqXAwrJOsIRaayKP'
SEARCH_TERM = ''
#####
get_type_def = {'track':{'endpoint':'statuses/filter','method':'track'},
                'history':{'endpoint':'search/tweets','method':'q'} }

#sys.stdout.encoding='utf-8'

#api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret, auth_type='oAuth2')
api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

## Tweets from San Pedro Garza Garcia
#r = api.request('search/tweets', {'q':SEARCH_TERM, 'geocode':'25.66173253,-100.35253104,5km','count':'450','until':'2015-05-21'})
## With track words
#r = api.request('statuses/filter', {'track':'mty,monterrey,sanpedro,spdp,garzagarcia,calzada del valle,margain,av vasconselos','locations':'-100.598350, 25.547178, -100.036674, 25.856532'})
## Track words
#r = api.request('statuses/filter', {'track':'NED CHI, Holland, Chile, HOL CHI, Holanda, Chile, World Cup, Paises Bajos Chile, Nederlands Chile, netherlands chile'})
## all mexico (includes part of Pacicic Ocean, Arizona, New Mexico, Texas and Alabama)
#r = api.request('statuses/filter', {'locations':'-118.001287, 14.443567, -87.591130, 32.480436'})
#r = api.request('statuses/filter', {'track':'btc,bitcoin', 'until':'2017-08-14'})
#r = api.request('statuses/filter', {'track':'btc,bitcoin'}) #,'since':'2017-08-15'})


#Time intervals
tintervals = ['onemin', 'fivemin', 'fifteenmin', 'thirtymin', 'hour', 'day']
tintervals_dt_measure = {'onemin':1, 'fivemin':5, 'fifteenmin':15,'thirtymin':30, 'hour':1, 'day':1}

v_args = []

#parses datetime into different interval buckets for later processing VS history exchange rate data
def parse_datetime(str_datetime):
        v_min = int(datetime.strftime(str_datetime, '%M'))
        v_fivemin = v_min/tintervals_dt_measure['fivemin']*tintervals_dt_measure['fivemin']
        v_fifteenmin = v_min/tintervals_dt_measure['fifteenmin']*tintervals_dt_measure['fifteenmin']
        v_thirtymin = v_min/tintervals_dt_measure['thirtymin']*tintervals_dt_measure['thirtymin']
        v_hour = int(datetime.strftime(str_datetime, '%H'))
        v_day = int(datetime.strftime(str_datetime, '%d'))
        v_month = int(datetime.strftime(str_datetime, '%m'))
        v_year = int(datetime.strftime(str_datetime, '%Y'))
        ### parsed datetime objects
        json_data = {}
        json_data['created_at_dt'] = str_datetime 
        json_data['onemin'] = datetime(v_year,v_month,v_day, v_hour,v_min,0,0, pytz.UTC)
        json_data['fivemin'] = datetime(v_year,v_month,v_day, v_hour,v_fivemin,0,0, pytz.UTC)
        json_data['fifteenmin'] = datetime(v_year,v_month,v_day, v_hour,v_fifteenmin,0,0, pytz.UTC)
        json_data['thirtymin'] = datetime(v_year,v_month,v_day, v_hour,v_thirtymin,0,0, pytz.UTC)
        json_data['hour'] = datetime(v_year,v_month,v_day, v_hour,0,0,0, pytz.UTC)
        json_data['day'] = datetime(v_year,v_month,v_day, 0,0,0,0, pytz.UTC)
        
        #print('dt: ', str_datetime,' five: ', v_fivemin, ' fifteen: ',v_fifteenmin, ' thirty: ',v_thirtymin, ' ---- dt_one: ', json_data['onemin'],' dt_five: ', json_data['fivemin'], ' dt_fifteen: ',json_data['fifteenmin'])
        return json_data

def get_store_tweets(get_type, vkeywords, dtsince, dtuntil, dbcollection):
        v_endpoint = get_type_def[get_type]['endpoint']
        v_method = get_type_def[get_type]['method']
        v_q_dates = {v_method: vkeywords, 'since':dtsince, 'until':dtuntil }
        if get_type == 'track':
                v_q_dates = { v_method: vkeywords }
                #v_endpoint = get_type_def['track']['endpoint']
                #v_endpoint = get_type_def['track']['method']
        '''
        print('Track: top lvl TRACK:', get_type_def['track'])
        print('History: top lvl HISTORY:', get_type_def['history'])
        print('Endpoint: ', v_endpoint)
        print('Method: ', v_method)
        '''
        #for i in range(0,5):
        while True:
                try:
                        r = TwitterRestPager(api,
                                             v_endpoint,
                                             v_q_dates)
                        for item in r.get_iterator():
                                if 'created_at' in item:
                                        created_at_dt = datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                                        item['dt_parsed'] = parse_datetime(created_at_dt)
                                        tquery = {'id':item['id']}
                                        tweetsDAO.insert_data_no_duplicates(dbcollection,tquery, item, 0,'')
                                        ### error handling for streaming
                                elif 'limit' in item:
                                        print('Tweets missed: ', item['limit']['track'])
                                elif 'disconnect' in item:
                                        print('Disconnecting because : ', item['disconnect']['reason'])
                                        break
                                ### error handling for history feed
                                elif 'message' in item:
                                        print('Received MESSAGE: ', item['messae'], ' code: ', item['code'] )
                                else:
                                        print('Tweet item: ', item)
                except KeyError as ke:
                        print('---- Missing Key error raw :', ke)
                except StandardError as e:
                        print('---- Error raw :', e)
                        print('---- Error forat : {0}'.format(e))
                        print('---- Error detail: ', sys.exc_info()[0])
                        #        except TwitterRequestError as tre:
                        #                print('---- Error raw: ', tre ,' formatted : {0}'.format(tre), ' Detail: ', sys.exc_info()[0])
                except TwitterError.TwitterRequestError as tre:
                        if tre.status_code < 500:
                                # something needs to be fixed before re-connecting
                                print('---- Error raw: ', tre ,' formatted : {0}'.format(tre), ' Detail: ', sys.exc_info()[0])
                                raise
                        else:
                                # temporary interruption, re-try request
                                pass
                except TwitterError.TwitterConnectionError as tce:
                        # temporary interruption, re-try request
                        print('---- Temporary interruption: ', tce ,' formatted : {0}'.format(tce), ' Detail: ', sys.exc_info()[0])
                        pass
        

def main(vargs):
#for item in r:

        get_store_tweets(vargs['get'], vargs['keywords'] , vargs['since'], vargs['until'], vargs['collection'])
        '''
        r = TwitterRestPager(api,'search/tweets', {'q':'btc/bitcoin', 'since':vargs['since'],'until':vargs['until']}) #,'since':'2017-08-15'})
        for item in r.get_iterator():
                created_at_dt = datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                item['dt_parsed'] = parse_datetime(created_at_dt)
                tweetsDAO.insert_data('tweets_test',item, 0,'')
        '''
        
def parse_args():
        parser = argparse.ArgumentParser(description='Process some tweets')
        parser.add_argument('-g','--get', choices=['track','history'], required=True)
        parser.add_argument('-s','--since', nargs='?', default='2017-01-01', metavar='YYYY-MM-DD') 
        parser.add_argument('-u','--until', nargs='?', default=time.strftime('%Y-%m-%d'), metavar='YYYY-MM-DD')
        parser.add_argument('-k','--keywords', nargs='?', default='btc,bitcoin', metavar='keywords_1, keyword_2, ... , keyword_N')
        parser.add_argument('-c','--collection', nargs='?', default=dbcollection_tweets, metavar='collection_name') 
        args = parser.parse_args()
        v_args = vars(args)
        print(args)
        return v_args

if __name__ == "__main__":
        v_args = parse_args()
        main(v_args)

'''
        if 'text' in item:
                tweetsDAO.insert_tweet(item)
        elif 'limit' in item:
                print('%d tweets missed' % item['limit']['track'])
        elif 'disconnect' in item:
                print('disconnecting because %s' % item['disconnect']['reason'])
                break
'''
