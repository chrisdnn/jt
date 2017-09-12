from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterRestPager
import json
import pymongo
import m_tweetsDAO_pre
from datetime import datetime
import argparse
import time

#first, setup mongodb connection
connection_string = "mongodb://cdunneg01"
connection = pymongo.MongoClient(connection_string)
#dbs = connection.dbtweets ## For MTY uses
dbs = connection.btctweets ## For MTY uses
tweetsDAO = m_tweetsDAO_pre.M_tweetsDAO_pre(dbs)


consumer_key = '6a8eiydF31fyJrJ3vJFtmpHV9'
consumer_secret = '8tyZ34jIk2AM3WxN18JimbRC58FRyzTUSX83l7lC7Tz54rrO2H'
access_token_key = '80065688-IMt9thIgmN0CM7oNK0tUGAVoC6Nkdr01XNWhl3Eu1'
access_token_secret = 'xUjSwaK5PGAVRhFJj37U1OqJ44iISWqXAwrJOsIRaayKP'
SEARCH_TERM = ''

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




v_args = []



def main(vargs):
#for item in r:
        r = TwitterRestPager(api,'search/tweets', {'q':'btc/bitcoin', 'since':vargs['since'],'until':vargs['until']}) #,'since':'2017-08-15'})
        for item in r.get_iterator():
                item['created_at_dt'] = datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                tweetsDAO.insert_tweet('tweets',item, 0,'')
        
def parse_args():
        parser = argparse.ArgumentParser(description='Process some tweets')
        parser.add_argument('-g','--get', choices=['track','history'], required=True)
        parser.add_argument('-s','--since', nargs='?', default='2017-01-01', metavar='YYYY-MM-DD') 
        parser.add_argument('-u','--until', nargs='?', default=time.strftime('%Y-%m-%d'), metavar='YYYY-MM-DD') 
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
