import liquidtap
import jwt
import json
import time
import datetime
from multiprocessing import Process, Queue
from threading import Thread
from pprint import pprint
import os
import json

import argparse
import csv
import pytz
import traceback
from quoine.client import Quoinex
from collections import deque

# マーケット情報 
def channel_market(q): 
    def update_callback_market(data): 
        data=json.loads(data) 
        q.put(data) 
    def on_connect(data): 
        tap.pusher.subscribe("product_cash_btcjpy_5").bind('updated', update_callback_market) 
    tap = liquidtap.Client() 
    tap.pusher.connection.bind('pusher:connection_established', on_connect) 
    tap.pusher.connect() 
    while True: 
        time.sleep(10) 

# 約定情報 
def channel_executions_cash(q): 
    def update_callback_executions(data): 
        data=json.loads(data) 
        q.put(data) 
    def on_connect(data): 
        tap.pusher.subscribe("executions_cash_btcjpy").bind('created', update_callback_executions) 
    tap = liquidtap.Client() 
    tap.pusher.connection.bind('pusher:connection_established', on_connect) 
    tap.pusher.connect() 
    while True: 
        time.sleep(10) 

# 約定情報（詳細。買注文idと売注文idがある）
def channel_execution_details_cash(q):
    def update_callback_executions_detail(data):
        data=json.loads(data)
        q.put(data)
    def on_connect(data):
        tap.pusher.subscribe("executions_cash_btcjpy").bind('created', update_callback_executions_detail)
    tap = liquidtap.Client()
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        time.sleep(10)

# 約定情報を集計しローソク足にする。->[timestamp,open,high,low,close,volume,taker_sell_volume,taker_buy_volume]
def executions_to_ohlcv(q1,q2):
    ohlcv=[datetime.datetime.now().timestamp(),-1,-1,-1,-1,-1,0,0]
    start_time=ohlcv[0]
    while True:
        v=q1.get()
        if float(v['timestamp'])-start_time>5:
            q2.put(ohlcv[:])
            ohlcv[1]=-1
            start_time+=5
        if ohlcv[1]==-1:
            ohlcv[0]=float(v['timestamp'])
            ohlcv[1]=v['price']
            ohlcv[2]=v['price']
            ohlcv[3]=v['price']
            ohlcv[4]=v['price']
            ohlcv[5]=v['price']*v['quantity']
            if v['taker_side']=='sell':
                ohlcv[6]=v['price']*v['quantity']
                ohlcv[7]=0
            else:
                ohlcv[6]=0
                ohlcv[7]=v['price']*v['quantity']
        else:
            ohlcv[0]=float(v['timestamp'])
            ohlcv[2]=max(ohlcv[2],v['price'])
            ohlcv[3]=min(ohlcv[3],v['price'])
            ohlcv[4]=v['price']
            ohlcv[5]+=v['price']*v['quantity']
            if v['taker_side']=='sell':
                ohlcv[6]+=v['price']*v['quantity']
            else:
                ohlcv[7]+=v['price']*v['quantity']
# ローソク足。だいたい5秒足
def channel_ohlcv(q):
    q1=Queue()
    t1=Thread(target=channel_execution_details_cash,args=(q1,))
    t2=Thread(target=executions_to_ohlcv,args=(q1,q))
    t1.start()
    t2.start()
    while True:
        time.sleep(10)

# 板情報
def channel_price_ladders(q):
    def update_callback(data):
        data=json.loads(data)
        q.put(data)
    def on_connect(data):
        tap.pusher.subscribe("price_ladders_cash_btcjpy").bind('updated', update_callback)
    tap = liquidtap.Client()
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        time.sleep(10)

token='token'
secret='secret'
try:
    current_dir=os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(current_dir,'setting.csv')) as f:
        ary=f.readline().split(',')
        ary=f.readline().split(',')
        token=ary[0]
        secret=ary[1]
except:
    pass
status_dict={'live':'open','filled':'closed','canceled':'canceled'}
# private order
def channel_user_order(q):
    def update_callback(data):
        data=json.loads(data)
        value={'id':data['id']
                ,'status':status_dict[data['status']]
                ,'filled':data['filled_quantity']
                ,'remaining':data['quantity']-data['filled_quantity']
                ,'quantity':data['quantity']
                ,'price':data['price']
                ,'side':data['side']
                ,'timestamp':data['created_at']
                }
        q.put(value)
    def on_connect(data):
        tap.pusher.subscribe("user_account_jpy_orders").bind('updated', update_callback)
    tap = liquidtap.Client(token,secret)
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        time.sleep(2)

# private trade
def channel_user_trade(q):
    def update_callback(data):
        data=json.loads(data)
        q.put(data)
    def on_connect(data):
        tap.pusher.subscribe("user_account_jpy_trades").bind('updated', update_callback)
    tap = liquidtap.Client(token,secret)
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        time.sleep(10)

# private executions
def channel_user_execution(q):
    def update_callback(data):
        data=json.loads(data)
        q.put(data)
    def on_connect(data):
        tap.pusher.subscribe("user_executions_cash_btcjpy").bind('created', update_callback)
    tap = liquidtap.Client(token,secret)
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        time.sleep(10)



def fetch_executions(timestamp, limit=1000, product_id=5):
    api = Quoinex("", "")
    retry_count = 0
    while True:
        try:
            results = api.get_executions_since_time(product_id, timestamp, limit=limit)
            return [{"id": item["id"],
                    "timestamp": float(item["created_at"]),
                    "taker_side": str(item["taker_side"]),
                    "price": float(item["price"]),
                    "quantity": float(item["quantity"])} for item in results]
        except Exception as e:
            retry_count += 1
            if retry_count <= 3:
                print("エラーが発生しました. 10秒待機して再トライします.",e)
                time.sleep(10)
            else:
                print("エラーが発生しました. 停止します.")
                raise e

# qに指定期間の約定情報をputする。apiで取得する。
def virtual_channel_execution_details_cash_api(q,start_timestamp=None,end_timestamp=None):
    if start_timestamp is None:
        start_timestamp=datetime.datetime.now()-datetime.timedelta(days=5)
        start_timestamp=datetime.datetime.strptime('202104010000','%Y%m%d%H%M')

        start_timestamp=start_timestamp.timestamp()
    if end_timestamp is None:
        end_timestamp=datetime.datetime.now()-datetime.timedelta(minutes=50)
        end_timestamp=datetime.datetime.strptime('202104040000','%Y%m%d%H%M')
        end_timestamp=end_timestamp.timestamp()
    
    counter = 0
    while True:
        counter+=1
        if start_timestamp > end_timestamp:
            break
        executions = fetch_executions(start_timestamp)
        if len(executions) == 0:
            break
        for execution in executions:
            q.put(execution)
        start_timestamp = executions[-1]['timestamp'] + 1
        time.sleep(1)

# qに指定期間の約定情報をputする。datファイルから取得する。
def virtual_channel_execution_details_cash(q,start_timestamp=None,end_timestamp=None):
    if start_timestamp is None:
        start_timestamp=datetime.datetime.now()-datetime.timedelta(days=5)
        start_timestamp=datetime.datetime.strptime('202104010000','%Y%m%d%H%M')
        start_timestamp=start_timestamp.timestamp()
    if end_timestamp is None:
        end_timestamp=datetime.datetime.now()-datetime.timedelta(minutes=50)
        end_timestamp=datetime.datetime.strptime('202104040000','%Y%m%d%H%M')
        end_timestamp=end_timestamp.timestamp()
    
    jst = datetime.timezone(datetime.timedelta(hours=9), "JST")
    start_datetime=datetime.datetime.fromtimestamp(start_timestamp,jst)
    end_datetime=datetime.datetime.fromtimestamp(end_timestamp,jst)
    files=[]
    while start_datetime<end_datetime:
        ymdh=datetime.datetime.strftime(start_datetime,'%Y%m%d_%H')
        files.append(f'execution_{ymdh}.dat')
        start_datetime+=datetime.timedelta(hours=1)
    current_dir=os.path.dirname(os.path.realpath(__file__))
    for fl in files:
        with open(os.path.join(current_dir,fl)) as f:
            data=f.readlines()
        for d in data:
            q.put(json.loads(d))

# qに指定期間の板情報をputする。datファイルから取得する。
def virtual_channel_price_ladders(q,start_timestamp=None,end_timestamp=None):
    if start_timestamp is None:
        start_timestamp=datetime.datetime.now()-datetime.timedelta(days=5)
        start_timestamp=datetime.datetime.strptime('202104010000','%Y%m%d%H%M')
        start_timestamp=start_timestamp.timestamp()
    if end_timestamp is None:
        end_timestamp=datetime.datetime.now()-datetime.timedelta(minutes=50)
        end_timestamp=datetime.datetime.strptime('202104040000','%Y%m%d%H%M')
        end_timestamp=end_timestamp.timestamp()
    
    jst = datetime.timezone(datetime.timedelta(hours=9), "JST")
    start_datetime=datetime.datetime.fromtimestamp(start_timestamp,jst)
    end_datetime=datetime.datetime.fromtimestamp(end_timestamp,jst)
    files=[]
    while start_datetime<end_datetime:
        ymdh=datetime.datetime.strftime(start_datetime,'%Y%m%d_%H')
        files.append(f'book_{ymdh}.dat')
        start_datetime+=datetime.timedelta(hours=1)

    current_dir=os.path.dirname(os.path.realpath(__file__))
    for fl in files:
        with open(os.path.join(current_dir,fl)) as f:
            data=f.readlines()
        for d in data:
            q.put(json.loads(d))

# qに指定期間の1秒ローソク足と板情報をputする
def virtual_channel_ohlcv_and_book(q_ohlcv,q_book,start_timestamp=None,end_timestamp=None):
    if start_timestamp is None:
        start_timestamp=datetime.datetime.now()-datetime.timedelta(days=5)
        start_timestamp=datetime.datetime.strptime('202104010000','%Y%m%d%H%M')
        start_timestamp=start_timestamp.timestamp()
    if end_timestamp is None:
        end_timestamp=datetime.datetime.now()-datetime.timedelta(minutes=50)
        end_timestamp=datetime.datetime.strptime('202104040000','%Y%m%d%H%M')
        end_timestamp=end_timestamp.timestamp()
    # 指定された期間のdatファイルを列挙
    ymd_h=datetime.fromtimestamp(start_timestamp)
    ymd_h=datetime.datetime.fromtimestamp(st,jst)
    book_dat=[]
    ohlcv_dat=[]
    start_datetime=datetime.datetime.fromtimestamp(start_timestamp,jst)
    datetime.datetime.strftime(start_datetime,'%y%m%d_%H')

    jst=datetime.timezone(datetime.timedelta(hours=9), "JST")
    datetime_format = "%Y-%m-%d %H:%M:%S"
    counter = 0
    pre_timestamp = start_timestamp - 1
    while True:
        if counter%100==0:
            now = datetime.datetime.fromtimestamp(start_timestamp, tz=jst).astimezone(jst).strftime(datetime_format)
        counter+=1
        if start_timestamp > end_timestamp:
            break
        executions = fetch_executions(start_timestamp)
        if len(executions) == 0:
            break
        for execution in executions:
            q.put(execution)
        start_timestamp = executions[-1]['timestamp'] + 1
        time.sleep(1)

if __name__=='__main__':
    # プロセス設定
    q=Queue()
    #p1=Process(target=channel_user_order,args=(q,))
    #p1=Process(target=channel_user_trade,args=(q,))
    #p1=Process(target=channel_user_execution,args=(q,))
    #p1=Process(target=channel_user_order,args=(q,))
    start_timestamp=datetime.datetime.strptime('202104091900','%Y%m%d%H%M')
    start_timestamp=start_timestamp.timestamp()
    end_timestamp=datetime.datetime.strptime('202104092000','%Y%m%d%H%M')
    end_timestamp=end_timestamp.timestamp()
    #virtual_channel_execution_details_cash(q,start_timestamp,end_timestamp)

    p1=Process(target=virtual_channel_price_ladders,args=(q,start_timestamp,end_timestamp))
    
    #p0=Process(target=data_handle,args=(q,))
    p1.start()
    #p2.start()
    #p3.start()
    #p0.start()
    for _ in range(100):
        v=q.get()
        print(v)
        #time.sleep(10)
    print(datetime.datetime.now().timestamp())
    p1.terminate()
    #p0.terminate()
