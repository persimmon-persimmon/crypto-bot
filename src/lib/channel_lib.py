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

# private order
def channel_user_order(q):
    def update_callback(data):
        data=json.loads(data)
        q.put(data)
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



# test
def data_handle(q):
    data=q.get()
    print(data)
    
if __name__=='__main__':
    # プロセス設定
    q=Queue()
    #p1=Process(target=channel_user_order,args=(q,))
    p1=Process(target=channel_user_trade,args=(q,))
    #p1=Process(target=channel_user_execution,args=(q,))
    #p1=Process(target=channel_user_order,args=(q,))
    
    #p0=Process(target=data_handle,args=(q,))
    p1.start()
    #p2.start()
    #p3.start()
    #p0.start()
    for _ in range(10):
        v=q.get()
        print(v)
        #time.sleep(10)
    p1.terminate()
    #p0.terminate()
