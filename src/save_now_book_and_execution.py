from lib.api_lib import *
from lib.channel_lib import *
from lib.utills import *
from threading import Thread
from collections import deque
import datetime
from multiprocessing import Process,Queue
from pprint import pprint
import time
import os
import csv
import json


def channel_connector(q_book,q_execution):
    t1=Thread(target=channel_execution_details_cash,args=(q_execution,))
    t2=Thread(target=channel_price_ladders,args=(q_book,))
    t1.start()
    t2.start()

def save_execution_json(q_execution):
    current_dir=os.path.dirname(__file__)
    jst=datetime.timezone(datetime.timedelta(hours=9), "JST")
    ymdh=datetime.datetime.now(jst).strftime('%Y%m%d_%H')
    file_name=f'execution_{ymdh}.dat'
    count=0
    output_encoding="shift_jis"
    while True:
        if count%10==0:
            ymdh=datetime.datetime.now(jst).strftime('%Y%m%d_%H')
            file_name=f'execution_{ymdh}.dat'
        v=q_execution.get()
        with open(os.path.join(current_dir,file_name),'a',encoding=output_encoding) as f:
            f.write(json.dumps(v))
            f.write('\n')
def save_book_json(q_book):
    current_dir=os.path.dirname(__file__)
    jst=datetime.timezone(datetime.timedelta(hours=9), "JST")
    ymdh=datetime.datetime.now(jst).strftime('%Y%m%d_%H')
    file_name=f'book_{ymdh}.dat'
    count=0
    output_encoding="shift_jis"
    pre=-1
    # n秒ごとの板情報
    n=5
    while True:
        if count%10==0:
            ymdh=datetime.datetime.now(jst).strftime('%Y%m%d_%H')
            file_name=f'book_{ymdh}.dat'
        v=q_book.get()
        v['timestamp']=float(v['timestamp'])
        if int(v['timestamp'])<int(pre)+n:continue
        pre=int(v['timestamp'])
        with open(os.path.join(current_dir,file_name),'a',encoding=output_encoding) as f:
            f.write(json.dumps(v))
            f.write('\n')

if __name__=='__main__':
    current_dir=os.path.dirname(__file__)
    q_book=Queue()
    q_execution=Queue()
    p1=Process(target=channel_connector,args=(q_book,q_execution))
    t1=Thread(target=save_book_json,args=(q_book,))
    t2=Thread(target=save_execution_json,args=(q_execution,))
    p1.start()
    t1.start()
    t2.start()
    while True:
        time.sleep(10)


if __name__=='__main__1':
    current_dir=os.path.dirname(__file__)
    with open(os.path.join(current_dir,'ohlcv.csv'),'r') as f:
        r=f.readlines()[1:]
        ohlcv=[list(map(float,x.split(','))) for x in r]
    ret_ohlcv=downsample_ohlcv(ohlcv,5)
    print('before')
    for x in ohlcv:print(*x)
    print('after')
    for x in ret_ohlcv:print(*x)

# sudo nohup python3 save_now_n_seconds_ohlcv.py &