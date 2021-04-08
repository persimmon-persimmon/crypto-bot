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



def channel_connector(q_book,q_execution):
    t1=Thread(target=channel_execution_details_cash,args=(q_execution,))
    t2=Thread(target=channel_price_ladders,args=(q_book,))
    t1.start()
    t2.start()

if __name__=='__main__':
    current_dir=os.path.dirname(__file__)
    q_book=Queue()
    q_execution=Queue()
    q1=Queue()
    output_encoding = "shift_jis"
    jst=datetime.timezone(datetime.timedelta(hours=9), "JST")
    ymdh=datetime.datetime.now(jst).strftime('%Y%m%d_%H')
    ohlcv_file=f'ohlcv_{ymdh}.csv'
    book_file=f'book_{ymdh}.csv'
    cm=CandleManager(q_execution,q1,n=1)
    bm=BookManager(q_book)
    p1=Process(target=channel_connector,args=(q_book,q_execution))
    p1.start()
    counter=0
    while True:
        if counter%10==0:print(counter)
        counter+=1
        ohlcv=q1.get()
        timestamp=ohlcv['timestamp']
        count=0
        while timestamp not in bm.book_dict:
            timestamp-=1
            count+=1
            if count==20:break
        if count==20:continue
        book=bm.book_dict[timestamp]
        nymdh=datetime.datetime.fromtimestamp(ohlcv['timestamp'],jst).strftime('%Y%m%d_%H')
        if ymdh!=nymdh:
            ymdh=nymdh
            ohlcv_file=f'ohlcv_{ymdh}.csv'
            book_file=f'book_{ymdh}.csv'

        with open(os.path.join(current_dir,ohlcv_file),'a',encoding=output_encoding) as f:
            f.write(str(ohlcv))
            f.write('\n')

        with open(os.path.join(current_dir,book_file),'a',encoding=output_encoding) as f:
            f.write(str(book))
            f.write('\n')

    cm.join()
    bm.join()
    p1.terminate()

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
