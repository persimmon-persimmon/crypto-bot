from lib.api_lib import *
from lib.channel_lib import *
from threading import Thread
from collections import deque
import datetime
from multiprocessing import Process,Queue
from pprint import pprint
import time
import os
import csv


class MessageHandler:
    def __init__(self,q_book,q_execution,q1,his_num=20,n=5):
        self.q_book=q_book
        self.q_execution=q_execution
        self.q1=q1
        self.his_num=his_num
        self.book_ary=deque([])
        self.book_newest=None
        self.ohlcv_ary=deque([])
        self.end_flg=0
        self.t_book=Thread(target=self.get_book)
        self.t_ohlcv=Thread(target=self.get_ohlcv)
        self.n=n
        self.t_book.start()
        self.t_ohlcv.start()

    def get_book(self):
        while self.end_flg==0:
            v=self.q_book.get()
            self.book_newest=v

    # n秒足
    def get_ohlcv(self):
        JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
        n=self.n
        v=self.q_execution.get()
        now=int(float(v['timestamp']))
        ohlcv=[0]*11
        # [timestamp,datetime_jst,open,high,low,close,volume,buy_volume,sell_volume,first_execution_id,last_execution_id]
        ohlcv[0]=now
        ohlcv[1]=datetime.datetime.fromtimestamp(now,JST)
        ohlcv[2]=v['price']
        ohlcv[3]=v['price']
        ohlcv[4]=v['price']
        ohlcv[5]=v['price']
        ohlcv[6]+=v['price']*v['quantity']
        if v['taker_side']=='buy':
            ohlcv[7]+=v['price']*v['quantity']
        else:
            ohlcv[8]+=v['price']*v['quantity']
        ohlcv[9]=v['id']
        ohlcv[10]=v['id']
        while self.end_flg==0:
            v=self.q_execution.get()
            v['timestamp']=int(float(v['timestamp']))
            if v['timestamp']-now<n:
                ohlcv[3]=max(ohlcv[2],v['price'])
                ohlcv[4]=min(ohlcv[3],v['price'])
                ohlcv[5]=v['price']
                ohlcv[6]=v['price']*v['quantity']
                if v['taker_side']=='buy':
                    ohlcv[7]+=v['price']*v['quantity']
                else:
                    ohlcv[8]+=v['price']*v['quantity']
                ohlcv[10]=v['id']
            else:
                while not v['timestamp']-now<n:
                    self.ohlcv_ary.append(ohlcv[:])
                    self.book_ary.append(self.book_newest.copy())
                    self.q1.put(self.ohlcv_ary[-1])
                    self.q1.put(self.book_ary[-1])
                    if len(self.book_ary)>self.his_num:self.book_ary.popleft()
                    if len(self.ohlcv_ary)>self.his_num:self.ohlcv_ary.popleft()
                    now+=n
                    close_price=ohlcv[5]
                    last_id=ohlcv[10]
                    ohlcv=[0]*11
                    ohlcv[0]=now
                    ohlcv[1]=datetime.datetime.fromtimestamp(now,JST)
                    ohlcv[2]=close_price
                    ohlcv[3]=close_price
                    ohlcv[4]=close_price
                    ohlcv[5]=close_price
                    ohlcv[6]=0
                    ohlcv[7]=0
                    ohlcv[8]=0
                    ohlcv[9]=last_id
                    ohlcv[10]=last_id
                ohlcv=[0]*11
                ohlcv[0]=now
                ohlcv[1]=datetime.datetime.fromtimestamp(now,JST)
                ohlcv[2]=v['price']
                ohlcv[3]=v['price']
                ohlcv[4]=v['price']
                ohlcv[5]=v['price']
                ohlcv[6]+=v['price']*v['quantity']
                if v['taker_side']=='buy':
                    ohlcv[7]+=v['price']*v['quantity']
                else:
                    ohlcv[8]+=v['price']*v['quantity']
                ohlcv[9]=v['id']
                ohlcv[10]=v['id']

    def join(self):
        self.end_flg=1
        self.t_book.join()
        self.t_ohlcv.join()
        
def channel_connector(q_book,q_execution):
    t1=Thread(target=channel_execution_details_cash,args=(q_execution,))
    t2=Thread(target=channel_price_ladders,args=(q_book,))
    t1.start()
    t2.start()

# タイムスケールをn倍にする
def downsample_ohlcv(ohlcv,n):
    ret_ohlcv=[]
    for i in range(0,len(ohlcv),n):
        tmp=ohlcv[i][:]
        for j in range(1,n):
            tmp[0]=ohlcv[i+j][0]
            tmp[2]=max(tmp[2],ohlcv[i+j][2])
            tmp[3]=min(tmp[3],ohlcv[i+j][3])
            tmp[4]=ohlcv[i+j][4]
            tmp[5]+=ohlcv[i+j][5]
            tmp[6]+=ohlcv[i+j][6]
            tmp[7]+=ohlcv[i+j][7]
            tmp[8]+=ohlcv[i+j][8]
        ret_ohlcv.append(tmp)
    return ret_ohlcv

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
    with open(os.path.join(current_dir,ohlcv_file),'w',encoding=output_encoding) as f:
        header=['timestamp','datetime_jst','open','high','low','close','volume','buy_volume','sell_volume','first_execution_id','last_execution_id']
        writer=csv.writer(f)
        writer.writerow(header)

    with open(os.path.join(current_dir,book_file),'w',encoding=output_encoding) as f:
        header=['timestamp']
        for i in range(21):
            header.append(f'ask_price_{i}')
            header.append(f'ask_quantity_{i}')
        for i in range(21):
            header.append(f'bid_price_{i}')
            header.append(f'bid_quantity_{i}')
        writer=csv.writer(f)
        writer.writerow(header)

    mh=MessageHandler(q_book,q_execution,q1,n=1)
    p1=Process(target=channel_connector,args=(q_book,q_execution))
    p1.start()
    counter=0
    while True:
        if counter%10==0:print(counter)
        counter+=1
        ohlcv=q1.get()
        book=q1.get()
        nymdh=datetime.datetime.fromtimestamp(ohlcv[0],jst).strftime('%Y%m%d_%H')
        if ymdh!=nymdh:
            ymdh=nymdh
            ohlcv_file=f'ohlcv_{ymdh}.csv'
            book_file=f'book_{ymdh}.csv'
            with open(os.path.join(current_dir,ohlcv_file),'w',encoding=output_encoding) as f:
                header=['timestamp','datetime_jst','open','high','low','close','volume','buy_volume','sell_volume','first_execution_id','last_execution_id']
                writer=csv.writer(f)
                writer.writerow(header)
            with open(os.path.join(current_dir,book_file),'w',encoding=output_encoding) as f:
                header=['timestamp']
                for i in range(21):
                    header.append(f'ask_price_{i}')
                    header.append(f'ask_quantity_{i}')
                for i in range(21):
                    header.append(f'bid_price_{i}')
                    header.append(f'bid_quantity_{i}')
                writer=csv.writer(f)
                writer.writerow(header)

        with open(os.path.join(current_dir,ohlcv_file),'a',encoding=output_encoding) as f:
            writer=csv.writer(f)
            writer.writerow(ohlcv)

        with open(os.path.join(current_dir,book_file),'a',encoding=output_encoding) as f:
            tmp=[book['timestamp']]
            for i in range(21):
                tmp.append(book['asks'][i][0])
                tmp.append(book['asks'][i][1])
            for i in range(21):
                tmp.append(book['bids'][i][0])
                tmp.append(book['bids'][i][1])
            writer=csv.writer(f)
            writer.writerow(tmp)

    mh.join()
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
