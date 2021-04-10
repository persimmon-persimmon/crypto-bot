from .api_lib import *
from .channel_lib import *
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import datetime
from multiprocessing import Process,Queue
from pprint import pprint
import time
import os
import datetime
from dataclasses import dataclass
import random
import logging
import numpy as np
# リアルタイムでローソク足を記録するクラス。n秒足を作成。過去his_num個分の履歴を持つ。
class CandleManager:
    def __init__(self,q_execution,q1=None,his_num=20,n=5,backtest=False):
        self.q_execution=q_execution
        self.q1=q1
        self.his_num=his_num
        self.ohlcv_ary=deque(maxlen=his_num)
        self.end_flg=0
        self.t_ohlcv=Thread(target=self.get_ohlcv)
        self.n=n
        self.backtest=backtest
        self.t_ohlcv.start()

    # n秒足
    def get_ohlcv(self):
        JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
        ohlcv={}
        # [timestamp,datetime_jst,open,high,low,close,volume,buy_volume,sell_volume,first_execution_id,last_execution_id]
        v=self.q_execution.get()
        now=int(float(v['timestamp']))
        ohlcv={}
        ohlcv['timestamp']=now
        ohlcv['open']=v['price']
        ohlcv['high']=v['price']
        ohlcv['low']=v['price']
        ohlcv['close']=v['price']
        ohlcv['volume']=v['price']*v['quantity']
        if v['taker_side']=='buy':
            ohlcv['buy_volume']=v['price']*v['quantity']
            ohlcv['sell_volume']=0
        else:
            ohlcv['buy_volume']=0
            ohlcv['sell_volume']=v['price']*v['quantity']
        ohlcv['first_execution_id']=v['id']
        ohlcv['last_execution_id']=v['id']
        while self.end_flg==0:
            try:
                v=self.q_execution.get(timeout=3)
            except:
                continue
            v['timestamp']=int(float(v['timestamp']))
            if v['timestamp']-now<self.n:
                ohlcv['high']=max(ohlcv['high'],v['price'])
                ohlcv['low']=min(ohlcv['low'],v['price'])
                ohlcv['close']=v['price']
                ohlcv['volume']+=v['price']*v['quantity']
                if v['taker_side']=='buy':
                    ohlcv['buy_volume']+=v['price']*v['quantity']
                else:
                    ohlcv['sell_volume']+=v['price']*v['quantity']
                ohlcv['last_execution_id']=v['id']
            else:
                while not v['timestamp']-now<self.n:
                    self.ohlcv_ary.append(ohlcv.copy())
                    if self.q1 is not None:self.q1.put(self.ohlcv_ary[-1])
                    now+=self.n
                    close_price=ohlcv['close']
                    last_id=ohlcv['last_execution_id']
                    ohlcv={}
                    ohlcv['timestamp']=now
                    ohlcv['open']=close_price
                    ohlcv['high']=close_price
                    ohlcv['low']=close_price
                    ohlcv['close']=close_price
                    ohlcv['volume']=0
                    ohlcv['buy_volume']=0
                    ohlcv['sell_volume']=0
                    ohlcv['first_execution_id']=last_id
                    ohlcv['last_execution_id']=last_id
                ohlcv['timestamp']=now
                ohlcv['open']=v['price']
                ohlcv['high']=v['price']
                ohlcv['low']=v['price']
                ohlcv['close']=v['price']
                ohlcv['volume']=v['price']*v['quantity']
                if v['taker_side']=='buy':
                    ohlcv['buy_volume']=v['price']*v['quantity']
                    ohlcv['sell_volume']=0
                else:
                    ohlcv['buy_volume']=0
                    ohlcv['sell_volume']=v['price']*v['quantity']
                ohlcv['first_execution_id']=v['id']
                ohlcv['last_execution_id']=v['id']
    # スレッドを止める
    def join(self):
        self.end_flg=1
        self.t_ohlcv.join()


# 一秒ごとの板情報。過去his_num個分の履歴を持つ。板情報の変更がない時刻に板情報は作られない。
class BookManager:
    def __init__(self,q_book,his_num=200):
        self.q_book=q_book
        self.his_num=his_num
        self.book_dict={}
        self.book_newest=None
        self.end_flg=0
        self.t_book=Thread(target=self.get_book)
        self.t_book.start()

    # 板情報
    def get_book(self):
        while self.end_flg==0:
            try:
                v=self.q_book.get(timeout=1)
            except:
                continue
            v['asks']=[[float(x),float(y)] for x,y in v['asks']]
            v['bids']=[[float(x),float(y)] for x,y in v['bids']]
            v['timestamp']=float(v['timestamp'])
            self.book_dict[int(v['timestamp'])]=v
            self.book_newest=v
            pop_stamp=int(v['timestamp'])-self.his_num
            if pop_stamp in self.book_dict:
                self.book_dict.pop(pop_stamp)

    # スレッドを止める
    def join(self):
        self.end_flg=1
        self.t_book.join()


# 注文を取り仕切る。WebSocketから流れてくる情報をキャッチし注文状況を更新する。在庫管理も行う。
class OrderManager:
    def __init__(self,q_user_order):
        self.q_user_order=q_user_order
        self.order_dict={}
        self.stock=0.
        self.end_flg=0
        self.sell_executed_quantity=0
        self.buy_executed_quantity=0
        self.sell_order_quantity=0
        self.buy_order_quantity=0
        self.jpy_delta=0
        self.executor=ThreadPoolExecutor(max_workers=4)
        self.start_timestamp=datetime.datetime.now().timestamp()
        self.t_user_order=Thread(target=self.get_user_order)
        self.t_user_order.start()

    def get_user_order(self,):
        while self.end_flg==0:
            try:
                order=self.q_user_order.get(timeout=5)
            except:
                continue
            if order['timestamp']<self.start_timestamp:continue
            pre=self.order_dict[order['id']]['filled'] if order['id'] in self.order_dict else 0.
            self.order_dict[order['id']]=order
            if order['side']=='buy':
                self.stock+=order['filled']-pre
                self.buy_executed_quantity+=order['filled']-pre
                self.jpy_delta-=(order['filled']-pre)*order['price']
            else:
                self.stock-=order['filled']-pre
                self.sell_executed_quantity+=order['filled']-pre
                self.jpy_delta+=(order['filled']-pre)*order['price']
            if order['status']=='closed':
                self.order_dict.pop(order['id'])
            self.stock=round(self.stock,8)

    def limit_order(self,args):
        value=self.executor.map(limit_leverage_pool,args)
        for order in value:
            if order['side']=='sell':
                self.sell_order_quantity+=order['quantity']
                self.sell_executed_quantity+=order['filled']
            else:
                self.buy_order_quantity+=order['quantity']
                self.buy_executed_quantity+=order['filled']
        return value

    # 指定したsideのオーダーをキャンセル
    def om_cancel_order(self,side=None):
        if side is None:
            args=[order['id'] for order in self.order_dict]
        elif side=='sell':
            args=[order['id'] for order in self.order_dict if order['side']=='sell']
        elif side=='buy':
            args=[order['id'] for order in self.order_dict if order['side']=='buy']
        for order_id in args:
            cancel_order_liquid(order_id)
            self.order_dict.pop(order_id)
    
    # 指定したポジションをクローズする。
    def om_position_close_all(self):
        values=position_close_all()
        for value in values:
            pass
        self.stock=0

    def average_price(self):
        if abs(self.stock)<1e-5:return None
        return self.jpy_delta/self.stock

    def join(self,):
        self.end_flg=1
        self.t_user_order.join()
        self.executor.shutdown()


# 注文を取り仕切る。WebSocketから流れてくる情報をキャッチし注文状況を更新する。在庫管理も行う。
class OrderManagerBacktest:
    def __init__(self,params):
        self.params=params
        self.execute_ratio=params.execute_ratio
        self.order_dict={}
        self.stock=0.
        self.sell_executed_quantity=0
        self.buy_executed_quantity=0
        self.sell_order_quantity=0
        self.buy_order_quantity=0
        self.jpy_delta=0
        self.start_timestamp=datetime.datetime.now().timestamp()

    def put_execution(self,execution):
        #execution=self.q_execution.get(timeout=5)
        if not self.order_dict:return
        # 自分の注文内に合致するものがあるか調べる。あれば更新。
        keys=list(self.order_dict.keys())
        for k in keys:
            myorder=self.order_dict[k]
            if myorder['timestamp']>=execution['timestamp']:continue
            if execution['taker_side']=='buy':
                if myorder['side']=='buy':continue
                if execution['price']>=myorder['price']:
                    # sell注文更新
                    d=min(execution['quantity'],myorder['remaining'])
                    myorder['filled']+=d
                    myorder['remaining']-=d
                    execution['quantity']-=d
                    self.stock-=d
                    self.sell_executed_quantity+=d
                    self.jpy_delta+=d*myorder['price']
                    if myorder['remaining']<=1e-6:
                        self.order_dict.pop(myorder['id'])
                    if execution['quantity']<=1e-6:
                        break

            if execution['taker_side']=='sell':
                if myorder['side']=='sell':continue
                if execution['price']<=myorder['price']:
                    # buy注文更新
                    d=min(execution['quantity'],myorder['remaining'])
                    myorder['filled']+=d
                    myorder['remaining']-=d
                    execution['quantity']-=d
                    self.stock+=d
                    self.buy_executed_quantity+=d
                    self.jpy_delta-=d*myorder['price']
                    if myorder['remaining']<=1e-6:
                        self.order_dict.pop(myorder['id'])
                    if execution['quantity']<=1e-6:
                        break
            self.stock=round(self.stock,8)

    # ('sell',paras.lot,ask_price,timestamp,paras.leverage_level)
    def limit_order(self,args):
        for side,size,price,timestamp,leverage_level in args:
            order={'id':datetime.datetime.now().timestamp(),'side':side,'quantity':size,'price':price,'timestamp':timestamp,'filled':0,'remaining':size}
            self.order_dict[order['id']]=order
            if side=='buy':
                self.buy_order_quantity+=size
            if side=='sell':
                self.sell_order_quantity+=size
    # 指定したside,priceのオーダーをキャンセル
    def om_cancel_order(self,side=None,price=None):
        if side is None:
            args=[order['id'] for order in self.order_dict]
        elif side=='sell':
            args=[order['id'] for order in self.order_dict if order['side']=='sell' and order['price']>=price]
        elif side=='buy':
            args=[order['id'] for order in self.order_dict if order['side']=='buy' and order['price']<=price]
        for order_id in args:
            self.order_dict.pop(order_id)
    
    # 指定したポジションをクローズする。
    def om_position_close_all(self,best_ask,best_bid):
        # ポジションを全決済
        if self.stock==0:
            pass
        elif self.stock<0:
            self.jpy_delta-=self.stock*(best_bid+self.params.slipage_price)
        else:
            self.jpy_delta+=self.stock*(best_ask-self.params.slipage_price)
        self.stock=0

    def average_price(self):
        if abs(self.stock)<1e-5:return None
        return self.jpy_delta/self.stock

    def join(self):
        return


# リアルタイムで板情報、ローソク足を記録するクラス。
class MessageHandler:
    def __init__(self,q_book,q_execution,q1,his_num=20,n=5):
        self.q_book=q_book
        self.q_execution=q_execution
        self.q1=q1
        self.his_num=his_num
        self.book_ary=deque(maxlen=his_num)
        self.book_dict={}
        self.book_newest=None
        self.ohlcv_ary=deque(maxlen=his_num)
        self.end_flg=0
        self.t_book=Thread(target=self.get_book)
        self.t_ohlcv=Thread(target=self.get_ohlcv)
        self.n=n
        self.t_book.start()
        self.t_ohlcv.start()

    # 板情報
    def get_book(self):
        while self.end_flg==0:
            v=self.q_book.get()
            v['asks']=[[float(x),float(y)] for x,y in v['asks']]
            v['bids']=[[float(x),float(y)] for x,y in v['bids']]
            v['timestamp']=float(v['timestamp'])
            self.book_dict[int(v['timestamp'])]=v
            self.book_newest=v
            pop_stamp=int(v['timestamp'])-self.his_num*self.n-1
            if pop_stamp in self.book_dict:
                self.book_dict.pop(pop_stamp)

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
                    self.book_ary.append(self.book_dict[now] if now in self.book_dict else self.book_newest)
                    if self.q1 is not None:self.q1.put(self.ohlcv_ary[-1])
                    if self.q1 is not None:self.q1.put(self.book_ary[-1])
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

    # スレッドを止める
    def join(self):
        self.end_flg=1
        self.t_book.join()
        self.t_ohlcv.join()



# タイムスケールをn倍にする
def downsample_ohlcv(ohlcv,n):
    ret_ohlcv=[]
    for i in range(0,len(ohlcv),n):
        tmp=ohlcv[i][:]
        for j in range(1,n):
            tmp[0]=ohlcv[i+j][0]
            if ohlcv[i+j][5]==0:continue
            if tmp[5]==0:
                tmp[1]=ohlcv[i+j][3]
                tmp[2]=ohlcv[i+j][2]
                tmp[3]=ohlcv[i+j][3]
            else:
                tmp[2]=max(tmp[2],ohlcv[i+j][2])
                tmp[3]=min(tmp[3],ohlcv[i+j][3])
            tmp[4]=ohlcv[i+j][4]
            tmp[5]+=ohlcv[i+j][5]
            tmp[6]+=ohlcv[i+j][6]
            tmp[7]+=ohlcv[i+j][7]
            tmp[8]+=ohlcv[i+j][8]
        ret_ohlcv.append(tmp)
    return ret_ohlcv


# 板情報を累積にする
# row={'timestamp':timestamp,'asks':[[ask_price,ask_quantity] for _ in range(40)],'bids':[[bid_price,bid_quantity] for _ in range(40)]}
# ->ask_ary,bid_ary :それぞれstまたはbest priceからlap円刻みの累積量n個分　外から中央値方向への累積
# best priceとstどちらを使うべきか。stを使う場合、spreadが自然に反映されるが、exp関数に従わなくなる。
# 瞬間の板情報やスプレッドを使うとロバスト性が低くなる。平均みたいなのを使いたい。
# 1秒足は逆に使わない方がいい？むしろ出来高のほうがいいのでは。
def book_to_accumulation_in(row,lap=250,n=40):
    st=(row['asks'][0][0]+row['bids'][0][0])/2
    ary=row['asks']
    now_price=st if st is not None else ary[0][0]
    now_quantity=sum([ary[i][1] for i in range(40) if abs(ary[0][0]-ary[i][0])<=lap*n])
    ask_ary=[round(now_quantity,8),round(now_quantity,8)]
    idx=0
    now_price+=lap
    for i in range(1,n):
        while idx<len(ary) and ary[idx][0]<now_price:
            now_quantity-=ary[idx][1]
            idx+=1
        now_price+=lap
        ask_ary.append(round(now_quantity,8))
        if len(ask_ary)==n:break
        if ask_ary[-1]==0.:break
    while len(ask_ary)<n:ask_ary.append(ask_ary[-1])
    ary=row['bids']
    now_price=st if st is not None else ary[0][0]
    now_quantity=sum([ary[i][1] for i in range(40) if abs(ary[0][0]-ary[i][0])<=lap*n])
    bid_ary=[round(now_quantity,8),round(now_quantity,8)]
    idx=0
    now_price-=lap
    for i in range(1,n):
        while idx<len(ary) and ary[idx][0]>now_price:
            now_quantity-=ary[idx][1]
            idx+=1
        now_price-=lap
        bid_ary.append(round(now_quantity,8))
        if len(bid_ary)==n:break
        if bid_ary[-1]==0.:break
    while len(bid_ary)<n:bid_ary.append(bid_ary[-1])
    return ask_ary,bid_ary

# 板情報を累積にする
# row={'timestamp':timestamp,'asks':[[ask_price,ask_quantity] for _ in range(40)],'bids':[[bid_price,bid_quantity] for _ in range(40)]}
# ->ask_ary,bid_ary :それぞれstまたはbest priceからlap円刻みの累積量n個分　中央値から外方向への累積
def book_to_accumulation_out(row,lap=250,n=40):
    st=(row['asks'][0][0]+row['bids'][0][0])/2
    ary=row['asks']
    now_price=st if st is not None else ary[0][0]
    now_quantity=0
    ask_ary=[]
    idx=0
    now_price+=lap
    for i in range(n):
        while idx<len(ary) and ary[idx][0]<now_price:
            now_quantity+=ary[idx][1]
            idx+=1
        now_price+=lap
        ask_ary.append(round(now_quantity,8))
        if len(ask_ary)==n:break
    while len(ask_ary)<n:ask_ary.append(ask_ary[-1])
    ary=row['bids']
    now_price=st if st is not None else ary[0][0]
    now_quantity=0
    bid_ary=[]
    idx=0
    now_price-=lap
    for i in range(n):
        while idx<len(ary) and ary[idx][0]>now_price:
            now_quantity+=ary[idx][1]
            idx+=1
        now_price-=lap
        bid_ary.append(round(now_quantity,8))
    while len(bid_ary)<n:bid_ary.append(bid_ary[-1])
    return ask_ary,bid_ary


#・板情報:（中央値からの）累積数量がxをはじめて越える中央値からの価格差。 
# (x=0.0001,0.0002,0.0004,0.0008,0.0016,0.0032,0.0064,0.0128,0.0256,0.0512,0.1024,0.2048,0.4096,0.8192,1.6384 (15個)) 
xary=[0.0001]
for _ in range(14):
    xary.append(xary[-1]*2)
def book_to_exp(book,x=xary):
    st=(book['asks'][0][0]+book['bids'][0][0])/2
    ask_exp=[]
    now_quantity=0
    now_price=st
    ary=book['asks']
    idx=0
    for x in xary:
        while idx<len(ary) and now_quantity+ary[idx][1]<=x:
            now_quantity+=ary[idx][1]
            idx+=1
        if idx==len(ary):
            ask_exp.append(ask_exp[-1])
        else:
            ask_exp.append(abs(st-ary[idx][0]))
    bid_exp=[]
    now_quantity=0
    now_price=st
    ary=book['bids']
    idx=0
    for x in xary:
        while idx<len(ary) and now_quantity+ary[idx][1]<=x:
            now_quantity+=ary[idx][1]
            now_price=ary[idx][0]
            idx+=1
        if idx==len(ary):
            bid_exp.append(bid_exp[-1])
        else:
            bid_exp.append(abs(st-ary[idx][0]))
    return ask_exp,bid_exp

from collections import deque
# 指数平潤移動平均
class Ema:
    def __init__(self,alpha,ary=[]):
        self.ori_ary=deque(maxlen=10)
        self.ema_ary=deque(maxlen=10)
        self.alpha=alpha
        for x in ary:
            self.add(x)
    def add(self,x):
        if len(self.ori_ary)==0:
            self.ori_ary.append(x)
            self.ema_ary.append(x)
        else:
            self.ori_ary.append(x)
            self.ema_ary.append(self.ema_ary[-1]+self.alpha*(x-self.ema_ary[-1]))

# 単純移動平均
class Sma:
    def __init__(self,n,ary=[]):
        self.ori_ary=deque(maxlen=n)
        self.sma_ary=deque(maxlen=n)
        self.n=n
        for x in ary:
            self.add(x)
    def add(self,x):
        if len(self.ori_ary)<self.n:
            self.ori_ary.append(x)
            if len(self.ori_ary)==self.n:
                self.sma_ary.append(sum(self.ori_ary)/self.n)
        else:
            self.sma_ary.append(self.sma_ary[-1]-self.ori_ary[0]/self.n+x/self.n)
            self.ori_ary.append(x)

def book_dict_to_row(book_dict):
    ret=[book_dict['timestamp']]
    for i in range(40):
        ret.append(book_dict['asks'][i][0])
        ret.append(book_dict['asks'][i][1])
    for i in range(40):
        ret.append(book_dict['bids'][i][0])
        ret.append(book_dict['bids'][i][1])
    return ret
def book_row_to_dict(book_row):
    ret={'timestamp':book_row[0]}
    ret['asks']=[]
    for i in range(21):
        ret['asks'].append([book_row[2*i+1],book_row[2*i+2]])
    ret['bids']=[]
    for i in range(21):
        ret['bids'].append([book_row[42+2*i+1],book_row[42+2*i+2]])
    return ret

import datetime
def ohlcv_dict_to_row(ohlcv_dict):
    JST=datetime.timezone(datetime.timedelta(hours=+9), 'JST')
    ret=[ohlcv_dict['timestamp']]
    ret.append(ohlcv_dict['open'])
    ret.append(ohlcv_dict['high'])
    ret.append(ohlcv_dict['low'])
    ret.append(ohlcv_dict['close'])
    ret.append(ohlcv_dict['volume'])
    ret.append(ohlcv_dict['buy_volume'])
    ret.append(ohlcv_dict['sell_volume'])
    ret.append(ohlcv_dict['first_execution_id'])
    ret.append(ohlcv_dict['last_execution_id'])
    return ret

def ohlcv_row_to_dict(ohlcv_row):
    ret={'timestamp':ohlcv_row[0]}
    ret['open']=ohlcv_row[1]
    ret['high']=ohlcv_row[2]
    ret['low']=ohlcv_row[3]
    ret['close']=ohlcv_row[4]
    ret['volume']=ohlcv_row[5]
    ret['buy_volume']=ohlcv_row[6]
    ret['sell_volume']=ohlcv_row[7]
    ret['first_execution_id']=ohlcv_row[8]
    ret['last_execution_id']=ohlcv_row[9]
    return ret