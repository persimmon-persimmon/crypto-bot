import datetime
import pickle
import numpy as np
import sklearn
from sklearn.ensemble import RandomForestRegressor as RFR
import logging
import os
import datetime
from threading import Thread
from multiprocessing import Process, Queue
from dataclasses import dataclass
from lib.channel_lib import *
from lib.api_lib import *
from collections import deque

@dataclass
class Flags:
    pair:str='BTC/JPY'
    coin:str='BTC'
    lot:float=0.0001
    delta:int=1
    product_id:int=5
    leverage_level:int=2
    long_indi:int=25
    short_indi:int=-25
    his_num:int=3
    book_num:int=5


class MessageHandler:
    def __init__(self,q_book,q_execution,q_out=None,his_num=20,n=1):
        self.q_book=q_book
        self.q_execution=q_execution
        self.q_out=q_out
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
        n=self.n
        v=self.q_execution.get()
        now=int(float(v['timestamp']))
        ohlcv=[0]*9
        # [timestamp,open,high,low,close,volume,sell_volume,buy_volume,count]
        ohlcv[0]=now
        ohlcv[1]=v['price']
        ohlcv[2]=v['price']
        ohlcv[3]=v['price']
        ohlcv[4]=v['price']
        ohlcv[5]+=v['price']*v['quantity']
        if v['taker_side']=='sell':
            ohlcv[6]+=v['price']*v['quantity']
        else:
            ohlcv[7]+=v['price']*v['quantity']
        ohlcv[8]+=1
        while self.end_flg==0:
            v=self.q_execution.get()
            v['timestamp']=int(float(v['timestamp']))
            if v['timestamp']-now<n:
                ohlcv[2]=max(ohlcv[2],v['price'])
                ohlcv[3]=min(ohlcv[3],v['price'])
                ohlcv[4]=v['price']
                ohlcv[5]=v['price']*v['quantity']
                if v['taker_side']=='sell':
                    ohlcv[6]=v['price']*v['quantity']
                    ohlcv[7]=0
                else:
                    ohlcv[6]=0
                    ohlcv[7]=v['price']*v['quantity']
                ohlcv[8]+=1
            else:
                while not v['timestamp']-now<n:
                    self.ohlcv_ary.append(ohlcv[:])
                    self.book_ary.append(self.book_newest.copy())
                    if self.q_out is not None:self.q_out.put(self.ohlcv_ary[-1])
                    if self.q_out is not None:self.q_out.put(self.book_ary[-1])
                    if len(self.book_ary)>self.his_num:self.book_ary.popleft()
                    if len(self.ohlcv_ary)>self.his_num:self.ohlcv_ary.popleft()
                    now+=n
                    close_price=ohlcv[4]
                    ohlcv=[0]*9
                    ohlcv[1]=close_price
                    ohlcv[2]=close_price
                    ohlcv[3]=close_price
                    ohlcv[4]=close_price
                    ohlcv[0]=now
                    ohlcv[8]=0
                ohlcv=[0]*9
                ohlcv[0]=int(float(v['timestamp']))
                ohlcv[1]=v['price']
                ohlcv[2]=v['price']
                ohlcv[3]=v['price']
                ohlcv[4]=v['price']
                ohlcv[5]+=v['price']*v['quantity']
                if v['taker_side']=='sell':
                    ohlcv[6]+=v['price']*v['quantity']
                else:
                    ohlcv[7]+=v['price']*v['quantity']
                ohlcv[8]+=1

    def join(self):
        self.end_flg=1
        self.t_book.join()
        self.t_ohlcv.join()
        
def channel_connector(q_book,q_execution):
    t1=Thread(target=channel_execution_details_cash,args=(q_execution,))
    t2=Thread(target=channel_price_ladders,args=(q_book,))
    t1.start()
    t2.start()


if __name__=='__main__':
    flags=Flags()

    current_dir=os.path.dirname(os.path.realpath(__file__))
    # ログ設定
    logger = logging.getLogger(__name__)
    # level 20以上のログを出す
    logger.setLevel(20)
    # ログをコンソールに表示する。
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    # ログをファイルに出す
    fh = logging.FileHandler(os.path.join(current_dir,f'{__file__}.log'))
    logger.addHandler(fh)
    # ログのフォーマットを指定する。
    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    # ポジションログ設定
    posLogger = logging.getLogger(__name__+'position_')
    # level 20以上のログを出す
    posLogger.setLevel(20)
    # ログをファイルに出す
    fhp = logging.FileHandler(os.path.join(current_dir,f'{__file__}.position.log'))
    posLogger.addHandler(fhp)

    # random forest load
    with open(os.path.join(current_dir,'rf.pkl'),'rb') as f:
        rg=pickle.load(f)
    result_dict={}
    for k in ['order_cnt','executed_order_cnt','total_return',]:
        result_dict[k]=0
    result_dict['return_ary']=[]
    ordered=None

    q_book=Queue()
    q_execution=Queue()
    mh=MessageHandler(q_book,q_execution)
    p_channel=Process(target=channel_connector,args=(q_book,q_execution))
    p_channel.start()
    logger.info('start')
    for j in range(10):
        time.sleep(5)
        if ordered is not None:# 前回ループで注文をしている場合、キャンセルまたはクローズする。
            close_flg=False
            d=cancel_order(ordered['id'],flags.pair)
            if d is None:
                result_dict['executed_order_cnt']+=1
                close_flg=True
            time.sleep(1)
            ordered=None
            if close_flg:
                poss=position_close_all()
                for pos in poss:
                    tmp=[]
                    for k in['id','side','quantity','open_price','close_price','pnl','created_at']:
                        tmp.append(str(pos[k]))
                    result_dict['return_ary'].append(pos['pnl'])
                    posLogger.info(','.join(tmp))
                    logger.info(','.join(tmp))
        # random forest の入力データ作成
        # 入力データ=[o,h,l,c,sell_v,buy_v]*his_num + [ask_price,ask_quantity]*book_num + [bid_price,bid_quantity]*book_num
        input_data=[]
        book=mh.book_newest
        ohlcv_ary=mh.ohlcv_ary
        if len(ohlcv_ary)<flags.his_num:continue
        # best_bid_priceを基準にする
        base=float(book['bids'][0][0])
        for i in range(flags.his_num):
            input_data.append(ohlcv_ary[-1-i][1]/base)
            input_data.append(ohlcv_ary[-1-i][2]/base)
            input_data.append(ohlcv_ary[-1-i][3]/base)
            input_data.append(ohlcv_ary[-1-i][4]/base)
            input_data.append(ohlcv_ary[-1-i][6]/base)
            input_data.append(ohlcv_ary[-1-i][7]/base)
        for i in range(flags.book_num):
            input_data.append(float(book['asks'][i][0])/base)
            input_data.append(float(book['asks'][i][1]))
        for i in range(flags.book_num):
            input_data.append(float(book['bids'][i][0])/base)
            input_data.append(float(book['bids'][i][1]))

        indi=int(rg.predict([input_data])[0])
        # 注文はロングのみ
        if indi>flags.long_indi:
            logger.info(f'long entry:indi={indi}, base={base}, {j},{len(mh.ohlcv_ary)}')
            order=limit_leverage('buy', flags.lot, base + flags.delta,datetime.datetime.now().timestamp()+1,flags.leverage_level,logger)
            ordered=order
            result_dict['order_cnt']+=1
        elif indi<flags.short_indi:
            logger.info(f'short entry:indi={indi}, base={base}, {j},{len(mh.ohlcv_ary)}')
            order=limit_leverage('sell', flags.lot, float(book['asks'][0][0]) - flags.delta,datetime.datetime.now().timestamp()+1,flags.leverage_level,logger)
            ordered=order
            result_dict['order_cnt']+=1
        else:
            logger.info(f'not entry:indi={indi}, base={base}, {j},{len(mh.ohlcv_ary)}')

    time.sleep(5)
    if ordered is not None:# 前回ループで注文をしている場合、キャンセルまたはクローズする。
        close_flg=False
        d=cancel_order(ordered['id'],flags.pair)
        if d is None:
            result_dict['executed_order_cnt']+=1
            close_flg=True
        time.sleep(1)
        ordered=None
        if close_flg:
            poss=position_close_all()
            for pos in poss:
                tmp=[]
                for k in['id','side','quantity','open_price','close_price','pnl','created_at']:
                    tmp.append(str(pos[k]))
                result_dict['return_ary'].append(pos['pnl'])
                posLogger.info(','.join(tmp))
                logger.info(','.join(tmp))

    mh.join()
    p_channel.terminate()
    ret_ary=result_dict['return_ary']
    order_cnt=result_dict['order_cnt']
    executed_order_cnt=result_dict['executed_order_cnt']
    if len(ret_ary):
        ret_ary=np.array(ret_ary)
        logger.info(f'order:{order_cnt}, executed order:{executed_order_cnt}, total return:{ret_ary.sum()}, sharpe ratio={ret_ary.mean()/ret_ary.var() if ret_ary.var() else None}')
    else:
        logger.info(f'order:{order_cnt}, executed order:{executed_order_cnt}')

