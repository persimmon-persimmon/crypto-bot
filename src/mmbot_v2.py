from lib.api_lib import *
from lib.channel_lib import *
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

# リアルタイムで板情報、ローソク足を記録するクラス。
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
            v['asks']=[[float(x),float(y)] for x,y in v['asks']]
            v['bids']=[[float(x),float(y)] for x,y in v['bids']]
            v['timestamp']=float(v['timestamp'])
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
                    if self.q1 is not None:self.q1.put(self.ohlcv_ary[-1])
                    if self.q1 is not None:self.q1.put(self.book_ary[-1])
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

# ログ書込プロセス用
def log_writer(q_log):
    # ログ設定
    # 取得ログ種類
    # ・プログラムログ
    # ・エントリーログ、イグジットログ
    # ・リターンログ

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
    #formatter = logging.Formatter('%(asctime)s:%(message)s')
    #fh.setFormatter(formatter)
    #sh.setFormatter(formatter)
    while True:
        v=q_log.get()
        # ログ書き込み
        logger.info(v)

# 板情報を累積にする
# row=[timestamp]+[ask_price,ask_quantity]*21+[bid_price,bid_quantity]*21
# ->ask_ary,bid_ary :それぞれbest_priceからlap円刻みの累積量n個分
def book_to_accumulation(row,lap=100,n=10):
    now_price=row[1]
    now_quantity=row[2]
    idx=3
    ask_ary=[now_quantity]
    while idx<42:
        if row[idx]<now_price+lap:
            ask_ary[-1]+=row[idx+1]
            idx+=2
        else:
            now_price+=lap
            ask_ary.append(ask_ary[-1])
            if len(ask_ary)==n:break
    while len(ask_ary)<n:ask_ary.append(ask_ary[-1])
    now_price=row[43]
    now_quantity=row[44]
    idx=45
    bid_ary=[now_quantity]
    while idx<84:
        if row[idx]>now_price-lap:
            bid_ary[-1]+=row[idx+1]
            idx+=2
        else:
            now_price-=lap
            bid_ary.append(bid_ary[-1])
            if len(bid_ary)==n:break
    while len(bid_ary)<n:bid_ary.append(bid_ary[-1])
    return ask_ary,bid_ary

# ハイパーパラメータ
@dataclass
class Paras:
    delta:int=10 # best priceに対するorder price
    allow_dd:float=0.8 # 許容ドローダウン
    lot:float=0.0001 # 一度の注文量
    entry_spread:int=800
    cut_return:int=200 # ポジションの期待リターンがこれを下回っていたら決済する。
    leverage_level:int=2
    loop_num:int=8
    append_loop_num:int=50
    sigma:int=30   # 値動きは分散ganmaのブラウン運動 ???
    ganma:float=0.5   # リスク回避度   ????
    


"""
処理フロー
(1).在庫、板状況、ローソク足情報を元に指値位置を決める。
(2).1の指値位置にロングとショート両方の注文を入れる。
(3).5秒待つ
(4).注文状況を確認する。在庫情報を更新。
(5).いくつかの注文をキャンセル、捌ける見込みのない在庫(ポジション)をクローズ。1に戻る。

改善すること
・remainingを考慮する
・指値価格決定ロジックを実装
・キャンセル、決済ロジックを実装
・リターン記録方法を改善。後から、この時刻の発注がダメだったとわかるように。
・ログ記録方法を改善。
"""
if __name__=='__main__':
    paras=Paras()
    current_dir=os.path.dirname(__file__)

    # web socket接続プロセス
    q_book=Queue()
    q_execution=Queue()
    mh=MessageHandler(q_book,q_execution,q1=None,n=1)
    p1=Process(target=channel_connector,args=(q_book,q_execution))
    p1.start()

    # log書込プロセス
    q_log=Queue()
    p2=Process(target=log_writer,args=(q_log,))
    p2.start()
    q_log.put('start')
    now_asset=get_free_balance()
    max_asset=now_asset
    while mh.book_newest is None:
        time.sleep(2)

    # 結果を記録する変数

    #注文用スレッドプール
    executor=ThreadPoolExecutor(max_workers=4)
    loop_count=0

    # 現在のエントリー状態
    entry_flg=False
    now_entry=None
    sells={}
    buys={}
    sell_ary=[]
    buy_ary=[]
    order_count=0
    # 在庫量
    stock=0

    while True:
        loop_count+=1
        timestamp=datetime.datetime.now().timestamp()

        # 何回かに一回行う処理
        if loop_count%10==0:
            # 資産を計算し、許容DDを超えていた場合、強制終了する
            # 現状、ここの処理をするとき未約定注文なし、ノーポジションなので日本円のみ取得すればいい。
            value=get_free_balance(timestamp) # 日本金を取得
            timestamp+=1
            now_asset=value
            if now_asset/max_asset<paras.allow_dd:
                # 注文キャンセル、ポジション決済
                # リターンを記録
                break
            max_asset=max(now_asset,max_asset)

        # テスト稼働用。一定数のループでbreak
        if loop_count>paras.loop_num and len(sells)==0 and len(buys)==0:break
        if loop_count>paras.append_loop_num:break

        # 指値を入れる処理

        # 在庫を指値位置を決める
        # 最良価格の中央値
        st=(mh.book_newest['asks'][0][0]+mh.book_newest['bids'][0][0])/2
        # Soffset
        so=st-paras.ganma*paras.sigma*stock
        ask_bid=paras.ganma*paras.sigma+1000
        ask_price=int(so+ask_bid/2)
        bid_price=int(so-ask_bid/2)
        s={}
        s['loop_count']=loop_count
        s['best_ask']=mh.book_newest['asks'][0][0]
        s['ask_price']=ask_price
        s['best_bid']=mh.book_newest['bids'][0][0]
        s['bid_price']=bid_price
        s['stock']=stock
        s['executed/order']=str(order_count-len(sells)-len(buys))+'/'+str(order_count)
        print(s)
        if loop_count<=paras.loop_num:
            args_ary=[] # buyとsellの注文引数 #side,size,price,timestamp,leverage_level
            args_ary.append(('sell',paras.lot,ask_price,timestamp,paras.leverage_level))
            timestamp+=1
            args_ary.append(('buy',paras.lot,bid_price,timestamp,paras.leverage_level))
            timestamp+=1
            value=executor.map(limit_leverage_pool,args_ary)
            for order in value:
                if order is None:continue
                if order['side']=='sell':
                    sells[order['id']]=order
                    sell_order=order
                if order['side']=='buy':
                    buys[order['id']]=order
                    buy_order=order

            order_count+=2
            q_log.put('entry sell_order:' + str(sell_order['price']) + ', buy_order:' + str(buy_order['price']) + ', delta:'+str(sell_order['price']-buy_order['price']))
            now_entry={'timestamp':datetime.datetime.now().timestamp(),'sell':sell_order['price'],'buy':buy_order['price'],'return':0
                        ,'stock':stock,'executed ratio':1-(len(buys)+len(sells))/order_count}
        
        #(2).5秒待つ
        time.sleep(5)

        # 注文の最新状態を取得
        order_ids=list(sells.keys())+list(buys.keys())
        value=get_some_orders(order_ids)

        for order in value:
            if order is None:continue
            if order['side']=='sell':
                if order['status']=='closed':
                    sells.pop(order['id'])
                    sell_ary.append(order['quantity']*order['price'])
            if order['side']=='buy':
                if order['status']=='closed':
                    buys.pop(order['id'])
                    buy_ary.append(order['quantity']*order['price'])
        # 在庫量更新
        stock=len(sells)-len(buys)


    print('return:',sum(sell_ary)-sum(buy_ary),'stock:',stock,'return(calcrated stock):',sum(sell_ary)-sum(buy_ary)+stock*st*paras.lot)

    print('terminate MessageHandler ..')
    mh.join()
    print('terminate WebSocket Connector ..')
    p1.terminate()
    print('terminate Log Writer ..')
    p2.terminate()
    print('shutdown thread pool ..')
    executor.shutdown()
