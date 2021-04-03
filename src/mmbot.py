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
    formatter = logging.Formatter('%(asctime)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    while True:
        v=q_log.get()
        # ログ書き込み
        logger.info(v)

# ハイパーパラメータ
@dataclass
class Paras:
    delta:int=5 # best priceに対するorder price
    allow_dd:float=0.8 # 許容ドローダウン
    lot:float=0.0001 # 一度の注文量
    entry_spread:int=500
    leverage_level:int=2

#処理フロー
#１．最新情報を取得しロングとショート両方の指値注文を入れる。
#２．5秒待つ
#３．両方約定していれば１に戻る。
#４．両方未約定ならキャンセルして１に戻る。
#５．ロングのみ約定しているとき。（ショートが未約定）
#５－１．best_askがロングの買値よりdelta以上高ければショートの指値を編集して２に戻る
#５－２．上に当てはまらないとき、ショート注文をキャンセル、ロングポジションを決済して１に戻る
#６．ショートのみ約定しているとき。（ロングが未約定）
#６－１．best_bidがショートの売値よりdelta以上低ければロングの指値を編集して２に戻る
#６－２．上に当てはまらないとき、ロング注文をキャンセル、ショートポジションを決済して１に戻る

if __name__=='__main__1':
    value=get_trades(status='closed')
    ret=value[0]['pnl']
    pprint(value[0])
    pprint(value[1])


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
    max_asset=get_free_balance()

    time.sleep(5)
    
    #pprint(mh.book_newest)
    #mh.join()
    #p1.terminate()

    #exit()
    entry_flg=False
    now_entry=None
    executor=ThreadPoolExecutor(max_workers=2)
    loop_count=0

    result_dict={}
    for k in ['order_count','cancel_count','limit_closed_count','market_closed_count']:
        result_dict[k]=0
    return_ary=[]
    value=get_trades()
    last_trade_id=value[0]['id'] # 記録済みでかつ最後のトレードid
    lase_trade_index=-1 # return_ary内で記録済みで最大のidx
    while True:
        if entry_flg==False:
            loop_count+=1
            #１．最新情報を取得しロングとショート両方の指値注文を入れる。

            # 資産を計算し、許容DDを超えていた場合、appendする
            # v1ではここの処理をするときノー注文、ノーポジションなので日本円のみ取得すればいい。
            timestamp=datetime.datetime.now().timestamp()
            if loop_count%10==0:
                value=get_free_balance(timestamp) # 日本金を取得
                timestamp+=1
                now_asset=value
                if now_asset/max_asset<paras.allow_dd:
                    # 注文キャンセル、ポジション決済
                    # リターンを記録
                    break
                max_asset=max(now_asset,max_asset)
                if len(return_ary)>0:
                    value=get_trades()
                    i=-1
                    for j in range(len(value)):
                        if value[-j-1]['id']==last_trade_id:
                            i=len(value)-j
                    while i<len(value):
                        lase_trade_index+=1
                        if len(return_ary)<=lase_trade_index:break
                        if return_ary[lase_trade_index]['close_type']=='limit':
                            if i+1>=len(value):break
                            return_ary[lase_trade_index]['return']=value[i]['pnl']+value[i+1]['pnl']
                            i+=2
                        else:
                            return_ary[lase_trade_index]['return']=value[i]['pnl']
                            i+=1
                        last_trade_id=value[i-1]['id']
                        assert lase_trade_index<=len(return_ary),'return_aryの数が足りない。return_ary:'+ str(len(return_ary)) + ', index:' + str(lase_trade_index)
                if loop_count>50:break
            if mh.book_newest['asks'][0][0]-mh.book_newest['bids'][0][0]<paras.entry_spread:
                time.sleep(random.random())
                continue
            args_ary=[] # buyとsellの注文引数 # side,size,price,timestamp
            #side,size,price,timestamp,leverage_level
            args_ary.append(('sell',paras.lot,mh.book_newest['asks'][0][0]-paras.delta,timestamp,paras.leverage_level))
            timestamp+=1
            args_ary.append(('buy',paras.lot,mh.book_newest['bids'][0][0]+paras.delta,timestamp,paras.leverage_level))
            timestamp+=1
            orders=executor.map(limit_leverage_pool,args_ary)
            for order in orders:
                if order['side']=='sell':
                    sell_order=order
                else:
                    buy_order=order
            q_log.put('entry sell_order:' + str(sell_order['price']) + ', buy_order:' + str(buy_order['price']))
            now_entry={'timestamp':datetime.datetime.now().timestamp(),'sell':sell_order['price'],'buy':buy_order['price'],'return':0}
            result_dict['order_count']+=1
            entry_flg=True
        #２．5秒待つ
        time.sleep(5)
        if entry_flg==False:continue
        timestamp=datetime.datetime.now().timestamp()
        
        # 注文の最新状態を取得
        order_ids=[str(buy_order['id']),str(sell_order['id'])]
        orders=get_some_orders(order_ids)
        # debug用：想定上発生しない例外
        assert orders is not None,'debug用:想定上発生しない例外。注文が存在しません:'+str(orders)
        for order in orders:
            if order['side']=='sell':
                sell_order=order
            else:
                buy_order=order

        # v1ではremainingは無視して処理を作る。つまり注文時の状態は、注文量すべて約定か、すべて未約定かのどちらかのみ。
        # 少量ロットで稼働する場合は問題ない。
        #３．両方約定していれば１に戻る。
        if sell_order['status']=='closed' and buy_order['status']=='closed':
            # リターンを記録
            # get_tradesは情報に遅延があるので即時リターンを取ることはできない。なので時間が経過してからリターンを取得する。
            # クローズしたタイミングではクローズ種類を記録し配列に入れる。
            now_entry['close_type']='limit'
            return_ary.append(now_entry)
            entry_flg=False
            result_dict['limit_closed_count']+=1
            q_log.put(f'limit close')

        #４．両方未約定ならキャンセルして１に戻る。  # v2:todo:タッチの差で約定した場合の処理
        if sell_order['status']=='open' and buy_order['status']=='open':
            args_ary=[sell_order['id'],buy_order['id']]
            executor.map(cancel_order,args_ary)
            entry_flg=False
            q_log.put('cancel')
            result_dict['cancel_count']+=1

        #５．ロングのみ約定しているとき。（ショートが未約定）
        if sell_order['status']=='open' and buy_order['status']=='closed':
            best_ask=mh.book_newest['asks'][0][0]
            #５－１．best_askがロングの買値よりdelta以上高ければショートの指値を編集→２に戻る
            #５－２．上に当てはまらないとき、ショート注文をキャンセル、ロングポジションを決済→１に戻る
            if best_ask>=buy_order['price']+paras.delta:
                #注文を編集
                value=edit_order(sell_order['id'],best_ask-paras.delta,paras.lot,timestamp)
                timestamp+=1
                if value is None: # 約定済み
                    # リターンを記録
                    now_entry['close_type']='limit'
                    return_ary.append(now_entry)
                    entry_flg=False
                    q_log.put(f'limit close')
                    result_dict['limit_closed_count']+=1
                else:
                    sell_order=value
            else:
                cancel_order(sell_order['id'])
                value=position_close_all()
                # リターンを記録
                now_entry['close_type']='market'
                return_ary.append(now_entry)
                entry_flg=False
                q_log.put(f'market close')
                result_dict['market_closed_count']+=1
                entry_flg=False

        #６．ショートのみ約定しているとき。（ロングが未約定）
        if sell_order['status']=='closed' and buy_order['status']=='open':
            best_bid=mh.book_newest['bids'][0][0]
            #６－１．best_bidがショートの売値よりdelta以上低ければロングの指値を編集→２に戻る
            #６－２．上に当てはまらないとき、ロング注文をキャンセル、ショートポジションを決済→１に戻る
            if best_bid<=sell_order['price']-paras.delta:
                value=edit_order(buy_order['id'],best_bid+paras.delta,paras.lot,timestamp)
                timestamp+=1
                if value is None: # 約定済み
                    # リターンを記録
                    now_entry['close_type']='limit'
                    return_ary.append(now_entry)
                    entry_flg=False
                    q_log.put(f'limit close')
                    result_dict['limit_closed_count']+=1
                else:
                    buy_order=value
            else:
                cancel_order(buy_order['id'])
                value=position_close_all()
                # リターンを記録
                now_entry['close_type']='market'
                return_ary.append(now_entry)
                entry_flg=False
                q_log.put(f'market close')
                result_dict['market_closed_count']+=1
                entry_flg=False

    for x in return_ary:
        q_log.put(json.dumps(x))

    if len(return_ary):
        return_ary=np.array([x['return'] for x in return_ary])
        q_log.put(f'{json.dumps(result_dict)}, total return:{return_ary.sum()}, sharpe ratio={return_ary.mean()/return_ary.var() if return_ary.var() else None}')
    else:
        q_log.put(f'{json.dumps(result_dict)}')

    print('terminate MessageHandler ..')
    mh.join()
    print('terminate WebSocket Connector ..')
    p1.terminate()
    print('terminate Log Writer ..')
    p2.terminate()
    print('shutdown thread pool ..')
    executor.shutdown()
    print('????')

"""
v2でやること
・v1:remainingを考慮しない
->v2:remainingを考慮する

・v1:片方のみ約定時、5秒に1回bookを確認し、損失が発生する前に決済する。
->v2:片方のみ約定時、常にbookを確認し、損失が発生する直前に決済する。

（オプション）
・v1:指値の値は機械的に算出する。
->v2:値動きを予測し、いい感じのところに指値を置く。

"""
