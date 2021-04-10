from lib.api_lib import *
from lib.channel_lib import *
from lib.utills import *
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
from math import log

def channel_connector(q_book,q_execution,q_user_order):
    t1=Thread(target=channel_execution_details_cash,args=(q_execution,))
    t2=Thread(target=channel_price_ladders,args=(q_book,))
    t3=Thread(target=channel_user_order,args=(q_user_order,))
    t1.start()
    t2.start()
    t3.start()

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
# 10分と5分の指数移動平均線でエントリーするbotを作ってみる。
# バックテストの関数も作る。
# 5分とかになったら板情報は無意味なきがするのでohlcvのみでバックテストができる関数

# ハイパーパラメータ
@dataclass
class Params:
    allow_dd:float=0.8 # 許容ドローダウン
    lot:float=0.0001 # 一度の注文量
    leverage_level:int=2
    loop_num:int=100
    
    # モデル変数。sigmaとalphaは変動する。ganmaは自分で設定する。???
    sigma:int=1500      # 値動きボラティリティσのブラウン運動 ???
    ganma:float=0.5     # リスク回避度γ   大きいほどリスクを回避する。??
                        # γ大：Soffsetが在庫を解消する方向に設定される。値幅(δask+bid)が大きくなる。???
    alpha:float=100     # 注文の強さを示す値。大きいほど強い。
                        # α大：板に対して成行が多い。値動きが活発になると予想される。
                        # α小：板に対して成行が少ない。値動きが少ないと予想される。
    # バックテスト用パラメータ
    backtest:bool=True
    start_timestamp:int=datetime.datetime.strptime('20210410_12','%Y%m%d_%H').timestamp()
    end_timestamp:int=datetime.datetime.strptime('20210410_16','%Y%m%d_%H').timestamp()
    execute_ratio:float=.7
    slipage_price:float=150
    delay:float=.1
    span:int=5
"""
処理フロー

"""
if __name__=='__main__':
    params=Params()
    current_dir=os.path.dirname(__file__)

    # データ取得先
    if params.backtest:
        # バックテスト用
        q_book=Queue()
        q_execution=Queue()
        #cm=CandleManager(q_execution)
        om=OrderManagerBacktest(params)
        bm=BookManager(q_book)
        vcb=virtual_channel('book',params)
        vce=virtual_channel('execution',params)
    else:
        # web socket接続プロセス
        q_book=Queue()
        q_execution=Queue()
        q_user_order=Queue()
        cm=CandleManager(q_execution)
        bm=BookManager(q_book)
        p1=Process(target=channel_connector,args=(q_book,q_execution,q_user_order))
        p1.start()
        om=OrderManager(q_user_order)

    """
    # log書込プロセス
    q_log=Queue()
    p2=Process(target=log_writer,args=(q_log,))
    p2.start()
    q_log.put('start')
    """
    # 初期
    if params.backtest:
        now_asset=100000
    else:
        now_asset=get_free_balance()
    max_asset=now_asset

    # 板情報にデータが入るまで時間を進める
    if params.backtest:
        while bm.book_newest is None:
            data=vce.next_data()
            for d in data:
                q_execution.put(d)
            data=vcb.next_data()
            for d in data:
                d['asks']=[[float(x),float(y)] for x,y in d['asks']]
                d['bids']=[[float(x),float(y)] for x,y in d['bids']]
                d['timestamp']=float(d['timestamp'])
                bm.book_newest=d
    else:
        while bm.book_newest is None:
            time.sleep(2)

    # 結果を記録する変数

    loop_count=0

    # 現在のエントリー状態
    now_entry=None
    if params.backtest:timestamp=vce.now_timestamp


    while True:
        if params.backtest:
            timestamp+=params.span
            if timestamp>=params.end_timestamp:break
        else:
            timestamp=datetime.datetime.now().timestamp()
        loop_count+=1

        # 何回かに一回行う処理
        if loop_count%10==0:
            # 資産を計算し、許容DDを超えていた場合、強制終了する
            # 現状、ここの処理をするとき未約定注文なし、ノーポジションなので日本円のみ取得すればいい。
            if params.backtest:
                now_asset=now_asset+om.jpy_delta
            else:
                value=get_free_balance(timestamp)
                timestamp+=1
                now_asset=value
            if now_asset/max_asset<params.allow_dd:
                break
            max_asset=max(now_asset,max_asset)

        # フォワードテスト用。一定数のループでbreak
        if params.backtest==False and loop_count>params.loop_num:break

        # 指値を入れる処理

        # 在庫を指値位置を決める
        # 最良価格の中央値
        st=(bm.book_newest['asks'][0][0]+bm.book_newest['bids'][0][0])/2
        # Soffset
        so=st-params.ganma*params.sigma*om.stock
        ask_bid=params.ganma*params.sigma+1/params.ganma*log(1+params.ganma/params.alpha)
        ask_price=int(so+ask_bid/2)
        bid_price=int(so-ask_bid/2)
        ask_price=bm.book_newest['asks'][0][0]-10
        bid_price=bm.book_newest['bids'][0][0]+10
        
        # 現在の注文状況
        s={}
        s['loop_count']=loop_count
        s['stock']=om.stock
        s['sell executed ratio']=round(om.sell_executed_quantity/om.sell_order_quantity,8) if om.sell_order_quantity else None
        s['buy executed ratio']=round(om.buy_executed_quantity/om.buy_order_quantity,8) if om.buy_order_quantity else None
        s['return(calcrated stock)']=om.jpy_delta+om.stock*st
        print(s)
        if params.backtest:
            # バックテストではtimestampで注文到着時刻を示す。遅延を考慮するためdelayをプラスする
            args_ary=[] # buyとsellの注文引数 #side,size,price,timestamp,leverage_level
            args_ary.append(('sell',params.lot,ask_price,timestamp+params.delay,params.leverage_level))
            args_ary.append(('buy',params.lot,bid_price,timestamp+params.delay,params.leverage_level))
        else:
            args_ary=[] # buyとsellの注文引数 #side,size,price,timestamp,leverage_level
            args_ary.append(('sell',params.lot,ask_price,timestamp,params.leverage_level))
            timestamp+=1
            args_ary.append(('buy',params.lot,bid_price,timestamp,params.leverage_level))
            timestamp+=1
        om.limit_order(args_ary)
        now_entry={'timestamp':timestamp,'sell':ask_price,'buy':bid_price,'delta':ask_price-bid_price}
    

        #(2).5秒待つ
        if params.backtest:
            # 5秒分の約定情報をOrderManageに入れる。
            data=vce.next_data()
            for d in data:
                q_execution.put(d)
                om.put_execution(d)

            # 5秒後の板情報をBookManagerに入れる。
            data=vcb.next_data()
            for d in data:
                d['asks']=[[float(x),float(y)] for x,y in d['asks']]
                d['bids']=[[float(x),float(y)] for x,y in d['bids']]
                d['timestamp']=float(d['timestamp'])
                bm.book_newest=d
        else:
            time.sleep(5)

    st=(bm.book_newest['asks'][0][0]+bm.book_newest['bids'][0][0])/2
    s={}
    s['loop_count']=loop_count
    s['stock']=om.stock
    s['sell executed ratio']=round(om.sell_executed_quantity/om.sell_order_quantity,8) if om.sell_order_quantity else None
    s['buy executed ratio']=round(om.buy_executed_quantity/om.buy_order_quantity,8) if om.buy_order_quantity else None
    s['return(calcrated stock)']=om.jpy_delta+om.stock*st
    print(s)

    print('return(calcrated stock):',round(om.jpy_delta+om.stock*st,8),'stock:',om.stock)
    print('terminate MessageHandler ..')
    #cm.join()
    bm.join()
    om.join()
    if params.backtest==False:
        print('terminate WebSocket Connector ..')
        p1.terminate()
    while not q_execution.empty():
        q_execution.get(timeout=0.1)
    while not q_book.empty():
        q_book.get(timeout=0.1)
    print('end')
