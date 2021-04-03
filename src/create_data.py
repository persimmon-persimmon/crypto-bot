import liquidtap
import time
from multiprocessing import Process, Queue
from threading import Thread
import os
import logging
import logging.config
from datetime import datetime, timedelta, timezone

import ccxt
exchange=ccxt.liquid()

# 約定情報（詳細。買注文idと売注文idがある）
import json
def websoket_liquid_execution_details_cash(q):
    def update_callback_executions(data):
        data=json.loads(data)
        # todo:必要な情報のみを選んでputする処理
        q.put(data)
    def on_connect(data):
        tap.pusher.subscribe("executions_cash_btcjpy").bind('created', update_callback_executions)
    tap = liquidtap.Client()
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        time.sleep(10)

from pprint import pprint
def executions_to_ohlcv(q1,q2):
    ohlcv=[datetime.now().timestamp(),-1,-1,-1,-1,-1,0,0]
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

# 
def create_new_file(JST):
    s=datetime.now(JST).strftime('%Y%m%d_%H%M%S')
    surfix=f'_{s}.csv'
    ohlcv_file=f'ohlcv{surfix}'
    ticker_file=f'ticker{surfix}'
    book_file=f'book{surfix}'
    oh=['timestamp','open','high','low','close','volume','taker_sell_volume','taker_buy_volume']
    with open(os.path.join(current_dir,ohlcv_file),'w') as f:
        f.write(','.join(oh))
        f.write('\n')
    th=['timestamp']+['high', 'low', 'bid', 'ask', 'open', 'close', 'last', 'change', 'percentage', 'average', 'baseVolume']
    with open(os.path.join(current_dir,ticker_file),'w') as f:
        f.write(','.join(th))
        f.write('\n')
    bh=['timestamp']
    for i in range(21):
        bh.append(f'ask_price_{i}')
        bh.append(f'ask_quantity_{i}')
    for i in range(21):
        bh.append(f'bid_price_{i}')
        bh.append(f'bid_quantity_{i}')
    with open(os.path.join(current_dir,book_file),'w') as f:
        f.write(','.join(map(str,bh)))
        f.write('\n')
    return ohlcv_file,ticker_file,book_file

# test
if __name__ == "__main__":

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

    # タイムゾーンの生成
    JST = timezone(timedelta(hours=+9), 'JST')

    # プロセス設定
    qe=Queue()
    qc=Queue()
    p1=Process(target=websoket_liquid_execution_details_cash,args=(qe,))
    p2=Process(target=executions_to_ohlcv,args=(qe,qc))
    p1.start()
    p2.start()
    pre_ymd='-1'
    # ticker内の情報で保存するもの
    th=['timestamp']+['high', 'low', 'bid', 'ask', 'open', 'close', 'last', 'change', 'percentage', 'average', 'baseVolume']
    logger.info('start')
    i=0
    while True:
        if i%100==0:
            now_ymd=datetime.now(JST).strftime('%Y%m%d')
            if now_ymd!=pre_ymd:
                ohlcv_file,ticker_file,book_file=create_new_file(JST)
                logger.info(f'new files {ohlcv_file,ticker_file,book_file}')
                i=0
            pre_ymd=now_ymd
            logger.info(f'count {i,now_ymd}')
        i+=1
        # 情報取得
        ohlcv=qc.get()
        book=exchange.fetch_order_book('BTC/JPY')

        # 共通のタイムスタンプを使う
        ohlcv[0]=int(ohlcv[0])
        timestamp=ohlcv[0]

        # データ書き込み
        with open(os.path.join(current_dir,ohlcv_file),'a') as f:
            f.write(','.join([str(x) for x in ohlcv]))
            f.write('\n')
        with open(os.path.join(current_dir,book_file),'a') as f:
            ary=[str(timestamp)]
            for i in range(21):
                ary.append(str(book['asks'][i][0]))
                ary.append(str(book['asks'][i][1]))
            for i in range(21):
                ary.append(str(book['bids'][i][0]))
                ary.append(str(book['bids'][i][1]))
            f.write(','.join(ary))
            f.write('\n')
        

    p1.terminate()
    p2.terminate()
    logger.info('end')
    for h in logger.handlers:
        logger.removeHandler(h)
    with open(os.path.join(current_dir,ohlcv_file),'r') as f:
        ary=f.readlines()[1:]
        pre=float(ary[0].split(',')[0])
        s=[]
        for tmp in ary[1:]:
            s.append(float(tmp.split(',')[0])-pre)
            pre=float(tmp.split(',')[0])
    
"""
nohup sudo python3 create_data_v2.py &

"""
