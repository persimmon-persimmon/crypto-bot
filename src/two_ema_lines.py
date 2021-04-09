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
import csv

def channel_connector(q_book=None,q_execution=None,q_user_order=None,backtest=False):
    if q_execution is not None:
        t1=Thread(target=virtual_channel_execution_details_cash,args=(q_execution,))
        t1.start()
    if q_book is not None:
        t2=Thread(target=channel_price_ladders,args=(q_book,))
        t2.start()
    if q_user_order is not None:
        t3=Thread(target=channel_user_order,args=(q_user_order,))
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
    lot:float=0.01 # 一度の注文量
    leverage_level:int=2
    loop_num:int=200
    append_loop_num:int=300
    timescale:int=1*60
    ema_num0:int=5  # 比較的、最近の値を重視
    ema_num1:int=16  # 比較的、昔の値も重視
    ema_alpha0:float=2/(ema_num0+1)
    ema_alpha1:float=2/(ema_num1+1)
    backtest:bool=True


"""
処理フロー

"""
if __name__=='__main__':
    params=Params()
    current_dir=os.path.dirname(__file__)
    with open(os.path.join(current_dir,'graph.csv'),'w') as f:
        writer=csv.writer(f)
        writer.writerow(['datetime','price','ema0','ema1','sell_volume','buy_volume','entry','asset','stock'])
    # web socket接続プロセス
    q_execution=Queue()
    #q_user_order=Queue()
    q_user_order=None
    q1=Queue()
    cm=CandleManager(q_execution,q1,10,params.timescale,params.backtest)
    om=OrderManager(q_user_order)

    p1=Process(target=channel_connector,args=(None,q_execution,q_user_order))
    p1.start()

    # EMAを作成
    # 過去のローソク足を取る。
    ary=[] #あとで実装
    ema0=Ema(params.ema_alpha0,ary)
    ema1=Ema(params.ema_alpha1,ary)

    # log書込プロセス
    q_log=Queue()
    p2=Process(target=log_writer,args=(q_log,))
    p2.start()
    q_log.put('start')
    if params.backtest:
        now_asset=0
    else:
        now_asset=get_free_balance()
    max_asset=now_asset

    loop_count=0

    # 現在のエントリー状態
    now_entry={'side':None,'best_price':0}
    stock=0
    ret_ary=[]
    while True:
        loop_count+=1
        timestamp=datetime.datetime.now().timestamp()

        # 何回かに一回行う処理
        if loop_count%10==0:
            if params.backtest:
                pass
            else:
                now_asset=get_free_balance()
            timestamp+=1
            #if now_asset/max_asset<params.allow_dd:
            #    # 注文キャンセル、ポジション決済
            #    # リターンを記録
            #    break
            max_asset=max(now_asset,max_asset)

        # テスト稼働用。一定数のループでbreak
        #if loop_count>params.loop_num and om.stock==0:break
        #if loop_count>params.append_loop_num:break

        ohlcv=q1.get()
        # ohlcv=[timestamp,datetime_jst,open,high,low,close,volume,buy_volume,sell_volume,first_execution_id,last_execution_id]
        ema0.add(ohlcv['close'])
        ema1.add(ohlcv['close'])
        if len(ema0.ema_ary)<5:continue
        if ema0.ema_ary[-1]>ema1.ema_ary[-1] and ema0.ema_ary[-2]<ema1.ema_ary[-2]:
            # 上昇トレンド発生
            now_asset+=stock*ohlcv['close']
            stock=0
            now_entry['side']='None'
            now_entry['price']=0
            now_entry['best_return']=0
            if abs(ema0.ema_ary[-2]-ema0.ema_ary[-1])>1.5*abs(ema1.ema_ary[-2]-ema1.ema_ary[-1]):
                
                stock+=params.lot
                now_asset-=ohlcv['close']*params.lot
                now_entry['side']='buy'
                now_entry['price']=ohlcv['close']
                now_entry['best_return']=0
                d={'price':ohlcv['close']
                    ,'date':str(ohlcv['datetime'])
                    ,'asset':now_asset+stock*ohlcv['close']
                    ,'stock':stock
                    ,'entry':now_entry
                }

                print('LONG!!',d)
            
        elif ema0.ema_ary[-1]<ema1.ema_ary[-1] and ema0.ema_ary[-2]>ema1.ema_ary[-2]:
            # 下降トレンド発生
            
            now_asset+=stock*ohlcv['close']
            stock=0
            now_entry['side']='None'
            now_entry['price']=0
            now_entry['best_return']=0
            #om.order_cancel_all('buy')
            #om.position_close_all('long')
            #if now_entry!='sell':
            #    om.market(params.lot,'sell')
            if abs(ema0.ema_ary[-2]-ema0.ema_ary[-1])>1.5*abs(ema1.ema_ary[-2]-ema1.ema_ary[-1]):
                now_entry['side']='sell'
                now_entry['price']=ohlcv['close']
                now_entry['best_return']=0
                stock-=params.lot
                now_asset+=ohlcv['close']*params.lot
                d={'price':ohlcv['close']
                    ,'date':str(ohlcv['datetime'])
                    ,'asset':now_asset+stock*ohlcv['close']
                    ,'stock':stock
                    ,'entry':now_entry
                }
                print('SHORT!!',d)
        else:
            if now_entry['side']=='buy':
                if now_entry['best_return']*0.95>now_entry['price']-ohlcv['close']:
                    now_asset+=stock*ohlcv['close']
                    stock=0
                    now_entry['side']='None'
                    now_entry['best_price']=0
                else:
                    now_entry['best_return']=max(now_entry['best_return'],now_entry['price']-ohlcv['close'])
            if now_entry['side']=='sell':
                if now_entry['best_return']*0.95>-now_entry['price']+ohlcv['close']:
                    now_asset+=stock*ohlcv['close']
                    stock=0
                    now_entry['side']='None'
                    now_entry['best_price']=0
                else:
                    now_entry['best_return']=max(now_entry['best_return'],-now_entry['price']+ohlcv['close'])
            
            d={'price':ohlcv['close']
                ,'date':str(ohlcv['datetime'])
                ,'asset':now_asset+stock*ohlcv['close']
                ,'stock':stock
                ,'entry':now_entry
            }

            print('None',d)
        ary=[ohlcv['datetime'],ohlcv['close'],ema0.ema_ary[-1],ema1.ema_ary[-1],ohlcv['sell_volume'],ohlcv['buy_volume'],now_entry['side'],now_asset+stock*ohlcv['close'],stock]
        with open(os.path.join(current_dir,'graph.csv'),'a') as f:
            writer=csv.writer(f)
            writer.writerow(ary)
        
        #(2).5秒待つ
        if params.backtest==False:time.sleep(5)


    #print('return:',om.jpy_delta,'stock:',om.stock,'return(calcrated stock):',om.jpy_delta+om.stock*st)

    print('terminate MessageHandler ..')
    #mh.join()
    om.join()
    print('terminate WebSocket Connector ..')
    p1.terminate()
    print('terminate Log Writer ..')
    p2.terminate()
