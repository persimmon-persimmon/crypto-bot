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
    loop_num:int=300
    entry_num:int=40
    exit_num:int=18
    ohlcv_secounds:int=10



#処理フロー
#(1).スプレッドが一定以上ならロングとショート両方の指値注文を入れる。
#(2).1秒待つ
#(3).両方約定していれば１に戻る。
#(4).両方未約定でスプレッドが一定以上なら注文の価格をbest_priceに合わせて編集し、2に戻る。
#(5).片方のみ約定なら、未約定の注文の価格をbest_priceに合わせて編集する。これを約定するまで続け、約定すれば(1)に戻る。
#    ただし期待リターンが低い場合、ポジションをクローズして(1)に戻る。

if __name__=='__main__':
    paras=Paras()
    current_dir=os.path.dirname(__file__)

    # web socket接続プロセス
    q_book=Queue()
    q_execution=Queue()
    q1=Queue()
    mh=MessageHandler(q_book,q_execution,q1=q1,his_num=paras.entry_num,n=paras.ohlcv_secounds)
    p1=Process(target=channel_connector,args=(q_book,q_execution))
    p1.start()

    # log書込プロセス
    q_log=Queue()
    p2=Process(target=log_writer,args=(q_log,))
    p2.start()
    q_log.put('start')

    # 結果を記録する変数
    result_dict={}
    for k in ['long_count','long_return','short_count','short_return']:
        result_dict[k]=0
    long_return_ary=[]
    short_return_ary=[]

    #注文用スレッドプール
    executor=ThreadPoolExecutor(max_workers=4)
    loop_count=0

    # 現在のエントリー状態
    entry_side='no'
    now_entry=None
    from collections import deque
    entry_ary=deque([])
    exit_ary=deque([])
    value=get_free_balance() # 日本金を取得
    max_asset=value

    while True:
        loop_count+=1
        ohlcv=q1.get()
        book=q1.get()
        entry_ary.append(ohlcv[5])
        exit_ary.append(ohlcv[5])
        if len(entry_ary)<paras.entry_num:
            print(f'prepare data.. loop:{loop_count} {datetime.datetime.now()}')
            continue
        long_entry=max(list(entry_ary)[:-1])
        long_exit=min(list(exit_ary)[:-1])
        short_entry=min(list(entry_ary)[:-1])
        short_exit=max(list(exit_ary)[:-1])
        print('indi:',short_entry,short_exit,short_exit,long_entry,' close:',ohlcv[5],' loop:',loop_count)
        if len(entry_ary)>paras.entry_num:entry_ary.popleft()
        if len(exit_ary)>paras.exit_num:exit_ary.popleft()

        # exit判定
        if entry_side=='short' and short_exit<ohlcv[5]:
            values=position_close_all()
            entry_side='no'
            ret=0
            for value in values:
                try:
                    short_return_ary.append(value['pnl'])
                    result_dict['short_return']+=value['pnl']
                    ret+=value['pnl']
                except Exception as e:
                    print(e,value)
            q_log.put(f'short_exit loop:{loop_count}, return:{ret}')
            time.sleep(1)

        elif entry_side=='long' and long_exit>ohlcv[5]:
            values=position_close_all()
            entry_side='no'
            ret=0
            for value in values:
                try:
                    long_return_ary.append(value['pnl'])
                    result_dict['long_return']+=value['pnl']
                    ret+=value['pnl']
                except Exception as e:
                    print(e,value)
            q_log.put(f'long_exit loop:{loop_count}, return:{ret}')
            time.sleep(1)

        # entry判定
        timestamp=datetime.datetime.now().timestamp()+1
        if loop_count>paras.loop_num:
            pass
        elif long_entry<ohlcv[5] and entry_side=='no':
            q_log.put(f'long_entry loop:{loop_count}')
            market_leverage('buy',paras.lot,timestamp)
            result_dict['long_count']+=1
            entry_side='long'
            time.sleep(1)
        elif short_entry>ohlcv[5] and entry_side=='no':
            q_log.put(f'short_entry loop:{loop_count}')
            market_leverage('sell',paras.lot,timestamp)
            result_dict['short_count']+=1
            entry_side='short'
            time.sleep(1)

        # 何回かに一回行う処理
        if loop_count%10==0:
            # 資産を計算し、許容DDを超えていた場合、強制終了する
            # 現状、ここの処理をするとき未約定注文なし、ノーポジションなので日本円のみ取得すればいい。
            if entry_side=='no':
                value=get_free_balance() # 日本金を取得
                now_asset=value
                if now_asset/max_asset<paras.allow_dd:
                    # 注文キャンセル、ポジション決済
                    # リターンを記録
                    break
                max_asset=max(now_asset,max_asset)

        # テスト稼働用。一定数のループでbreak
        if entry_side=='no' and loop_count>paras.loop_num:break


    q_log.put(f'{json.dumps(result_dict)}')
    if long_return_ary:
        return_ary=np.array(long_return_ary)
        q_log.put(f'long total return:{return_ary.sum()}, sharpe ratio={return_ary.mean()/return_ary.var() if return_ary.var() else None}')
    if short_return_ary:
        return_ary=np.array(short_return_ary)
        q_log.put(f'short total return:{return_ary.sum()}, sharpe ratio={return_ary.mean()/return_ary.var() if return_ary.var() else None}')

    print('terminate MessageHandler ..')
    mh.join()
    print('terminate WebSocket Connector ..')
    p1.terminate()
    print('terminate Log Writer ..')
    p2.terminate()
    print('shutdown thread pool ..')
    executor.shutdown()

"""
改善すること
・remainingを考慮する

・現在は一回のエントーで必ず両方決済させるが、残ってもいいので次のエントリーをする。在庫管理。なるべく成行決済を避ける。

・決済ロジックを改善

・指値の値は機械的に算出しているが、値動きを予測し、いい感じのところに指値を置くようにする。

・両方キャンセルする箇所で、タッチの差で約定した場合の処理を入れる。

"""
