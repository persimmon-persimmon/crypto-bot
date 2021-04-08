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
class Paras:
    allow_dd:float=0.8 # 許容ドローダウン
    lot:float=0.0001 # 一度の注文量
    leverage_level:int=2
    loop_num:int=10
    append_loop_num:int=30
    
    # モデル変数。sigmaとalphaは変動する。ganmaは自分で設定する。???
    sigma:int=1500      # 値動きボラティリティσのブラウン運動 ???
    ganma:float=0.7     # リスク回避度γ   大きいほどリスクを回避する。??
                        # γ大：Soffsetが在庫を解消する方向に設定される。値幅(δask+bid)が大きくなる。???
    alpha:float=100     # 注文の強さを示す値。大きいほど強い。
                        # α大：板に対して成行が多い。値動きが活発になると予想される。
                        # α小：板に対して成行が少ない。値動きが少ないと予想される。

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
    q_user_order=Queue()
    mh=MessageHandler(q_book,q_execution,q1=None,n=1)
    p1=Process(target=channel_connector,args=(q_book,q_execution,q_user_order))
    p1.start()
    om=OrderManager(q_user_order)

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

    loop_count=0

    # 現在のエントリー状態
    now_entry=None

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
        if loop_count>paras.loop_num and om.stock==0:break
        if loop_count>paras.append_loop_num:break

        # 指値を入れる処理

        # 在庫を指値位置を決める
        # 最良価格の中央値
        st=(mh.book_newest['asks'][0][0]+mh.book_newest['bids'][0][0])/2
        # Soffset
        so=st-paras.ganma*paras.sigma*om.stock
        ask_bid=paras.ganma*paras.sigma+1/paras.ganma*log(1+paras.ganma/paras.alpha)
        ask_price=int(so+ask_bid/2)
        bid_price=int(so-ask_bid/2)
        # 現在の注文状況
        s={}
        s['loop_count']=loop_count
        s['stock']=om.stock
        s['sell executed ratio']=om.sell_executed_quantity/om.sell_order_quantity if om.sell_order_quantity else None
        s['buy executed ratio']=om.buy_executed_quantity/om.buy_order_quantity if om.buy_order_quantity else None
        s['return(calcrated stock)']=om.jpy_delta+om.stock*st
        print(s)
        if loop_count<=paras.loop_num:
            args_ary=[] # buyとsellの注文引数 #side,size,price,timestamp,leverage_level
            args_ary.append(('sell',paras.lot,ask_price,timestamp,paras.leverage_level))
            timestamp+=1
            args_ary.append(('buy',paras.lot,bid_price,timestamp,paras.leverage_level))
            timestamp+=1
            value=om.limit_order(args_ary)
            now_entry={'timestamp':datetime.datetime.now().timestamp(),'sell':ask_price,'buy':bid_price,'delta':ask_price-bid_price}
            q_log.put(now_entry)
        
        #(2).5秒待つ
        time.sleep(5)



    print('return:',om.jpy_delta,'stock:',om.stock,'return(calcrated stock):',om.jpy_delta+om.stock*st)

    print('terminate MessageHandler ..')
    mh.join()
    om.join()
    print('terminate WebSocket Connector ..')
    p1.terminate()
    print('terminate Log Writer ..')
    p2.terminate()

"""
「マーケット戦略の理論」
http://we.love-profit.com/entry/2018/02/12/113916

u:効用関数
u=E[-exp(-γ*(Ct+Qt*St))]←これを最大化するδaskとδbidを求める。
γ：リスク回避度
Ct：時刻t時点のキャッシュ
Qt：時刻t時点の在庫
St：時刻t時点の中央値

値動き
分散σ^2のブラウン運動で近似
dSt=σdWt
σ：ボラティリティと解釈できる

板の厚み
exp関数で近似（式不明）

α：注文の強さ。ここでは一定と仮定
λ：板の厚み関数。buyとsellで注文の強さ、板の強さを同じと仮定している。
δ：中央値からの価格差。中央値に近いほど厚みは大きくなる。
A ：定数？ λ(0)=Aなので、中央値の板の厚みと考えてよい？

λ(δ)=A * exp(-αδ)

下では「α小→注文弱い」「α大→注文強い」と言っている。
α小→λ大→2/λ小→δask+bid小
α大→λ小→2/λ大→δask+bid大
α大だと、中央値から離れると急激に板の厚みが薄くなる。
α小だと、中央値から離れても板の厚みはあまり変わらない。

解
(1)δask+bid=γ*σ^2*(T-t) + 2/γ*ln(1+γ/α)
(2)Soffset=-γ*σ^2*(T-t) * Qt
ask値=ST+Soffset+δask+bid/2
bid値=ST+Soffset-δask+bid/2

(1)について
・第1項
残存ボラティリティ*リスク回避度
残存ボラティリティとは、時点tから満了期間Tまでの残り時間における分散の大きさであり、σ^2×（T-t）で表されます。
つまりリスク回避度が大きいもしくは残存ボラが大きい場合、指値の幅を広げて約定する注文の量を抑制します。
ショックの発生によってボラティリティが極端に大きくなった場合、指値の幅は無限大に近づき、このときには指値は全く約定しなくなります。


・第2項
注文の強さから決まる値です。
この値は、注文の強さ（板の厚みと成行注文量）に対して約定確率×値幅の期待値が最も高くなるポイントに設定されます。
この項のリスク回避度への依存性は低く（分子と分母で打ち消しあうため）、注文の強さのパラメータαが支配的となります。
注文の強さが強い場合（αが大、板に対して成行が多い場合）、Stから離れた板まで約定する確率が高く、このときは指値の位置をStより遠ざけます。
逆に注文の強さが弱い場合（αが小、板に対して成行が少ない場合）、Stから離れた板は約定する確率が低く、このときは指値の位置をStへ近づけます。
なお、上記の式はAsk側とBid側の注文の強さが同じものとして式を簡略化していることにも注意してください。

まとめると、
・リスク回避度を大きく取る場合は指値の幅を広げる
・（残存）ボラティリティが大きいときは指値の幅を広げる
・注文の強さが強い（板に対して成行が多い）場合、指値の幅を広げる


(2)中央値からのオフセットSoffset

この項は、－リスク回避度×残存ボラティリティ×在庫量となっています。
考え方としては、ある在庫qtを保有しているとき、それを解消する方向に指値が約定するようオフセット量が調整されます。
このとき、リスク回避度を大きく取る場合にはオフセット量が大きくなり、積極的に在庫を解消するような挙動となります。
また、値動きのボラティリティが大きくなれば、それに応じてオフセット量が大きくなり在庫の解消が促進されます。

まとめると、
・在庫を解消するよう、オフセットを加算する
・リスク回避度を大きく取る場合、オフセット量は大きくなる
・（残存）ボラティリティが大きいときは、オフセット量は大きくなる

考えたこと
・ボラティリティと板の厚みは関係してそう。板が薄く注文が強いときはボラティリティは大きくなる。
・buyとsellで異なる板の厚み、注文強さを仮定したらどうなるか。buyとsellの差分はオフセットに影響する？
・板は必ずしも中央値が一番厚いわけではない。その場合、EXP関数の近似は正しいのか。
・δask+bidの第2項の2/λはどう解釈すればいいのか。λ(δ)のδは？ー原文見てみる。→原文ではλではなくγとなっている。
・

bookとohlcvを収集するPG、ohlcvができたときのbook_newestを対応させているが、
タイムスタンプで対応させる方がいい。ohlcvができるのは約定ができたタイミングだけ。約定がない間のbookの動きがわからないため。
どうやってデータを取るか
ローソク：今までと同様
板：channel_price_laddersで送られてくるデータのタイムスタンプをみて判断する。
timestampをキーとするdook_dictを作り、ローソク足ができたら、そこから対応するbookを取る。
古いものをpop
→データ作成中は必ずしも板配列とローソク配列の個数は一致しない、という状態になる。

データ作成中はこれでよくて、bot稼働時はどうすべきか。
→配列の最新要素のタイムスタンプを確認し、必要に応じて配列を補って使用する。

注文の約定状況で正確な在庫数を把握するのは不可能。最大20件しかとれないため。
get_trades('open')を使えばもう少し撮れるが、これも最大20件なので、在庫が増えると無理になる。
発注した価格ごとに発注量を記録。

"""
