import time 
import datetime
import requests
import json
import liquidtap
import jwt
import os
import datetime
from pprint import pprint
import ccxt
from dataclasses import dataclass
import sys

token='token'
secret='secret'
try:
    current_dir=os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(current_dir,'setting.csv')) as f:
        ary=f.readline().split(',')
        ary=f.readline().split(',')
        token=ary[0]
        secret=ary[1]
except:
    pass

exchange=ccxt.liquid({'apiKey':token,'secret':secret})

def get_book(pair='BTC/JPY'):
    while True:
        yield  exchange.fetch_order_book(pair)


def get_asset():
    while True:
        try:
            value = exchange.fetch_balance()
            break
        except Exception as e:
            print(e,sys._getframe().f_code.co_name)
            time.sleep(1)
    return value

# JPY残高を参照する
def get_balance():
    return float(get_asset()['info']['fiat_accounts'][0]['balance'])


# JPY証拠金を参照する
def get_free_balance(timestamp=None):
    path = '/accounts/JPY'
    query = ''
    while True:
        try:
            url = 'https://api.liquid.com' + path + query
            if timestamp is None:timestamp = datetime.datetime.now().timestamp()
            payload = {
                "path": path,
                "nonce": timestamp,
                "token_id": token
            }
            signature = jwt.encode(payload, secret, algorithm='HS256')
            headers = {
                'X-Quoine-API-Version': '2',
                'X-Quoine-Auth': signature,
                'Content-Type' : 'application/json'
            }
            res = requests.get(url, headers=headers)
            data=json.loads(res.text)
            ret=data['free_balance']
            break
        except Exception as e:
            print(e,sys._getframe().f_code.co_name)
            time.sleep(1)
    return ret
"""
order情報
{
'id': order id,
'status': open or filled or closed, 
'filled': filled size,
'remaining': remaining,
'amount': order size, 
'price': order price,
'side':sell or buy,
'indicate':,
'result':,
'return':,
'timestamp':
}
"""
# 注文関係の関数

# 関数内で作成する注文情報の項目
order_col={'id', 'status', 'filled', 'remaining', 'amount', 'price', 'side'}

# 成行注文する関数
def market(side, size,pair='BTC/JPY'):
    while True:
        try:
            value = exchange.create_order(pair, type = 'market', side = side, amount = size)
            break
        except Exception as e:
            print(e,sys._getframe().f_code.co_name)
            time.sleep(1)
    ret={}
    for key in order_col:
        ret[key]=value[key]
    return ret

# 指値注文する関数
def limit(side, size, price,pair='BTC/JPY'):
    while True:
        try:
            value = exchange.create_order(pair, type = 'limit', side = side, amount = size, price = price)
            break
        except Exception as e:
            print(e,sys._getframe().f_code.co_name)
            time.sleep(1)
    # return value
    ret={}
    for key in order_col:
        ret[key]=value[key]
    return ret

# 指値注文する関数（レバレッジ）
def limit_leverage_pool(params):
    side,size,price,timestamp,leverage_level=params
    return limit_leverage(side,size,price,timestamp,leverage_level)

# 指値注文する関数（レバレッジ）
def limit_leverage(side,size,price,timestamp=None,leverage_level=2):
    path = '/orders/'
    query = ''
    url = 'https://api.liquid.com' + path + query
    if timestamp is None:timestamp = datetime.datetime.now().timestamp()
    payload = {
        "path": path,
        #"nonce": timestamp,
        "token_id": token
    }
    signature = jwt.encode(payload, secret, algorithm='HS256')
    headers = {
        'X-Quoine-API-Version': '2',
        'X-Quoine-Auth': signature,
        'Content-Type' : 'application/json'
    }
    data = {
        "order":{
        "order_type":"limit",
        "margin_type":"cross",
        "product_id":5,
        "side":side,
        "quantity":size,
        "price":price,
        "leverage_level":leverage_level,
        "funding_currency":'JPY',
        "order_direction":'netout',
        "client_order_id": timestamp
        }
    }
    json_data = json.dumps(data)

    while True:
        try:
            res = requests.post(url, headers=headers, data=json_data)
            value = json.loads(res.text)
            if 'id' not in value:
                print(value)
                timestamp = datetime.datetime.now().timestamp()
                continue
            break
        except Exception as e:
            print(e,sys._getframe().f_code.co_name)
            time.sleep(1)
    ret={'id':value['id'], 'status':'open',  # liquid上では注文状況はlive,filled,canceledだが、ccxtのopen,closed,canceledに合わせる
        'filled':0., 'remaining':size,
        'amount':size,'price':price,'side':side}
    return ret

def edit_order(order_id,price=None,size=None,timestamp=None):
    path = f'/orders/{order_id}'
    query = ''
    while True:
        try:
            url = 'https://api.liquid.com' + path + query
            if timestamp is None:timestamp = datetime.datetime.now().timestamp()
            payload = {
                "path": path,
                "nonce": timestamp,
                "token_id": token
            }
            signature = jwt.encode(payload, secret, algorithm='HS256')
            headers = {
                'X-Quoine-API-Version': '2',
                'X-Quoine-Auth': signature,
                'Content-Type' : 'application/json'
            }
            data={}
            if size is not None:
                data['quantity']=size
            if price is not None:
                data['price']=price
            json_data = json.dumps(data)

            res = requests.put(url, headers=headers, data=json_data)
            value = json.loads(res.text)
            break
        except Exception as e:
            print(e,res,sys._getframe().f_code.co_name)
            timestamp+=1
            time.sleep(1)
    if 'id' not in value:return None # 約定済み
    ret={'id':value['id'], 'status':'open',  # liquid上では注文状況はlive,filled,canceledだが、ccxtのopen,closed,canceledに合わせる
        'filled':0., 'remaining':size,
        'amount':size,'price':price,'side':value['side']}
    return ret

# 注文をキャンセルする関数
def cancel_order(order_id,pair='BTC/JPY'):
    try:
        value=exchange.cancelOrder(symbol = pair, id = order_id)
    except Exception as e:
        # 指値が約定していた(=キャンセルが通らなかった)場合、Noneを返す。
        return None
    ret={}
    for key in order_col:
        ret[key]=value[key]
    return ret


# 注文を確認
def get_orders(status=None,pair='BTC/JPY'):
    orders=[]
    if status is None:
        orders=exchange.fetch_orders(symbol=pair,params = { "product_code" : pair })
    elif status=='open':
        orders=exchange.fetch_open_orders(symbol=pair,params = { "product_code" : pair })
    elif status=='closed':
        orders=exchange.fetch_closed_orders(symbol=pair,params = { "product_code" : pair })
    ret=[]
    for order in orders:
        d={}
        for key in order_col:
            d[key]=order[key]
        ret.append(d.copy())
    return ret


# 指定した注文の最新状態を取得する関数
def get_order(order_id):
    value=None
    while True:
        try:
            values = get_orders()
            for v in values:
                if v['id']==order_id:
                    value=v
                    break
            break
        except Exception as e:
            print(e,sys._getframe().f_code.co_name)
            time.sleep(1)
    if value is None:return None # 指定した注文が存在しない。
    ret={}
    for key in order_col:
        ret[key]=value[key]
    return ret


# 指定した注文の最新状態を取得する関数
def get_some_orders(order_ids):
    value=[]
    while True:
        try:
            values = get_orders()
            for v in values:
                if v['id'] in order_ids:
                    value.append(v)
            break
        except Exception as e:
            print(e,sys._getframe().f_code.co_name)
            time.sleep(1)

    if len(value)==0:return None # 指定した注文が存在しない。
    return value

# ポジション関係の関数

# オープンポジション、クローズポジションの各種情報を取得出来ます。
def get_trades(status=None):
        path = '/trades/'
        query = ''
        url = 'https://api.liquid.com' + path + query
        timestamp = datetime.datetime.now().timestamp()
        payload = {
            "path": path,
            "nonce": timestamp,
            "token_id": token
        }
        signature = jwt.encode(payload, secret, algorithm='HS256')
        headers = {
            'X-Quoine-API-Version': '2',
            'X-Quoine-Auth': signature,
            'Content-Type' : 'application/json'
        }
        if status is None:
            res = requests.get(url, headers=headers)
            data = json.loads(res.text)
        else:
            data = {
                'status':status
            }
            json_data = json.dumps(data)
            res = requests.get(url, headers=headers, data=json_data)
            data = json.loads(res.text)
        return data['models']
# 指定したポジションを取得する
def get_trade_status(trade_id):
    values=get_trades()
    for v in values:
        if v['id']==trade_id:
            return v
    return None # 指定したポジションがない

# 指定したidのポジションをクローズする。
def position_close(trade_id):

    path = f'/trades/{trade_id}/close/'
    query = ''

    url = 'https://api.liquid.com' + path + query
    timestamp = datetime.datetime.now().timestamp()
    payload = {
        "path": path,
        "nonce": timestamp,
        "token_id": token
    }
    signature = jwt.encode(payload, secret, algorithm='HS256')
    headers = {
        'X-Quoine-API-Version': '2',
        'X-Quoine-Auth': signature,
        'Content-Type' : 'application/json'
    }

    res = requests.put(url, headers=headers)
    return json.loads(res.text)

# 全ポジションを決済
def position_close_all():
    path = f'/trades/close_all/'
    query = ''
    url = 'https://api.liquid.com' + path + query
    timestamp = datetime.datetime.now().timestamp()
    payload = {
        "path": path,
        "nonce": timestamp,
        "token_id": token
    }
    signature = jwt.encode(payload, secret, algorithm='HS256')
    headers = {
        'X-Quoine-API-Version': '2',
        'X-Quoine-Auth': signature,
        'Content-Type' : 'application/json'
    }
    res = requests.put(url, headers=headers)
    return json.loads(res.text)


# 約定情報を取得
def get_executions(product_id=5):
    path = f'/executions/me/'
    query=''
    url = 'https://api.liquid.com' + path + query
    timestamp = datetime.datetime.now().timestamp()
    payload = {
        "path": path,
        "nonce": timestamp,
        "token_id": token
    }
    signature = jwt.encode(payload, secret, algorithm='HS256')
    headers = {
        'Content-Type' : 'application/json'
    }
    data={'product_id':str(product_id)}
    data=json.dumps(data)
    res = requests.get(url, headers=headers,data=data)
    return json.loads(res.text)


# 約定情報（詳細。買注文idと売注文idがある）
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

def executions_to_ohlcv(q1,q2):
    ohlcv=[datetime.datetime.now().timestamp(),-1,-1,-1,-1,-1,0,0]
    while True:
        v=q1.get()
        if ohlcv[1]==-1:
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
            ohlcv[2]=max(ohlcv[2],v['price'])
            ohlcv[3]=min(ohlcv[3],v['price'])
            ohlcv[4]=v['price']
            ohlcv[5]+=v['price']*v['quantity']
            if v['taker_side']=='sell':
                ohlcv[6]+=v['price']*v['quantity']
            else:
                ohlcv[7]+=v['price']*v['quantity']
            if float(v['timestamp'])-ohlcv[0]>=4.5:
                ohlcv[0]=float(v['timestamp'])
                q2.put(ohlcv[:])
                ohlcv[1]=-1

# test
if __name__=='__main__':
    pass