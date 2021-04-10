from lib.api_lib import *
from lib.channel_lib import *
from lib.utills import *
import os
import json

if __name__=='__main__':
    current_dir=os.path.dirname(__file__)
    with open(os.path.join(current_dir,'dat/book_20210409_22.dat'),'r') as f:
        datas=f.readlines()
    for data in datas:
        data=json.loads(data)
        data['asks']=[[float(x),float(y)] for x,y in data['asks']]
        data['bids']=[[float(x),float(y)] for x,y in data['bids']]
        data['timestamp']=float(data['timestamp'])
        with open(os.path.join(current_dir,'book_20210409_22.dat'),'a') as f:
            f.write(json.dumps(data))
            f.write('\n')
