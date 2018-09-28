import websockets
import asyncio
import time
import datetime
import json
from zmq import Context
import zmq
# import sys
import redis
# sys.path.append(r'D:\tools\Tools\Carry')

# from mysite import HSD

class RedisPool:
    """ Redis 数据库存取 """
    # _singleton = None
    _conn = None

    # def __new__(cls, *args, **kwargs):
    #     if cls._singleton is None:
    #         cls._singleton = super(RedisPool, cls).__new__(cls)
    #     return cls._singleton

    def __init__(self):
        if self._conn is None:
            self.conn()

    def conn(self):
        """ 连接 Redis 数据库"""
        self._conn = redis.Redis(host='localhost')

    def get(self, key):
        """
        获取键对应的值
        :param key: 键
        :return: 值
        """
        try:
            value = self._conn.get(key)
        except:
            self.conn()
            value = self._conn.get(key)
        return value and json.loads(value)

    def set(self, key, value, expiry=259200):
        """
        写入 Redis 数据库
        :param key: 键
        :param value: 值，可为 python 各种数据类型
        :param expiry: 过期时间，默认3天
        :return: None
        """
        try:
            self._conn.set(key, json.dumps(value))
            self._conn.expire(key, expiry)
        except:
            self.conn()
            self._conn.set(key, json.dumps(value))

    def delete(self, key):
        """
        删除 键
        :param key: 键
        :return:
        """
        try:
            self._conn.delete(key)
        except:
            self.conn()
            self._conn.delete(key)

red = RedisPool()

def GetRealTimeData(times, price, amount):
    '''得到推送点数据'''
    amount = amount
    is_time = red.get('is_time')
    objArr = red.get("objArr")
    objArr = objArr if objArr else [times * 1000, price, price, price, price, 0]
    if is_time and int(times / 60) == int(is_time / 60):  # 若不满一分钟,修改数据
        objArr = [
            times * 1000,  # 时间
            objArr[1],  # 开盘价
            price if objArr[2] < price else objArr[2],  # 高
            price if objArr[3] > price else objArr[3],  # 低
            price,  # 收盘价
            amount + objArr[5]  # 量
        ]
        red.set("objArr", objArr, 60)
    else:
        objArr = [
            times * 1000,  # 时间
            price,  # 开盘价
            price,  # 高
            price,  # 低
            price,  # 收盘价
            amount  # 量
        ]
        red.set('is_time', times, 60)
        red.set("objArr", objArr, 60)



async def hello(websocket, path):
    name = await websocket.recv()
    print(f'A new client: {name}')
    # zbjs = HSD.Zbjs().main()
    # zs = zbjs.send(None)
    tcp = '192.168.2.204' # HSD.get_tcp()
    poller = zmq.Poller()
    ctx1 = Context()
    sub_socket = ctx1.socket(zmq.SUB)
    sub_socket.connect('tcp://{}:6868'.format(tcp))
    sub_socket.setsockopt_unicode(zmq.SUBSCRIBE, '')
    poller.register(sub_socket, zmq.POLLIN)
    for i in range(1000):
        ticker = sub_socket.recv_pyobj()
        this_time = ticker.TickerTime
        objArr = red.get("objArr")
        times, opens, high, low, close, vol = objArr if objArr else (
            ticker.TickerTime * 1000, ticker.Price, ticker.Price, ticker.Price, ticker.Price, ticker.Qty)
        GetRealTimeData(ticker.TickerTime, ticker.Price, ticker.Qty)
        zs = 0
        if time.localtime(this_time).tm_min != time.localtime(times / 1000).tm_min:
            tm = time.localtime(times / 1000)
            tm = datetime.datetime(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min)
            # zs = zbjs.send((tm, opens, high, low, close))
            zs = 0 # zs[tm]['datetimes'][-1][1] if zs[tm]['datetimes'] else 0
        if this_time * 1000 != times:
            data = {'times': str(times), 'opens': str(opens), 'high': str(high), 'low': str(low),
                    'close': str(close), 'vol': str(vol), 'zs': str(zs)}  # ,'_ch':0
            data = json.dumps(data).encode()
            await websocket.send(data)

        # dt = str(datetime.datetime.now())
        # s = json.dumps({'test':f"{name} {i} {dt}"})
        # await websocket.send(s)
        # print(f'send {name} {i} {dt}')
        # time.sleep(0.1)


start_server = websockets.serve(hello, '192.168.2.204', 8008)
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()