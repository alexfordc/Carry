from tornado.options import options, define, parse_command_line  
from django.core.wsgi import get_wsgi_application  
import tornado.httpserver  
import tornado.ioloop  
import tornado.web
from tornado.web import asynchronous
import tornado.wsgi
import tornado.websocket
import os, sys
import datetime
import time
from django.core.cache import cache
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import zmq
from zmq import Context

from mysite import HSD


def GetRealTimeData(times,price,amount):
    '''得到推送点数据'''
    amount=amount
    is_time=cache.get('is_time')
    objArr = cache.get("objArr")
    objArr = objArr if objArr else [times*1000,price,price,price,price,0]
    if is_time and int(times/60)==int(is_time/60): #若不满一分钟,修改数据
        objArr = [
                    times*1000, #时间
                    objArr[1], #开盘价
                    price if objArr[2]<price else objArr[2], #高
                    price if objArr[3]>price else objArr[3], #低
                    price, #收盘价
                    amount+objArr[5] #量
                ]
        cache.set("objArr",objArr,60)
    else:
        objArr = [
                    times*1000, #时间
                    price, #开盘价
                    price, #高
                    price, #低
                    price, #收盘价
                    amount #量
                ]
        cache.set('is_time',times,60)
        cache.set("objArr",objArr,60)

#@csrf_exempt #取消csrf验证
#def getkline():



SITE_ROOT = os.path.dirname(os.getcwd())   
PROJECT_NAME = os.path.basename(os.getcwd())  
  
sys.path.append( SITE_ROOT )  
os.environ['DJANGO_SETTINGS_MODULE'] = PROJECT_NAME + '.settings'  

define('port', type=int, default=8000)



class WebSocketHandler(tornado.websocket.WebSocketHandler):
    socket_handlers = set()
    zbjs=HSD.Zbjs().main()
    zs=zbjs.send(None)
    def check_origin(self, origin):
        return True

    def open(self):
        WebSocketHandler.socket_handlers.add(self)

    @asynchronous
    def on_message(self, message):
        tcp=HSD.get_tcp()
        poller = zmq.Poller()
        ctx1 = Context()
        sub_socket = ctx1.socket(zmq.SUB)
        sub_socket.connect('tcp://{}:6868'.format(tcp))
        sub_socket.setsockopt_unicode(zmq.SUBSCRIBE, '')
        poller.register(sub_socket, zmq.POLLIN)

        while 1:  # 循环推送数据
            for handler in WebSocketHandler.socket_handlers:
                ticker=sub_socket.recv_pyobj()
                this_time=ticker.TickerTime
                objArr = cache.get("objArr")
                times,opens,high,low,close,vol=objArr if objArr else (ticker.TickerTime*1000,ticker.Price,ticker.Price,ticker.Price,ticker.Price,ticker.Qty)
                GetRealTimeData(ticker.TickerTime,ticker.Price,ticker.Qty)
                #print(times,opens,high,low,close)
                self.zs=0
                if time.localtime(this_time).tm_min!=time.localtime(times/1000).tm_min:
                    tm=time.localtime(times/1000)
                    tm=datetime.datetime(tm.tm_year,tm.tm_mon,tm.tm_mday,tm.tm_hour,tm.tm_min)
                    self.zs=self.zbjs.send((tm,opens,high,low,close))
                    self.zs=self.zs[tm]['datetimes'][-1][1] if self.zs[tm]['datetimes'] else 0
                if this_time*1000!=times:
                    data={'times':str(times),'opens':str(opens),'high':str(high),'low':str(low),'close':str(close),'vol':str(vol),'zs':str(self.zs)}
                    data=json.dumps(data).encode()
                    handler.write_message(data)
                #time.sleep(1)

    def on_close(self):
        pass



def main():  
    tornado.options.parse_command_line()  
  
    wsgi_app = tornado.wsgi.WSGIContainer(  
        get_wsgi_application())

    settings = {
        'template_path': 'templates',
        'static_path': 'static',
        'static_url_prefix': '/static/',
    }
    twf=tornado.web.FallbackHandler
    dtf=dict(fallback=wsgi_app)
    tornado_app = tornado.web.Application(  
        [
            (r'/', twf, dtf),
            (r'/admin/(.*)', twf, dtf),
            (r'/stockData/(.*)', twf, dtf),
            (r'/stockDatas/(.*)', twf, dtf),
            (r'/min/(.*)', twf, dtf),
            (r'/getData/(.*)', twf, dtf),
            (r'/zt/(.*)', twf, dtf),
            (r'/zx/(.*)', twf, dtf),
            (r'/tj/(.*)', twf, dtf),
            (r'/ts/(.*)', twf, dtf),
            (r'/zhutu2/(.*)', twf, dtf),
            (r'/kline/(.*)', twf, dtf),
            (r'/getkline/(.*)', twf, dtf),
            (r'/zhangting/(.*)', twf, dtf),
            (r'/moni/(.*)', twf, dtf),
            (r'/getwebsocket', WebSocketHandler),
        ],**settings)
  
    server = tornado.httpserver.HTTPServer(tornado_app)  
    server.listen(options.port)  
    tornado.ioloop.IOLoop.instance().start()  
  
if __name__ == '__main__':  
        main()  
