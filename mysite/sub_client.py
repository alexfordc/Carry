import zmq
from zmq import Context
from datetime import datetime
from threading import Thread
from mysite import HSD

tcp=HSD.get_tcp() # IP地址
poller = zmq.Poller()
ctx1 = Context()
ticker_sub_socket = ctx1.socket(zmq.SUB)
ticker_sub_socket.connect('tcp://{}:6868'.format(tcp))
ticker_sub_socket.setsockopt_unicode(zmq.SUBSCRIBE, '')
poller.register(ticker_sub_socket, zmq.POLLIN)

ticker_sub_socket.setsockopt_unicode(zmq.SUBSCRIBE, '')
poller.register(ticker_sub_socket, zmq.POLLIN)
ctx3 = Context()
req_price_socket = ctx3.socket(zmq.REQ)
req_price_socket.connect('tcp://{}:6870'.format(tcp))
ctx4 = Context()
handle_socket = ctx4.socket(zmq.REQ)
handle_socket.connect('tcp://{}:6666'.format(tcp))  #237

class sub_ticker:

    def __init__(self, prodcode):
        self._prodcode = prodcode
        self._is_active = False
        self._is_sub = False

    def _run(self, func):
        while self._is_active:
            ticker = ticker_sub_socket.recv_pyobj()
            print(ticker)
            if ticker.ProdCode.decode() == self._prodcode:
                func(ticker)

    def __call__(self, func):
        self._func = func
        return self

    def start(self):
        if self._is_active == False:
            self._is_active = True
            self._thread = Thread(target=self._run, args=(self._func,))
            self._thread.setDaemon(True)
            self._thread.start()


    def stop(self):
        self._is_active = False
        # self._thread.join()

    def sub(self):
        self.handle_socket.send_multipart([b'sub_ticker', self._prodcode.encode()])
        self._is_sub = True
        print(handle_socket.recv_string())

    def unsub(self):
        handle_socket.send_multipart([b'unsub_ticker', self._prodcode.encode()])
        self._is_sub = False
        print(handle_socket.recv_string())


def getTickData():
    global ticker_sub_socket
    #return ticker_sub_socket
    while True:
        try:
            ticker = ticker_sub_socket.recv_pyobj()
            yield ticker#.TickerTime,ticker.Price,ticker.Qty

        except Exception as exc:
            print(exc)

if __name__ == '__main__':
    for i,j,k in getTickData():
        print(i,j,k)
