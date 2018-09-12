import requests
import re
import time
import datetime
import pymysql
import json
import os
import numpy as np
import pandas as pd
import logging
import configparser
import requests
from sklearn.externals import joblib
from collections import Counter
import random
import socket
import redis
import sys
import pymongo
from pyquery import PyQuery
from copy import deepcopy

from mysite.DataIndex import ZB

config = configparser.ConfigParser()
config.read('log\\conf.conf', encoding='utf-8')

logging.basicConfig(
    filename='log\\logging.log',
    filemode='a',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    format='%(filename)s[%(asctime)s][%(levelname)s]：%(message)s'
)

logging = logging

# 昨天收盘指数价格
YESTERDAT_PRICE = (None, None)
# 权重依次最大的股
WEIGHT_BASE = ('腾讯控股', '汇丰控股', '建设银行', '友邦保险', '工商银行', '中国移动', '中国平安', '中国银行', '香港交易所', '长和')
# 影响最大的股
WEIGHT = ['腾讯控股', '汇丰控股', '建设银行', '友邦保险', '工商银行', '中国移动', '中国平安', '中国银行', '香港交易所', '长和']
# 颜色
COLORS = ['#FF0000', '#00FF00', '#003300', '#FF6600', '#FFD700', '#663366', '#066606', '#0000FF', '#FF0080', '#00FFFF',
          '#2E8B57', '#B8860B', '#CD5C5C', '#006400']

# 代码：名称
CODE_NAME = {'hk00700': '腾讯控股', 'hk00005': '汇丰控股', 'hk00939': '建设银行', 'hk01299': '友邦保险', 'hk01398': '工商银行',
             'hk00941': '中国移动', 'hk02318': '中国平安', 'hk03988': '中国银行', 'hk00388': '香港交易所', 'hk00001': '长和',
             'hk00883': '中国海洋石油', 'hk01113': '长实集团', 'hk02628': '中国人寿', 'hk00016': '新鸿基地产', 'hk00027': '银河娱乐',
             'hk00386': '中国石油化工股份', 'hk00002': '中电控股', 'hk00011': '恒生银行', 'hk00823': '领展房产基金', 'hk02388': '中银香港',
             'hk00175': '吉利汽车', 'hk00003': '香港中华煤气', 'hk00857': '中国石油股份', 'hk02018': '瑞声科技', 'hk01928': '金沙中国有限公司',
             'hk00688': '中国海外发展', 'hk02007': '碧桂园', 'hk02382': '舜宇光学科技', 'hk00006': '电能实业', 'hk00288': '万洲国际',
             'hk01109': '华润置地', 'hk01088': '中国神华', 'hk00066': '港铁公司', 'hk00762': '中国联通', 'hk02319': '蒙牛乳业',
             'hk00017': '新世界发展', 'hk00267': '中信股份', 'hk01997': '九龙仓置业', 'hk00012': '恒基地产', 'hk01044': '恒安国际',
             'hk03328': '交通银行', 'hk00023': '东亚银行', 'hk00083': '信和置业', 'hk01038': '长江基建集团', 'hk00151': '中国旺旺',
             'hk00019': '太古股份公司', 'hk00101': '恒隆地产', 'hk00004': '九龙仓集团', 'hk00836': '华润电力', 'hk00992': '联想集团',
             'hk00144': '招商局港口'}
# 代码：股本，亿
CODE_EQUITY = {'hk00005': 203.78, 'hk00011': 19.12, 'hk00023': 27.67, 'hk00388': 12.40, 'hk00939': 2404.17,
               'hk01299': 120.75, 'hk01398': 867.94, 'hk02318': 74.48, 'hk02388': 105.73, 'hk02628': 74.41,
               'hk03328': 350.12, 'hk03988': 836.22, 'hk00002': 25.26, 'hk00003': 139.88, 'hk00006': 21.34,
               'hk00836': 48.10, 'hk01038': 26.51, 'hk00004': 30.44, 'hk00012': 40.01, 'hk00016': 28.97,
               'hk00017': 101.00, 'hk00083': 64.48, 'hk00101': 44.98, 'hk00688': 109.56, 'hk00823': 8.938,
               'hk01109': 69.31, 'hk01113': 36.97, 'hk01997': 30.36, 'hk02007': 217.40, 'hk00001': 38.58,
               'hk00019': 9.05, 'hk00027': 43.14, 'hk00066': 60.08, 'hk00144': 32.78, 'hk00151': 124.62,
               'hk00175': 89.73, 'hk00267': 290.90, 'hk00288': 146.75, 'hk00386': 255.13, 'hk00700': 94.99,
               'hk00762': 305.98, 'hk00857': 210.99, 'hk00883': 446.47, 'hk00941': 204.75, 'hk00992': 120.15,
               'hk01044': 12.06, 'hk01088': 33.99, 'hk01928': 80.76, 'hk02018': 12.22, 'hk02319': 39.27,
               'hk02382': 10.97}

# 代码：比重上限系数，%
CODE_WEIGHT = {'hk00001': 1.0, 'hk00002': 1.0, 'hk00003': 1.0, 'hk00004': 1.0, 'hk00005': 0.6105, 'hk00006': 1.0,
               'hk00011': 1.0, 'hk00012': 1.0, 'hk00016': 1.0, 'hk00017': 1.0, 'hk00019': 1.0, 'hk00023': 1.0,
               'hk00027': 1.0, 'hk00066': 1.0, 'hk00083': 1.0, 'hk00101': 1.0, 'hk00144': 1.0, 'hk00151': 1.0,
               'hk00175': 1.0, 'hk00267': 1.0, 'hk00288': 1.0, 'hk00386': 1.0, 'hk00388': 1.0, 'hk00688': 1.0,
               'hk00700': 0.3836, 'hk00762': 1.0, 'hk00823': 1.0, 'hk00836': 1.0, 'hk00857': 1.0, 'hk00883': 1.0,
               'hk00939': 1.0, 'hk00941': 1.0, 'hk00992': 1.0, 'hk01038': 1.0, 'hk01044': 1.0, 'hk01088': 1.0,
               'hk01109': 1.0, 'hk01113': 1.0, 'hk01299': 1.0, 'hk01398': 1.0, 'hk01928': 1.0, 'hk01997': 1.0,
               'hk02007': 1.0, 'hk02018': 1.0, 'hk02318': 1.0, 'hk02319': 1.0, 'hk02382': 1.0, 'hk02388': 1.0,
               'hk02628': 1.0, 'hk03328': 1.0, 'hk03988': 1.0}
# 代码：流通系数，%
CODE_FLOW = {'hk00001': 0.7, 'hk00002': 0.75, 'hk00003': 0.6, 'hk00004': 0.4, 'hk00005': 1.0, 'hk00006': 0.65,
             'hk00011': 0.4, 'hk00012': 0.3, 'hk00016': 0.45, 'hk00017': 0.6, 'hk00019': 0.55, 'hk00023': 0.55,
             'hk00027': 0.55, 'hk00066': 0.3, 'hk00083': 0.45, 'hk00101': 0.45, 'hk00144': 0.45, 'hk00151': 0.5,
             'hk00175': 0.6, 'hk00267': 0.2, 'hk00288': 0.6, 'hk00386': 1.0, 'hk00388': 0.95, 'hk00688': 0.35,
             'hk00700': 0.6, 'hk00762': 0.25, 'hk00823': 1.0, 'hk00836': 0.4, 'hk00857': 1.0, 'hk00883': 0.4,
             'hk00939': 0.4, 'hk00941': 0.3, 'hk00992': 0.6, 'hk01038': 0.25, 'hk01044': 0.6, 'hk01088': 1.0,
             'hk01109': 0.4, 'hk01113': 0.7, 'hk01299': 1.0, 'hk01398': 0.85, 'hk01928': 0.3, 'hk01997': 0.4,
             'hk02007': 0.35, 'hk02018': 0.6, 'hk02318': 0.7, 'hk02319': 0.7, 'hk02382': 0.65, 'hk02388': 0.35,
             'hk02628': 1.0, 'hk03328': 0.25, 'hk03988': 0.95}

CODE_PRODUCT = {'hk00001': 27.006, 'hk00002': 18.945, 'hk00003': 83.928, 'hk00004': 12.156, 'hk00005': 124.06,
                'hk00006': 13.871, 'hk00011': 7.648, 'hk00012': 12.003, 'hk00016': 13.037, 'hk00017': 60.582,
                'hk00019': 4.978, 'hk00023': 15.207, 'hk00027': 23.694, 'hk00066': 18.024, 'hk00083': 29.016,
                'hk00101': 20.241, 'hk00144': 14.751, 'hk00151': 62.33, 'hk00175': 53.832, 'hk00267': 58.18,
                'hk00288': 87.984, 'hk00386': 255.13, 'hk00388': 11.78, 'hk00688': 38.346, 'hk00700': 21.863,
                'hk00762': 76.495, 'hk00823': 21.89, 'hk00836': 19.24, 'hk00857': 210.99, 'hk00883': 178.588,
                'hk00939': 961.668, 'hk00941': 61.425, 'hk00992': 72.09, 'hk01038': 6.628, 'hk01044': 7.236,
                'hk01088': 33.99, 'hk01109': 27.724, 'hk01113': 25.879, 'hk01299': 120.75, 'hk01398': 737.749,
                'hk01928': 24.222, 'hk01997': 12.144, 'hk02007': 74.48, 'hk02018': 7.332, 'hk02318': 52.136,
                'hk02319': 27.489, 'hk02382': 7.131, 'hk02388': 37.005, 'hk02628': 74.41, 'hk03328': 87.53,
                'hk03988': 794.409}

# 国内期货代码、名称
FUTURE_NAME = {
    'I': '铁矿石', 'JD': '鸡蛋', 'FB': '纤维板', 'BB': '胶合板', 'PP': '聚丙烯', 'CS': '玉米淀粉', 'PM': '普 麦',
    'WH': '强麦', 'SR': '白糖', 'OI': '菜籽油', 'RI': '早稻', 'LR': '晚稻', 'MA': '甲醇', 'FG': '玻璃', 'RS': '油菜籽',
    'RM': '菜籽粕', 'TC': '动力煤(至604)', 'ZC': '动力煤(605起)', 'JR': '粳稻', 'SF': '硅铁', 'SM': '锰硅',
    'IF': '沪深300指数', 'IH': '上证50指数', 'IC': '中证500指数', 'TF': '5年期国债', 'T': '10年期国债', 'CU': '铜',
    'AL': '铝', 'ZN': '锌', 'RU': '橡胶', 'FU': '燃料油', 'AU': '黄金', 'AG': '白银', 'RB': '螺纹钢', 'WR': '线材',
    'PB': '铅', 'BU': '石油沥青', 'HC': '热轧卷板', 'NI': '镍', 'SN': '锡', 'A': '黄 大豆1号', 'B': '黄大豆2号',
    'M': '豆粕', 'C': '玉米', 'Y': '豆油', 'P': '棕榈油', 'L': '聚乙烯', 'V': '聚 氯乙烯', 'J': '冶金焦炭',
    'JM': '焦煤', 'bu': '石油沥青', 'al': '铝', 'AP': '苹果', 'CF': '棉花', 'TA': ' 精对苯二甲酸'
}

SQL = {
    'get_history': 'select number,name,time from weight where time>"{}" AND name in {}',
    'tongji': 'select id,trader_name,available,origin_asset,remain_asset from account_info order by available desc',
    "calculate_earn": "select F.`datetime`,F.`ticket`,O.Account_ID,F.`tickertime`,F.`tickerprice`,F.`openclose`,F.`longshort`,F.`HSI_ask`,F.`HSI_bid`,F.`MHI_ask`,F.`MHI_bid`,\
	O.Profit from futures_comparison as F,order_detail as O where F.ticket=O.Ticket and O.Status=2 and O.Symbol like 'HSENG%'",
    'get_idName': 'SELECT id,trader_name FROM account_info',
    'limit_init': 'select bazaar,code,chineseName from stock_code where bazaar in ("sz","sh")',
    "order_detail": "SELECT Account_ID,DATE_ADD(DATE_FORMAT(OpenTime,'%Y-%m-%d %H:%i:%S'),INTERVAL 8 HOUR),OpenPrice,DATE_ADD(DATE_FORMAT(CloseTime,'%Y-%m-%d %H:%i:%S'),INTERVAL 8 HOUR),"
                    "ClosePrice,Profit,Type,Lots,Status,StopLoss,TakeProfit,Ticket,Symbol FROM order_detail WHERE Status!=-1 AND Status<6 AND OpenTime>'{}' AND OpenTime<'{}' AND Symbol LIKE 'HSENG%'",
}

computer_name = socket.gethostname()  # 计算机名称


def get_date(d=None):
    ''' 返回当前日期，格式为：2018-04-04
        若参数为整数，则把当前日期按参数向前后推移 '''
    if isinstance(d, int):
        return str(datetime.datetime.now() + datetime.timedelta(days=d))[:10]
    return str(datetime.datetime.now())[:10]


def get_config(root, son):
    ''' 获取配置文件的值 '''
    if root in config and son in config[root]:
        return config[root][son]
    return '()'


def dtf(d):
    """ 日期时间格式化 """
    if isinstance(d, str):
        d = d.strip()
        if len(d) == 10:
            d = datetime.datetime.strptime(d, '%Y-%m-%d')
        else:
            d = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        return d
    elif isinstance(d, datetime.datetime):
        d = datetime.datetime.strftime(d, '%Y-%m-%d %H:%M:%S')
        return d
    elif isinstance(d, datetime.date):
        d = datetime.datetime.strftime(d, '%Y-%m-%d')
        return d


class RedisPool:
    """ Redis 数据库存取 """
    _singleton = None
    _conn = None

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super(RedisPool, cls).__new__(cls)
        return cls._singleton

    def __init__(self):
        if self._conn is None:
            self.conn()

    def conn(self):
        self._conn = redis.Redis(host='localhost')

    def get(self,key):
        try:
            value = self._conn.get(key)
        except:
            self.conn()
            value = self._conn.get(key)
        return value and json.loads(value)

    def set(self,key,value):
        try:
            self._conn.set(key,json.dumps(value))
        except:
            self.conn()
            self._conn.set(key, json.dumps(value))

class SqlPool:
    """ MySQL 数据库连接池，单例模式（继承需慎重） """
    _singleton = None
    _conn = {}  # 连接池字典
    _js = {}  # 连接数量字典
    _minSize = 3  # 空闲时的连接数，需要最大连接达到
    _maxSize = 10  # 最大连接数

    def __new__(cls, *args, **kwargs):
        if not cls._singleton:
            cls._singleton = super(SqlPool, cls).__new__(cls)
        return cls._singleton

    def __init__(self, name):
        # name：数据库名称
        self.re_conn(name)

    def re_conn(self, name, res=False):
        _conn = self._conn
        _js = self._js
        self.name = name
        if name not in _conn or res:
            _conn[name] = []
            _js[name] = 0
        if not _conn[name] and _js[name] <= self._maxSize:
            conn = pymysql.connect(db=name, user=config['U']['us'], passwd=config['U']['ps'],
                                   host=config['U']['hs'],
                                   charset='utf8')
            _conn[name].append(conn)
            _js[name] += 1

    def get_conn(self, name, closed=False):
        ''' 获取连接 '''
        _conn = self._conn
        if closed:
            self.re_conn(name, res=True)
        if name not in _conn or not _conn[name]:
            self.re_conn(name)
        conn = _conn[name].pop()
        return conn

    def set_conn(self, name, conn):
        ''' 每使用完连接后，需回收连接 '''
        _conn = self._conn[name]
        l_c = len(_conn)
        if l_c < self._minSize:
            _conn.append(conn)
        else:
            _js = self._js[name]
            _js = _js - 1 if _js > 0 else l_c
            conn.close()


def runSqlData(db, sql, params=None):
    """ 执行SQL语句，返回查询结果 db：数据库名称
    (carry_investment,stock_data)；sql：SQL语句；params：参数 """
    sp = SqlPool(db)
    data = None
    conn = None
    try:
        conn = sp.get_conn(db)
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()  # 提交
        data = cur.fetchall()
    except:
        # 认为数据库已经中断连接，重连，再执行
        try:
            conn = sp.get_conn(db, closed=True)  # get_conn(conn.db, isclose=True)
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()  # 提交
            data = cur.fetchall()
        except:
            return
    finally:
        if conn:
            sp.set_conn(db, conn)
    return data


class MongoDBData:
    """ MongoDB 数据库的连接与数据查询处理类，单例模式（继承需慎重）"""
    _singleton = None
    _coll = None

    def __new__(cls, *args, **kwargs):
        if not cls._singleton:
            cls._singleton = super(MongoDBData, cls).__new__(cls)
        return cls._singleton

    def __init__(self):
        if not self._coll:
            self._coll = self.get_coll()

    def get_coll(self):
        client = pymongo.MongoClient('mongodb://192.168.2.226:27017')
        db = client['Future']
        coll = db['future_1min']
        return coll

    def data_day(self, code, date):
        """ 获取一天的期货数据 """
        day = datetime.timedelta(days=1)
        data = self._coll.find({'datetime': {'$gte': date, '$lt': date + day}, 'code': code},
                               projection=['datetime', 'open', 'high', 'low', 'close','trade'])
        # 时间，开盘，收盘，最低，最高，成交量
        data = [[i['datetime'], i['open'], i['close'], i['low'], i['high'], i['trade']] for i in data]
        data.sort()
        return data

    def get_data(self, code, sd, ed):
        """ 获取指定时间区间的数据，参数：合约代码，开始日期，结束日期 """
        if isinstance(sd,str):
            sd = dtf(sd)
        if isinstance(ed, str):
            ed = dtf(ed)
        re_code = re.search('\d+', code)
        if re_code and len(re_code[0]) == 3:
            code = code[:-3] + '1' + re_code[0]
        days = (ed - sd).days
        res = []

        for j in range(days + 1):
            d1, d2 = [], []
            day = datetime.timedelta(days=j)
            [d2.append([str(i[0])] + i[1:]) if i[0].hour < 18 else d1.append([str(i[0])] + i[1:]) for i in self.data_day(code, sd + day)]
            res += d1 + d2
        # res = pd.DataFrame(res, columns=['datetime', 'open', 'close', 'low', 'high'])
        return res


def get_tcp():
    ''' 返回IP地址 '''
    return config['U']['hs'] if computer_name != 'doc' else '192.168.2.204'



def format_int(*args):
    """  字符串批量转换为整数 """
    if isinstance(args, tuple) and len(args) == 1:
        return int(args[0])
    else:
        return (int(i) for i in args)


IP_NAME = {}


def get_ip_name(files):
    # 访客字典
    global IP_NAME
    if not IP_NAME and os.path.isfile(files):
        with open(files, 'r') as f:
            IP_NAME = json.loads(f.read())
        return IP_NAME
    else:
        return IP_NAME


def get_ip_address(ip):
    res = ''
    try:
        d = requests.get('http://www.ip138.com/ips1388.asp?ip={}&action=2'.format(ip))
        d.encoding = 'gb2312'
        d = d.text
        p = PyQuery(d)
        a = p('ul')
        a = a('li')[0].text
        res = a[5:]
    except:
        pass
    return res


def get_data(url=None):
    '''获取恒生指数成分股数据，格式为：
    [(34, '腾讯控股'), (27, '香港交易所'), (21, '建设银行'), (18, '中国银行'), (16, '工商银行')...]，时间'''
    url = url if url else 'https://www.hsi.com.hk/HSI-Net/HSI-Net?cmd=nxgenindex&index=00001&sector=00'
    req = requests.get(url).text
    req = re.sub('\s', '', req)
    # req=re.findall('<constituentscount="51">(.*?)</stock></constituents><isContingency>',req)
    com = re.compile('contribution="([+|-]*\d+)".*?<name>.*?</name><cname>(.*?)</cname></stock>')
    s = re.findall(com, req)
    rq = str(datetime.datetime.now())[:11]
    ti = re.findall('datetime="(.*?)"current', req)[0][-10:]
    s1 = rq + ti  # 时间
    result = [(int(i[0]), i[1].replace('\'', '').replace('A', '')) for i in s if i[0] != '0']
    result.sort()
    result.reverse()

    global WEIGHT
    resSize = len(result)
    WEIGHT = [result[i][1] for i in range(resSize) if (result[i][
                                                           1] in WEIGHT_BASE or 2 > i or i >= resSize - 2)]  # WEIGHT_BASE + [result[i][1] for i in range(-2, 2)]
    return result, s1


def get_min_history():
    '''返回数据格式为：(点数，名称，时间)...'''
    global YESTERDAT_PRICE
    codes = [i for i in CODE_NAME if CODE_NAME[i] in WEIGHT]
    result2 = {}
    jjres = {i: [] for i in WEIGHT}
    for code in codes:
        try:
            data = requests.get(
                'http://web.ifzq.gtimg.cn/appstock/app/minute/query?_var=min_data_%s&code=%s' % (code, code)).text
            jsons = json.loads(data.split('=', 1)[1])
            data = jsons['data'][code]['data']['data']
            closes = jsons['data'][code]['qt'][code][4]
            result2[code] = [[float(i.split()[1]), float(closes), int(i.split()[0])] for i in data]
        except Exception as exc:
            logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))

    zs = YESTERDAT_PRICE[0]  # 昨日收盘价
    if zs is None or YESTERDAT_PRICE[1] is not time.localtime(time.time())[2]:
        YESTERDAT_PRICE = get_yesterday_price()
        zs = YESTERDAT_PRICE[0]
    count_time = [i[2] for i in result2[codes[0]]]
    result, _ = read_changes()
    up = sum([result[i][1] * CODE_PRODUCT[i] for i in result])
    for ti in count_time:
        result = {}
        for i in result2:
            res = [j for j in result2[i] if j[2] == ti]
            if res:
                result[i] = res[0]
        if not result:
            continue

        for i in result:
            t = result[i][0] * CODE_PRODUCT[i]  # CODE_EQUITY[i] * CODE_WEIGHT[i] * CODE_FLOW[i]
            u = result[i][1] * CODE_PRODUCT[i]  # CODE_EQUITY[i] * CODE_WEIGHT[i] * CODE_FLOW[i]
            gx = (t - u) / up * zs
            jjres[CODE_NAME[i]].append([round(gx), ti])
    return jjres


def get_history(db):
    '''返回数据格式为：(点数，时间)...'''
    this_day = get_date()
    result = runSqlData(db, SQL['get_history'].format(this_day, tuple(WEIGHT)))
    if not result:
        result = runSqlData(db, 'SELECT * FROM weight ORDER BY TIME DESC LIMIT 600')
        result = list(result)
        result.reverse()
    result1 = {}
    for name in WEIGHT:
        result1[name] = [[i[0], str(i[2])[-8:-3].replace(':', '')] for i in result if i[1] == name]
    return result1


def read_changes(url='http://qt.gtimg.cn/q'):
    '''获取51支权重股数据，返回的数据格式：{代码：[当前价，昨日收盘价,涨幅],...}，时间'''
    result = dict()
    times = str(datetime.datetime.now()).split('.')[0]
    codes = ','.join(['r_' + i for i in CODE_NAME])
    # urls=f'http://web.sqt.gtimg.cn/q={codes}'
    urls = '{}={}'.format(url, codes)
    data = requests.get(urls).text
    if data:
        data = data.replace('\n', '').split(';')[:-1]
        for d in data:
            da = d.split('=')
            cod = da[0][-7:] if 'hk' in da[0] else da[0][-10:]
            res = da[1].split('~')
            result[cod] = [float(res[3]), float(res[4]), float(res[32])]
        else:
            times = res[30]

    return result, times


def get_yesterday_price():
    '''获取昨日收盘指数价格'''
    this_time = time.localtime(time.time())[2]
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:57.0) Gecko/20100101 Firefox / 57.0'}
    try:
        req = requests.get('https://cn.investing.com/indices/hang-sen-40', headers=headers).text
        req = re.sub('\s', '', req)
        p = re.findall('昨收:</span><spandir="ltr">(\d+,\d+.\d+)</span></li>', req)[0]
        p = p.replace(',', '')
        return (float(p), this_time)
    except:
        data = requests.get('http://web.sqt.gtimg.cn/q=hkHSI').text
        data = data.split('=')[1].split('~')[3:6]
        return (float(data[1]), this_time)


def get_price():
    '''获取并计算恒生指数成分股所贡献的点数，
    返回数据格式为：[(34, '腾讯控股'), (27, '香港交易所'), (21, '建设银行'), (18, '中国银行'), (16, '工商银行')...]，时间'''
    global YESTERDAT_PRICE
    zs = YESTERDAT_PRICE[0]
    if zs is None or YESTERDAT_PRICE[1] is not time.localtime(time.time())[2]:
        YESTERDAT_PRICE = get_yesterday_price()
        zs = YESTERDAT_PRICE[0]
    try:
        result, times = read_changes()
    except:
        result, times = read_changes(url='http://web.sqt.gtimg.cn/q')
    up = 0
    # zs = 32958.69  # 昨日收盘价
    jjres = []

    up = sum([result[i][1] * CODE_PRODUCT[i] for i in result])
    for i in result:
        t = result[i][0] * CODE_PRODUCT[i]  # CODE_EQUITY[i] * CODE_WEIGHT[i] * CODE_FLOW[i]
        u = result[i][1] * CODE_PRODUCT[i]  # CODE_EQUITY[i] * CODE_WEIGHT[i] * CODE_FLOW[i]
        gx = (t - u) / up * zs
        jjres.append((round(gx), result[i][2], CODE_NAME[i]))
    jjres.sort()
    jjres.reverse()

    global WEIGHT
    resSize = len(jjres)
    WEIGHT = [jjres[i][-1] for i in range(resSize) if (jjres[i][
                                                           -1] in WEIGHT_BASE or 2 > i or i >= resSize - 2)]  # WEIGHT_BASE + [jjres[i][-1] for i in range(-2,2)]
    return jjres, times


IDS = []  # 初始化账号列表


def get_idName(cur=None):
    sql = SQL['get_idName']
    id_name = runSqlData('carry_investment', sql)
    # conn.close()
    id_name = {i: j for i, j in id_name}
    return id_name


def tongji():
    global IDS
    results = runSqlData('carry_investment', SQL['tongji'])
    # conn.close()
    IDS = [i[0] for i in results]
    return results


def get_date_add_day(dates, ts):
    asd = dates.split('-')
    asd = datetime.datetime(int(asd[0]), int(asd[1]), int(asd[2])) + datetime.timedelta(days=ts)
    return str(asd)[:10]


def order_detail(dates=None, end_date=None):
    global IDS
    if dates is None and end_date is None:
        dates = get_date(-30)
        end_date = get_date(1)
    end_date = dtf(end_date) + datetime.timedelta(days=1)
    sql = SQL['order_detail']
    sql = sql.format(dates, end_date)
    data = runSqlData('carry_investment', sql)
    IDS = set([i[0] for i in data])
    return data


def sp_order_trade(size=None):
    ''' :return data ['2018-07-13 09:49:09', 28608.0, 697, 1, 'MHIN8', '01-0520186-00', '卖', '已成交', 28608.0, 1, 0, 1, 14734039, -1, 0]'''
    status = {0: '发送中', 1: '工作中', 2: '无效', 3: '待定', 4: '新增中', 5: '更改中', 6: '删除中',
              7: '无效中', 8: '部分成交', 9: '已成交', 10: '已删除', 18: '等待批准',
              20: '成交已覆盘', 21: '删除已覆盘', 24: '同步异常中', 28: '部分成交已删除',
              29: '部分成交并删除已覆盘 ', 30: '交易所無效',
              }

    timeStamp = time.mktime(time.strptime(str(datetime.datetime.now())[:10], '%Y-%m-%d'))
    # 时间，成交价，
    sql_trade = "SELECT TradeTime,AvgPrice,IntOrderNo,Qty,ProdCode,AccNo,BuySell,Status,OrderPrice,TotalQty,RemainingQty,TradedQty,RecNo FROM sp_trade_records WHERE TradeDate=%s" % timeStamp
    # 时间，合约，价格，止损价，订单编号，剩余数量，已成交数量，总数量，用户，买卖，状态
    sql_order = "SELECT TIMESTAMP,ProdCode,Price,StopLevel,IntOrderNo,Qty,TradedQty,TotalQty,AccNo,BuySell,STATUS FROM sp_order_records WHERE STATUS IN (1,2,3)"
    data = runSqlData('carry_investment', sql_trade)

    ran = range(len(data))
    # data (1531271751, 28001.0, 630, 1, 'MHIN8', '01-0520186-00', 'S', 9, 28001.0, 1, 0, 1, 14726449)
    users = {i[5] for i in data}
    prods = {i[4] for i in data}
    res = {}
    used = []  # 已经处理了的单
    for user in users:
        res[user] = {}
        for prod in prods:
            count = 0
            kc = []
            data2 = []
            for i in ran:
                if i in used or not (data[i][5] == user and data[i][4] == prod):
                    continue
                used.append(i)
                dt = [str(datetime.datetime.fromtimestamp(data[i][0]))]
                dt += list(data[i][1:])
                dt.append(sum(data[j][3] if data[j][6] == 'B' else -data[j][3] for j in range(0, i + 1) if
                              data[j][5] == user and data[j][4] == prod))
                try:
                    for j in range(dt[3]):
                        kc.append(dt[1])
                        if data2 and abs(dt[13]) < abs(data2[-1][13]):
                            count += -(kc.pop() - kc.pop()) if dt[6] == 'B' else (
                                kc.pop() - kc.pop() if dt[6] == 'S' else 0)

                except Exception as exc:
                    logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))
                dt.append(count)
                data2.append(dt.copy())
            data2 = [i[0:6] + ['买' if i[6] == 'B' else '卖'] + [status.get(i[7])] + i[8:] for i in data2]
            if len(data2) > 0:
                data2.insert(0, [0 if i != 5 else user for i in range(15)])
            res[user][prod] = data2

    data = [res[j][i] for j in res for i in res[j]]
    data = [i for j in data for i in j]
    if size == 1:
        # conn.close()
        return data
    datagd = runSqlData('carry_investment', sql_order)
    datagd = [[str(datetime.datetime.fromtimestamp(i[0]))[:19]] + list(i[1:9]) + ['买' if i[9] == 'B' else '卖'] + [
        status.get(i[10])] for i in datagd]
    # conn.close()
    return data, datagd


def sp_order_record(start_date=None, end_date=None):
    global IDS
    IDS = set()
    std = time.mktime(time.strptime(start_date, '%Y-%m-%d'))
    endd = time.mktime(time.strptime(end_date, '%Y-%m-%d'))

    sql_trade = "SELECT FROM_UNIXTIME(TradeTime,'%Y-%m-%d %H:%i:%S'),AvgPrice,IntOrderNo,Qty,ProdCode,AccNo,BuySell,Status,OrderPrice,TotalQty,RemainingQty,TradedQty,RecNo FROM sp_trade_records " \
                "WHERE TradeDate>={} and TradeDate<={} AND RecNo not in {}".format(std, endd, (14722630, 14722737))
    # 时间，合约，价格，止损价，订单编号，剩余数量，已成交数量，总数量，用户，买卖，状态
    data = runSqlData('carry_investment', sql_trade)
    # data = data[2:]  # 数据缺失导致
    # conn.close()
    ran = range(len(data))
    # data (1531271751, 28001.0, 630, 1, 'MHIN8', '01-0520186-00', 'S', 9, 28001.0, 1, 0, 1, 14726449)
    users = {i[5] for i in data}
    prods = {i[4] for i in data}
    resAll = []
    huizong = {}
    for user in users:
        for prod in prods:
            kc = []
            data2 = []
            res = []
            upk = user + prod
            for i in ran:
                if not (data[i][5] == user and data[i][4] == prod):
                    continue
                dt = list(data[i][:7])
                # huizong: 日期，账号，合约，盈亏，多单数，空单数，总下单数，盈利单数
                if upk not in huizong:
                    huizong[upk] = [dt[0][:10], user, prod, 0, 0, 0, 0, 0]

                dt.append(sum(data[j][3] if data[j][6] == 'B' else -data[j][3] for j in range(0, i + 1) if
                              data[j][5] == user and data[j][4] == prod))
                try:
                    for j in range(dt[3]):
                        kc.append(dt)
                        # dt ['2018-07-30 10:08:02', 28714.0, 5821, 1, 'HSIQ8', '01-0202975-00', 'B', 1]
                        if data2 and abs(dt[7]) < abs(data2[-1][7]):
                            stop = kc.pop()
                            start = kc.pop()
                            yk = 0
                            if dt[6] == 'B':  # 卖
                                yk = (start[1] - stop[1])
                                huizong[upk][5] += 1
                            elif dt[6] == 'S':  # 买
                                yk = (stop[1] - start[1])
                                huizong[upk][4] += 1
                            res.append(
                                [user, prod, start[0], start[1], stop[0], stop[1], yk, '多' if dt[6] == 'S' else '空', 1,
                                 '已平仓'])
                            huizong[upk][3] += yk
                            huizong[upk][6] += 1
                            huizong[upk][7] += (1 if yk > 0 else 0)
                    data2.append(dt)
                except Exception as exc:
                    logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))
            if upk in huizong:
                sl = round(huizong[upk][7] / huizong[upk][6] * 100, 1) if huizong[upk][6] > 0 else 0
                huizong[upk].append(sl)
            # ['2018-07-30 10:08:02', 28714.0, 5821, 1, 'HSIQ8', '01-0202975-00', 'B', 1]
            kc = [[k[5], k[4], k[0], k[1], None, None, None, '多' if k[6] == 'B' else '空', 1, '未平仓'] for k in kc]
            res = sorted(kc + res, key=lambda x: x[2])
            resAll += res
        IDS.add(user)
    return resAll, huizong


def calculate_earn(dates, end_date):
    '''计算所赚。参数：‘2018-01-01’
    返回值：[[开仓时间，单号，ID，平仓时间，价格，开仓0平仓1，做多0做空1，赚得金额，正向跟单，反向跟单]...]'''
    global IDS
    sql = SQL['calculate_earn']
    if dates:
        sql += " and F.`datetime`>'{}' and F.`datetime`<'{}'".format(dates, end_date)
    com = runSqlData('carry_investment', sql)
    # conn.close()
    result = []
    ticket = [i[1] for i in com]
    dt_tk = [(dtf(i[0]), i[1]) for i in com]
    ids = []
    for inz, i in enumerate(com):
        if ticket.count(i[1]) == 2:
            in1 = dt_tk.index((dtf(i[0]), i[1]))
            if in1 < inz:
                continue
            try:
                in2 = dt_tk[in1 + 1:].index((dtf(i[0]), i[1])) + in1 + 1
            except:
                continue
            if com[in1][6] == 0:  # longshort为0则是做多
                price_z = com[in2][7] - com[in1][8]
                price_f = com[in1][7] - com[in2][8]
            else:
                price_z = com[in1][8] - com[in2][7]
                price_f = com[in2][8] - com[in1][7]
            ids.append(com[in1][2]) if com[in1][2] not in ids else 0  # 账号列表
            # 开仓时间，单号，ID，平仓时间，价格，做多0做空1，赚得金额，正向跟单，反向跟单
            result.append(list(com[in1][:3]) + [com[in2][0], com[in1][4], com[in1][6], com[in1][-1], price_z, price_f])
    IDS = ids
    return result


class Limit_up:
    """ 股票涨停板的处理类 """
    __slots__ = ('cdate', 'this_up', 'chineseName', 'codes')
    def __init__(self):
        '''初始化，从数据库更新所有股票代码'''
        self.cdate = datetime.datetime(*time.localtime()[:3])  # 当前日期
        self.this_up = False
        self.chineseName = {}  # 中文名称
        time1 = -1
        # 获取存储股票代码文件的修改时间
        if os.path.isfile('log\\codes_gp.txt'):
            times = os.path.getmtime('log\\codes_gp.txt')
            time1 = time.localtime(times)[2]
        # 若不是在同一天修改的，则重新写入
        if time.localtime()[2] != time1:
            codes = runSqlData('stock_data', SQL['limit_init'])
            # self.conn.close()
            # 获取股票代码
            codes = [i for i in codes if i[1][:3] in ('600', '000', '300', '601', '002', '603')]
            self.chineseName = {i[0] + i[1]: i[2] for i in codes if
                                i[1][:3] in ('600', '000', '300', '601', '002', '603')}

            su = 1
            with open('log\\codes_gp.txt', 'w') as f:
                for i in codes:
                    f.write(i[0] + i[1])
                    su += 1
                    if su % 60 == 0:
                        f.write('\n')
                    else:
                        f.write(',')

        if os.path.isfile('log\\dict_data.txt'):
            times = time.localtime(os.path.getmtime('log\\dict_data.txt'))
            this_t = time.localtime()
            if this_t.tm_mday == times.tm_mday and (
                    1502 > int(str(this_t[3]) + str(this_t[4])) > 901 or times.tm_hour >= 15):
                self.this_up = True
        self.codes = []
        try:
            with open('log\\codes_gp.txt', 'r') as f:
                self.codes = f.readlines()
        except Exception as exc:
            logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))(exc)

    def f_date(self, s_date):
        '''格式化日期时间，格式：20180103151743'''
        y = int(s_date[:4])
        m = int(s_date[4:6]) if int(s_date[4]) > 0 else int(s_date[5])
        d = int(s_date[6:8]) if int(s_date[6]) > 0 else int(s_date[7])
        h = int(s_date[8:10]) if int(s_date[8]) > 0 else int(s_date[9])
        M = int(s_date[10:12]) if int(s_date[10]) > 0 else int(s_date[11])
        return datetime.datetime(y, m, d, h, M)

    def getData(self):
        '''获取指定股票的开盘收盘数据，返回格式：
        dict{'code':[2.46, 2.54, 2.48, 2.41, 2.36, 2.34, 2.33, 2.35, 2.35, 2.47, 2.47, 2.38, 2.27, 2.29, 2.29, 2.29, 2.29, 2.19],...}'''

        with open('log\\dict_data.txt') as f, open('log\\dict_name.txt') as f2:
            dict_data = json.loads(f.read())
            dict_name = json.loads(f2.read())

        if self.this_up:
            return dict_data, dict_name

        codes = self.codes
        dict_data1 = {}
        for code1 in codes:
            try:
                html = requests.get('http://qt.gtimg.cn/q=%s' % code1).text
                html = html.replace('\n', '').split(';')
                html = [i.split('~') for i in html[:-1]]
                html = [i for i in html if 0 < float(i[3])]
                html = [[i[0][2:10],  # 市场加代码
                         i[1],  # 中文名称
                         float(i[5]),  # 开盘价
                         float(i[3]),  # 收盘价
                         i[30][:8],  # 时间20180314
                         ] for i in html
                        ]
                last_date = dict_data.get('last_date')
                for ht in html:
                    d = dict_data.get(ht[0])
                    if d and len(d) >= 16:
                        if last_date == ht[4]:
                            dict_data[ht[0]][-2:] = [ht[2], ht[3]]
                        else:
                            dict_data[ht[0]] = d[-16:] + [ht[2], ht[3]]
                            dict_name[ht[0]] = ht[1]
                    elif d:
                        if last_date == ht[4]:
                            dict_data1[ht[0]][-2:] = [ht[2], ht[3]]
                        else:
                            dict_data1[ht[0]] = d + [ht[2], ht[3]]
                    else:
                        dict_data1[ht[0]] = [ht[2], ht[3]]
                else:
                    dict_data['last_date'] = ht[4]
            except Exception as exc:
                continue
        with open('log\\dict_data.txt', 'w') as f, open('log\\dict_name.txt', 'w') as f2:
            # 合并2个字典并保存
            f.write(json.dumps(dict(dict_data, **dict_data1)))
            f2.write(json.dumps(dict_name))

        return dict_data, dict_name

    def read_code(self):
        jtzt = []
        if os.path.isfile('log\\jtzt_gp.txt'):
            times = time.localtime(os.path.getmtime('log\\jtzt_gp.txt'))
            this_t = time.localtime()
            if this_t.tm_mday == times.tm_mday and (times.tm_hour >= 15 or times.tm_hour < 9):
                with open('log\\jtzt_gp.txt', 'r') as f:
                    jtzt = f.read()
                    jtzt = json.loads(jtzt)
                return jtzt
        codes = self.codes
        rou = lambda f: round((f * 1.1) - f, 2)
        # while 1:
        stock_up = []
        for code in codes:
            try:
                html = requests.get('http://qt.gtimg.cn/q=%s' % code).text
            except:
                continue
            html = html.replace('\n', '').split(';')
            html = [i.split('~') for i in html[:-1]]
            html = [i for i in html if 0 < float(i[3])]
            html = [[i[0][2:10],  # 市场加代码
                     i[1],  # 中文名称
                     float(i[5]),  # 开盘价
                     float(i[33]),  # 最高价
                     float(i[34]),  # 最低价
                     float(i[3]),  # 收盘价
                     float(i[37]),  # 成交额（万）
                     float(i[36]),  # 成交量（手）
                     float(i[4]),  # 昨日收盘价
                     self.f_date(i[30]),  # 涨停时间
                     1 if float(i[31]) >= rou(float(i[4])) else 0,  # 当前是否涨停(1是，0否)
                     self.cdate  # 创建的日期
                     ] for i in html
                    if round((float(i[33]) - float(i[4])) / float(i[4]), 2) >= 0.1]  # 计算是否涨停
            stock_up += html
        statistics = [[i[0], i[1], i[10]] for i in stock_up]
        with open('log\\jtzt_gp.txt', 'w') as f:
            f.write(json.dumps(statistics))
        return statistics

    def yanzen(self, rq_date):
        jyzt = {}
        mx = {}
        # mx_n={}
        with open('log\\codes_gp.txt') as f:
            codes = f.read()
        codes = codes.split(',')[:-1]
        try:
            data, dict_name = self.getData()
        except Exception as exc:
            logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))(exc)
            return
        file_index = r'E:\黄海军\资料\Carry\mysite\jyzt_gp.txt' if computer_name != 'doc' else r'D:\tools\Tools\EveryDay\jyzt_gp.txt'
        if os.path.isfile(file_index):
            times = time.localtime(os.path.getmtime(file_index))
            this_t = time.localtime()
            with open(file_index, 'r') as f:
                jyzt = json.loads(f.read())
            jyzt_l = jyzt.get(rq_date, [])
            if jyzt_l or (this_t.tm_hour < 15 or this_t.tm_mday == times.tm_mday):
                jyzt_l = [[i[0], dict_name.get(i[0]), i[1]] for i in jyzt_l]
                return jyzt_l

        if os.path.isfile('log\\jyzt_gp.txt'):
            times = time.localtime(os.path.getmtime('log\\jyzt_gp.txt'))
            this_t = time.localtime()
            with open('log\\jyzt_gp.txt', 'r') as f:
                jyzt = json.loads(f.read())
            jyzt_l = jyzt.get(rq_date, [])
            if jyzt_l or (this_t.tm_hour < 15 or this_t.tm_mday == times.tm_mday):
                jyzt_l = [[i[0], dict_name.get(i[0]), i[1]] for i in jyzt_l]
                return jyzt_l
        for m in os.listdir('log\\models'):
            if m[-2:] == '.m':
                mx[m[:-2]] = joblib.load('log\\models\\' + m)
                # mx_n[m[:-2]]=[]

        jyzt_l = []
        for code in codes:
            data2 = data.get(code)
            if not data2 or len(data2) != 18:
                continue
            for i in mx.keys():
                r = mx[i].predict([data2])
                if r == 1:
                    jyzt_l.append(code)
                    # mx_n[i].append(code)

        jyzt_l = Counter(jyzt_l).most_common()
        jyzt[rq_date] = jyzt_l[:10]
        with open('log\\jyzt_gp.txt', 'w') as f:  # 保存
            f.write(json.dumps(jyzt))

        jyzt = [[i[0], dict_name.get(i[0]), i[1]] for i in jyzt]

        return jyzt  # mx_n


class Zbjs(ZB):
    def __init__(self):
        self.tab_name = {'1': 'wh_same_month_min', '2': 'wh_min', '3': 'handle_min', '4': 'index_min'}  # index_min
        super(Zbjs, self).__init__()

    def main(self, _ma=60):
        sql = 'SELECT a.datetime,a.open,a.high,a.low,a.close FROM futures_min a INNER JOIN (SELECT DATETIME FROM futures_min ORDER BY DATETIME DESC LIMIT 0,{})b ON a.datetime=b.datetime'.format(
            _ma)
        data = runSqlData('carry_investment', sql)
        dyn = self.dynamic_index(data)
        dyn.send(None)
        req = None
        param = 1
        while param:
            param = yield req
            req = dyn.send(param)
        dyn.close()
        self.sendNone(dyn)

    def get_hkHSI_date(self, db, size=60, database=None):
        ''' 从数据库或者网站获取恒生指数期货日线数据，放回数据结构为：{'2018-01-01':[open,close,high,low]...} '''
        if database != None:
            tab_name = self.tab_name.get(database, 'wh_same_month_min')  # futures_min
            sql = "SELECT DATE_FORMAT(DATETIME,'%Y-%m-%d'),OPEN,CLOSE,MAX(high),MIN(low) FROM {} WHERE prodcode='HSI' GROUP BY DATE_FORMAT(DATETIME,'%Y-%m-%d')".format(
                tab_name)
            data = runSqlData(db, sql)
            data = {de[0]: [de[2] - de[1], de[3] - de[4]] for de in data}
            return data
        data = requests.get(
            'http://web.ifzq.gtimg.cn/appstock/app/kline/kline?_var=kline_dayqfq&param=hkHSI,day,,,%s' % size).text
        data = json.loads(data.split('=')[1])
        data = data['data']['hkHSI']['day']
        data = {de[0]: [float(de[2]) - float(de[1]), float(de[3]) - float(de[4])] for de in data}
        return data

    def get_data(self, db, dates, dates2, database):
        ''' 从指定数据库获取指定日期的数据 '''
        if database == '1':
            sql = "SELECT DATETIME,OPEN,high,low,CLOSE,vol FROM wh_same_month_min WHERE prodcode='HSI' AND datetime>='{}' AND datetime<='{}'".format(
                dates, dates2)
        else:
            # sql="SELECT datetime,open,high,low,close,vol FROM index_min WHERE code='HSIc1' AND datetime>'{}' AND datetime<'{}'".format(dates,dates2)
            sql = "SELECT datetime,open,high,low,close FROM {} WHERE datetime>='{}' AND datetime<='{}'".format(
                self.tab_name['2'], dates, dates2)
        return runSqlData(db, sql)

    def main2(self, _ma, _dates, end_date, _fa, database, reverse=False, param=None):
        end_date = dtf(end_date) + datetime.timedelta(days=1)
        _res, first_time = {}, []
        huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
                   'avg_day': 0, 'least2': 0, 'most2': 0, 'zs': 0, 'ydzs': 0, 'zy': 0}
        # conn = get_conn('carry_investment')  # if database == '1' else get_conn('stock_data')
        # prodcode = runSqlData(conn,"SELECT prodcode FROM futures_min WHERE datetime>='{}' AND datetime<='{}' GROUP BY prodcode".format(_dates,end_date))
        all_price = []
        is_inst_date = []
        # for code in prodcode:
        # da = self.get_data(conn, _dates, end_date, database,code[0])
        da = self.get_data('carry_investment', _dates, end_date, database)
        self.zdata = da
        res, first_time = self.trd(_fa, reverse=reverse, param=param)

        hk = self.get_hkHSI_date(db='carry_investment', database=database)  # 当日波动

        res_key = list(res.keys())
        for i in res_key:
            if i in is_inst_date:
                continue
            is_inst_date.append(i)
            if not res[i]['datetimes']:
                del res[i]
                continue
            mony = res[i]['mony']
            huizong['yk'] += mony
            huizong['zl'] += (res[i]['duo'] + res[i]['kong'])
            huizong['least'] = [i, mony, hk.get(i)[0], hk.get(i)[1]] if mony < huizong['least'][1] else huizong[
                'least']
            huizong['most'] = [i, mony, hk.get(i)[0], hk.get(i)[1]] if mony > huizong['most'][1] else huizong[
                'most']
            mtsl = []
            for j in res[i]['datetimes']:
                mtsl.append(j[3])
                if j[4] == -1 and j[3] < 0:
                    huizong['zs'] += 1
                elif j[4] == 1:
                    huizong['zy'] += 1
                elif j[4] == -1:
                    huizong['ydzs'] += 1
            all_price += mtsl
            if not res[i].get('ylds'):
                res[i]['ylds'] = 0
            if mtsl:
                ylds = len([sl for sl in mtsl if sl > 0])
                res[i]['ylds'] += ylds  # 盈利单数
                res[i]['shenglv'] = round(ylds / len(mtsl) * 100, 2)  # 每天胜率
            else:
                res[i]['shenglv'] = 0

        _res = dict(res, **_res)

        huizong['shenglv'] += len([p for p in all_price if p > 0])
        huizong['shenglv'] = int(huizong['shenglv'] / huizong['zl'] * 100) if huizong['zl'] > 0 else 0  # 胜率
        huizong['avg'] = huizong['yk'] / huizong['zl'] if huizong['zl'] > 0 else 0  # 平均每单盈亏
        res_size = len(_res)
        huizong['avg_day'] = huizong['yk'] / res_size if res_size > 0 else 0  # 平均每天盈亏
        huizong['least2'] = min(all_price) if all_price else 0
        huizong['most2'] = max(all_price) if all_price else 0

        # closeConn(conn)  # 关闭数据库连接
        return _res, huizong, first_time

    def main_all(self, _dates, end_date, database, reverse=True, param=None):
        _res = {}
        _huizong = {
            k: {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
                'avg_day': 0, 'least2': 0, 'most2': 0, 'first_time': None}
            for k in self.xzfa
        }
        # conn = get_conn('carry_investment')  # if database == '1' else get_conn('stock_data')
        # prodcode = runSqlData(conn,"SELECT prodcode FROM futures_min WHERE datetime>='{}' AND datetime<='{}' GROUP BY prodcode".format(_dates, end_date))
        hk = self.get_hkHSI_date(db='carry_investment')  # 当日波动
        all_price = {}
        # for code in prodcode:
        da = self.get_data('carry_investment', _dates, end_date, database)
        self.zdata = da
        res, first_time = self.trd_all(reverse=reverse, param=param)
        res_key = list(res.keys())
        for fa in res_key:
            res_fa_key = list(res[fa].keys())
            all_price[fa] = all_price[fa] if all_price.get(fa) else []
            _huizong[fa]['first_time'] = first_time[fa] if first_time[fa] else _huizong[fa]['first_time']  # 未平仓的单
            for i in res_fa_key:
                if not res[fa][i]['datetimes']:
                    del res[fa][i]
                    continue
                if _res.get(fa) and i in _res[fa]:
                    continue
                bd = hk.get(i, [0, 0])
                mony = res[fa][i]['mony']
                _huizong[fa]['yk'] += mony
                _huizong[fa]['zl'] += (res[fa][i]['duo'] + res[fa][i]['kong'])
                _huizong[fa]['least'] = [i, mony, bd[0], bd[1]] if mony < _huizong[fa]['least'][1] else _huizong[fa][
                    'least']
                _huizong[fa]['most'] = [i, mony, bd[0], bd[1]] if mony > _huizong[fa]['most'][1] else _huizong[fa][
                    'most']
                mtsl = [j[3] for j in res[fa][i]['datetimes']]
                all_price[fa] += mtsl
                if not res[fa][i].get('ylds'):
                    res[fa][i]['ylds'] = 0
                if mtsl:
                    ylds = len([sl for sl in mtsl if sl > 0])
                    res[fa][i]['ylds'] += ylds  # 盈利单数
                    res[fa][i]['shenglv'] = round(ylds / len(mtsl) * 100, 2) if len(mtsl) != 0 else 0  # 每天胜率
                else:
                    res[fa][i]['shenglv'] = 0
            # if code == prodcode[-1]:

            _huizong[fa]['shenglv'] += len([p for p in all_price[fa] if p > 0])
            _huizong[fa]['shenglv'] = int(_huizong[fa]['shenglv'] / _huizong[fa]['zl'] * 100) if _huizong[fa][
                                                                                                     'zl'] > 0 else 0  # 胜率
            _huizong[fa]['avg'] = _huizong[fa]['yk'] / _huizong[fa]['zl'] if _huizong[fa]['zl'] > 0 else 0  # 平均每单盈亏
            res_size = len(res[fa])
            _huizong[fa]['avg_day'] = _huizong[fa]['yk'] / res_size if res_size > 0 else 0  # 平均每天盈亏
            _huizong[fa]['least2'] = min(all_price[fa]) if all_price[fa] else 0  # 亏损最多的一单
            _huizong[fa]['most2'] = max(all_price[fa]) if all_price[fa] else 0  # 盈利最多的一单

        if _res:
            _res = {k: dict(res[k], **_res[k]) for k in _res}
        else:
            _res = res

        # closeConn(conn)  # 关闭数据库连接
        return _res, _huizong

    def main_new(self, _ma, _dates, end_date, database, reverse=False, param=None):
        _res, first_time = {}, []
        huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
                   'avg_day': 0, 'least2': 0, 'most2': 0}
        # conn = get_conn('carry_investment')  # if database == '1' else get_conn('stock_data')
        prodcode = runSqlData('carry_investment',
                              "SELECT prodcode FROM futures_min WHERE datetime>='{}' AND datetime<='{}' GROUP BY prodcode".format(
                                  _dates, end_date))
        all_price = []
        is_inst_date = []
        # for code in prodcode:
        # da = self.get_data(conn, _dates, end_date, database,code[0])
        da = self.get_data('carry_investment', _dates, end_date, database)
        self.zdata = da
        res, first_time = self.trd_new(reverse=reverse, param=param)

        hk = self.get_hkHSI_date(db='carry_investment', database=database)  # 当日波动

        res_key = list(res.keys())
        for i in res_key:
            if i in is_inst_date:
                continue
            is_inst_date.append(i)
            if not res[i]['datetimes']:
                del res[i]
                continue
            mony = res[i]['mony']
            huizong['yk'] += mony
            huizong['zl'] += (res[i]['duo'] + res[i]['kong'])
            huizong['least'] = [i, mony, hk.get(i)[0], hk.get(i)[1]] if mony < huizong['least'][1] else huizong[
                'least']
            huizong['most'] = [i, mony, hk.get(i)[0], hk.get(i)[1]] if mony > huizong['most'][1] else huizong[
                'most']
            mtsl = [j[3] for j in res[i]['datetimes']]
            all_price += mtsl
            if not res[i].get('ylds'):
                res[i]['ylds'] = 0
            if mtsl:
                ylds = len([sl for sl in mtsl if sl > 0])
                res[i]['ylds'] += ylds  # 盈利单数
                res[i]['shenglv'] = round(ylds / len(mtsl) * 100, 2)  # 每天胜率
            else:
                res[i]['shenglv'] = 0

        _res = dict(res, **_res)

        huizong['shenglv'] += len([p for p in all_price if p > 0])
        huizong['shenglv'] = int(huizong['shenglv'] / huizong['zl'] * 100) if huizong['zl'] > 0 else 0  # 胜率
        huizong['avg'] = huizong['yk'] / huizong['zl'] if huizong['zl'] > 0 else 0  # 平均每单盈亏
        res_size = len(_res)
        huizong['avg_day'] = huizong['yk'] / res_size if res_size > 0 else 0  # 平均每天盈亏
        huizong['least2'] = min(all_price)
        huizong['most2'] = max(all_price)

        # closeConn(conn)  # 关闭数据库连接
        return _res, huizong, first_time

    def get_future(self, this_date):
        ''' 计算日线数据 '''
        dd = dtf(this_date)
        size = datetime.datetime.now() - dd
        size = size.days + 8
        data = requests.get(
            "http://web.ifzq.gtimg.cn/appstock/app/kline/kline?_var=kline_dayqfq&param=hkHSI,day,,,%s" % size).text
        data = data.split("=")[1]
        data = json.loads(data)
        data = data["data"]["hkHSI"]["day"]
        data2 = [[i[1], i[2], i[3], i[4]] for i in data if i[0] <= this_date]
        gs = float(data2[-1][0]) - float(data2[-2][1])
        zs = float(data2[-1][1]) - float(data2[-1][0])
        data = []
        data2.pop()
        for d in data2[-6:]:
            data += [d[0], d[1], d[2], d[3]]
        gd = 0
        zd = 0
        for m in os.listdir('log\\svm_gd'):
            svm = joblib.load('log\\svm_gd\\' + m)
            jg = svm.predict([data])[0]
            gd += 1 if jg == "1" else 0
        for m in os.listdir('log\\svm_zd'):
            svm = joblib.load('log\\svm_zd\\' + m)
            jg = svm.predict([data])[0]
            zd += 1 if jg == "1" else 0

        return gd, zd, gs, zs


class RedisHelper:
    """ Redis 数据库连接与发布消息 """
    __slots__ = ('__conn', 'chan_pub', 'is_run')

    def __init__(self):
        self.__conn = redis.Redis(host='localhost')
        self.chan_pub = 'test'
        self.is_run = True

    # 发送消息
    def public(self, msg):
        self.__conn.publish(self.chan_pub, msg)
        return True

    def main(self):
        while self.is_run:
            dt = str(datetime.datetime.now())
            self.public(dt[:19])  # 发布
            time.sleep(1)
            break


def day_this_mins(start_date, end_date, code='HSI'):
    """ 获取指定时间段的分钟数据，转换为指定分钟的分钟数据"""
    sql = "SELECT DATE_FORMAT(datetime,'%d/%H:%i'),close FROM wh_same_month_min WHERE prodcode='{}' " \
          "AND DATE_FORMAT(datetime,'%Y-%m-%d')>='{}' AND DATE_FORMAT(datetime,'%Y-%m-%d')<='{}' ORDER BY datetime".format(
        code, start_date, end_date)  # AND DATE_FORMAT(DATETIME,'%H')<17
    d = runSqlData('carry_investment', sql)
    times = [i[0] for i in d]
    datas = [i[1] for i in d]
    return times, datas


def huices(res, huizong, init_money, dates, end_date, pinzhong=None):
    keys = [i for i in res if res[i]['datetimes']]
    keys.sort()
    jyts = len(keys)
    if not keys:
        return {}, huizong
    jyys = (int(keys[-1][:4])*12+int(keys[-1][-5:-3])) - (int(keys[0][:4])*12+int(keys[0][-5:-3])) + 1
    hc = {
        'jyts': jyts,  # 交易天数
        'jyys': jyys,  # 交易月数
        'hlbfb': round(huizong['yk'] / init_money * 100, 2),  # 总获利百分比
        'dhl': round(huizong['yk'] / jyts / init_money * 100, 2),  # 日获利百分比
        'mhl': round(huizong['yk'] / jyys / init_money * 100, 2),  # 月获利百分比
        'ye': init_money + huizong['yk'],  # 余额
        'cgzd': [0, 0, 0],  # 成功的做多交易； 赚钱的单数，总单数，正确率
        'cgzk': [0, 0, 0],  # 成功的做空交易； 赚钱的单数，总单数，正确率
        'avglr': 0,  # 平均获利
        'alllr': 0,  # 总获利
        'avgss': 0,  # 平均损失
        'allss': 0,  # 总损失
        'zzl': [],  # 增长率
        'vol': [],  # 手数
        'dayye': [],  # 每天余额
        'daylr': [],  # 每天利润，不叠加
        'zjhcs': [0],  # 资金回测
        'ccsj': 0,     # 平均每手持仓时间
        'allcchz': [],       # 持仓时间，多空(1,0），盈亏，手数，日内隔夜(1,0），平仓时间
    }
    jingzhi = []  # 净值
    zx_x = []
    zx_y = []
    zdhc = 0
    zjhc = 0
    max_jz = 0
    ccsj = 0  # 总持仓时间
    count_yl = 0  # 总盈利
    count_ks = 0  # 总亏损
    avg = huizong['yk'] / huizong['zl']  # 平均盈亏
    count_var = 0
    code_index = {}
    for i in keys:
        je = round(res[i]['mony'])
        # 这里的净值计算是初始化一个不变的入金计算的，所以保留，没有采用
        jingzhi.append(jingzhi[-1] + je if jingzhi else init_money + je)  # 净值
        zx_x.append(''.join(i.split('-'))[2:])  # 日期
        zx_y.append(zx_y[-1] + je if zx_y else je)  # 总盈亏，每天叠加
        hc['zzl'].append(round(zx_y[-1] / init_money * 100, 2))
        hc['vol'].append(res[i]['duo'] + res[i]['kong'])
        hc['dayye'].append(zx_y[-1] + init_money)  # 每天余额
        hc['daylr'].append(je)  # 每天利润
        if len(jingzhi) > 1:
            max_jz = jingzhi[-2] if jingzhi[-2]>max_jz else max_jz
            zjhc = round((max_jz - jingzhi[-1]) / max_jz * 100, 2)
            # zdhc = zdhc2 if zdhc2 > zdhc else zdhc
        hc['zjhcs'].append(zjhc if zjhc > 0 else 0)  # 资金回测
        for j in res[i]['datetimes']:
            # j: 开仓时间，平仓时间，多空，盈亏，开仓价格，平仓价格，手数，合约代码
            # j: ['2018-08-31 13:44:41', '2018-09-03 09:01:03', '多', -1450, 2449.5, 2435.0, 1, 'J1901']
            if j[2] == '多':
                hc['cgzd'][1] += 1
                if j[3] > 0:
                    hc['cgzd'][0] += 1
            elif j[2] == '空':
                hc['cgzk'][1] += 1
                if j[3] > 0:
                    hc['cgzk'][0] += 1
            if j[3] > 0:
                hc['alllr'] += j[3]
                hc['avglr'] += 1
            else:
                hc['allss'] += j[3]
                hc['avgss'] += 1

            # 计算持仓时间
            if pinzhong is None:  # 按实际物理时间计算
                start_d = j[0].replace(':', '-').replace(' ', '-')
                start_d = start_d.split('-') + [0, 0, 0]
                start_d = [int(sd) for sd in start_d]
                end_d = j[1].replace(':', '-').replace(' ', '-')
                end_d = end_d.split('-') + [0, 0, 0]
                end_d = [int(ed) for ed in end_d]
                ccsj += (time.mktime(tuple(end_d)) - time.mktime(tuple(start_d)))*j[6] if end_d >= start_d else 0
            else:  # 按K线的条数计算
                if j[7] not in code_index:
                    code_index[j[7]] = re.search(r'[A-z]+',j[7])[0] + 'L8'
                this_ccsj = (pinzhong[code_index[j[7]]][j[1][:-3]] - pinzhong[code_index[j[7]]][j[0][:-3]])*j[6]
                ccsj += this_ccsj
                allcchz = (this_ccsj,1 if j[2]=='多' else 0,j[3],j[6],1 if j[0][:10]==j[1][:10] else 0,j[1],j[7])
                hc['allcchz'].append(allcchz)
            # 计算利润因子
            if j[3] > 0:
                count_yl += j[3]
            else:
                count_ks += -j[3]
            # 方差
            count_var += (j[3] - avg) ** 2
    try:
        hc['cgzd'][2] = round(hc['cgzd'][0] / hc['cgzd'][1] * 100, 2) if hc['cgzd'][1] != 0 else 0
        hc['cgzk'][2] = round(hc['cgzk'][0] / hc['cgzk'][1] * 100, 2) if hc['cgzk'][1] != 0 else 0
        if pinzhong is None:
            ccsj = ccsj / 60 / huizong['zl'] if huizong['zl'] != 0 else 0
            if ccsj > 1440:
                _ccsj = round(ccsj / 60 / 24, 2)  # 平均持仓时间
                hc['ccsj'] = str(_ccsj) + ' 天'
            elif ccsj > 60:
                _ccsj = round(ccsj / 60, 2)  # 平均持仓时间
                hc['ccsj'] = str(_ccsj) + ' 小时'
            else:
                _ccsj = round(ccsj, 2)  # 平均持仓时间
                hc['ccsj'] = str(_ccsj) + ' 分钟'
        else:
            ccsj = ccsj / huizong['zl'] if huizong['zl'] != 0 else 0
            _ccsj = round(ccsj/60,2)
            hc['ccsj'] = str(_ccsj) + ' 小时'
        hc['lryz'] = round(count_yl / count_ks, 2) if count_ks != 0 else 0
        count_var = count_var / huizong['zl'] if huizong['zl'] != 0 else 0
        hc['std'] = round(count_var ** 0.5, 2)

        hc['zjhc'] = max(hc['zjhcs'])  # 最大回测
        hc['avglr'] = hc['alllr'] / hc['avglr'] if hc['avglr'] != 0 else 0  # 平均获利
        hc['avgss'] = hc['allss'] / hc['avgss'] if hc['avgss'] != 0 else 0  # 平均损失
        hc['zjhc'] = hc['zjhc'] if hc['zjhc'] >= 0 else 0
        hc['jingzhi'] = jingzhi  # 每天净值
        hc['max_jz'] = max(jingzhi)  # 最高
        hc['zx_x'] = zx_x  # 折线图x轴 时间
        hc['zx_y'] = zx_y  # 折线图y轴 利润

    except Exception as exc:
        logging.error("文件：HSD.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))

    def get_jy(jg):
        jy1 = round(jg['mony'] / init_money * 100, 2)  # 获利
        jy2 = jg['mony']  # 利润
        jy3 = jg['mony']  # 点
        jy4 = jg['shenglv']  # 每天胜率
        jy5 = jg['duo'] + jg['kong']  # 开的单数
        jy6 = jy5  # 手
        jy7 = jg['ylds']
        return jy1, jy2, jy3, jy4, jy5, jy6, jy7

    try:
        this_date = dtf(end_date)  # - datetime.timedelta(days=1)
        this_date = datetime.datetime.now() if this_date > datetime.datetime.now() else this_date
        week = [str(this_date - datetime.timedelta(days=tw))[:10] for tw in
                range(this_date.weekday() + 1)]  # 这个星期的日期
        month = [str(this_date - datetime.timedelta(days=td))[:10] for td in range(this_date.day)]  # 这个月的日期
        time_date = time.strptime(end_date, '%Y-%m-%d')
        year = [str(this_date - datetime.timedelta(days=ty))[:10] for ty in range(time_date.tm_yday)]  # 这一年的日期
        year.sort()

        this_week = [0, 0, 0, 0, 0, 0, 0]
        this_month = [0, 0, 0, 0, 0, 0, 0]
        this_year = [0, 0, 0, 0, 0, 0, 0]
        year_s = min(set(keys).intersection(set(year)))  # keys 与 year 的交集 的最小值
        for yd in year[year.index(year_s):]:
            jg = res.get(yd)
            if not jg or len(jg['datetimes']) < 1:
                continue
            jys = get_jy(jg)
            if yd == str(this_date)[:10]:
                hc['this_day'] = jys
            if yd in week:
                this_week[0] += jys[0]
                this_week[1] += jys[1]
                this_week[2] += jys[2]
                # this_week[3] += jys[3]
                this_week[4] += jys[4]
                this_week[5] += jys[5]
                this_week[6] += jys[6]
            if yd in month:
                this_month[0] += jys[0]
                this_month[1] += jys[1]
                this_month[2] += jys[2]
                # this_month[3] += jys[3]
                this_month[4] += jys[4]
                this_month[5] += jys[5]
                this_month[6] += jys[6]
            this_year[0] += jys[0]
            this_year[1] += jys[1]
            this_year[2] += jys[2]
            # this_year[3] += jys[3]
            this_year[4] += jys[4]
            this_year[5] += jys[5]
            this_year[6] += jys[6]
        this_week[0] = round(this_week[0], 2)
        this_week[3] = round(this_week[6] / this_week[4] * 100, 2)
        this_month[0] = round(this_month[0], 2)
        this_month[3] = round(this_month[6] / this_month[4] * 100, 2)
        this_year[0] = round(this_year[0], 2)
        this_year[3] = round(this_year[6] / this_year[4] * 100, 2)
    except Exception as exc:
        logging.error("文件 HSD.py 第{}行报错：{}".format(sys._getframe().f_lineno, exc))
    hc['this_week'] = this_week
    hc['this_month'] = this_month
    hc['this_year'] = this_year
    huizong['kuilv'] = 100 - huizong['shenglv']

    return hc, huizong


def huice_day(res, init_money, real=False):
    """ res：交易详细记录，init_money：入金，real：是否是实盘 """
    keys = [i for i in res if res[i]['datetimes']]
    keys.sort()
    jyts = len(keys)
    if not keys:
        return {}
    jyys = int(keys[-1][-5:-3]) - int(keys[0][-5:-3]) + 1
    hc = {
        'vol': [],  # 手数
        'dayye': [],  # 每天余额
        'singlelr': [],  # 每单利润，不叠加
        'zjhcs': [0],  # 资金回测
        'day_time': [],  # 当天每单交易时间
        'day_yk': [],  # 当天每单交易盈亏，包括未平仓的状态
        'day_ykall': [],  # 当天每单叠加交易盈亏，不包括未平仓的状态
        'day_pcyk': [],  # 当天每次平仓盈亏
        'day_close': [],  # 当天交易时的收盘价
        'day_x': 0,  # 当天分钟行情时间
        'samedatetime': '',  # 当天的日期
        'subtracted': 0,  # 行情价格减去的值
        'interval_1': [],
    }
    jingzhi = []  # 净值
    zx_x = []
    zx_y = []
    count_yl = 0  # 总盈利
    count_ks = 0  # 总亏损
    sameday = []  # 当天的所有交易
    for i in keys:
        je = round(res[i]['mony'])
        zx_x.append(i[-5:-3] + i[-2:])  # 日期
        zx_y.append(zx_y[-1] + je if zx_y else je)  # 总盈亏，每天叠加

        for j in res[i]['datetimes']:
            if real:
                for vol in range(abs(int(j[6]))):
                    # st = j[6] if j[2] == '多' else -j[6]
                    st = 1 if j[2] == '多' else -1
                    sameday.append([j[0], st, j[4]])
                    sameday.append([j[1], -st, j[5]])
                hc['samedatetime'] = i
            else:
                for vol in range(abs(int(j[6]))):
                    # st = j[6] if j[2] == '多' else -j[6]
                    st = 1 if j[2] == '多' else -1
                    sameday.append([j[0], st, j[4], j[7]])
                    sameday.append([j[1], -st, j[5], j[7]])
                hc['samedatetime'] = i
    try:
        sameday.sort()
        sameday2 = []
        hc['day_time'] = [sa[0][-11:-3].replace(' ', '/') for sa in sameday]
        day_time = deepcopy(hc['day_time'])
        # hc['day_yk'] = [sa[1] for sa in sameday]
        hc['day_x'], hc['day_close'] = day_this_mins(keys[0], keys[-1])

        def get_ind(l, x, s):
            if s > 0:
                inde = l.index(x) + 1
                return inde + get_ind(l[inde:], x, s - 1)
            else:
                return l.index(x)

        syc = []  # 开仓价
        syc2 = []  # 模拟时的单号
        cc = 0  # 持仓
        last_cc = 0  # 上一次持仓
        zjhcs = 0
        for y in range(len(hc['day_x'])):
            cl = hc['day_close'][y]  # 分钟收盘价
            dx = hc['day_x'][y]  # 日期时间
            pcyk = 0  # 分钟平仓盈亏
            if dx not in day_time:
                if cc == 0:
                    yk = 0
                elif cc < 0:
                    yk = sum(syc) - len(syc) * cl  # (sum(syc) if real else sum(sycs[0] for sycs in syc))
                else:
                    yk = len(syc) * cl - sum(syc)  # (sum(syc) if real else sum(sycs[0] for sycs in syc))
                hc['day_yk'].append(round(sum(hc['day_pcyk']) + yk, 1))
            else:
                yk = 0  # 分钟盈亏
                reg = day_time.count(dx)
                # reg = sum([abs(s[1]) for s in sameday if s[0][8:10]+'/'+s[0][11:16]==dx])
                for c2 in range(reg):
                    sind = get_ind(day_time, dx, c2)
                    same = sameday[sind]
                    # sameday2.append(same[1])
                    if real:  # 实盘
                        syc.append(same[2])
                        if same[1] == 1:
                            if cc < 0:
                                pop = -(syc.pop() - syc.pop())
                                yk += pop
                                pcyk += pop
                                yk += (sum(syc) - len(syc) * cl if c2 == reg - 1 else 0)
                            else:
                                yk += (len(syc) * cl - sum(syc) if c2 == reg - 1 else 0)
                        elif same[1] == -1:
                            if cc > 0:
                                pop = syc.pop() - syc.pop()
                                yk += pop
                                pcyk += pop
                                yk += (len(syc) * cl - sum(syc) if c2 == reg - 1 else 0)
                            else:
                                yk += (sum(syc) - len(syc) * cl if c2 == reg - 1 else 0)
                    else:
                        syc.append(same[2])
                        syc2.append(same[3])
                        if same[1] > 0:
                            if syc2.count(syc2[-1]) >= 2 and last_cc != cc:
                                o2 = syc2.index(syc2[-1])
                                pop = -(syc.pop() - syc.pop(o2))
                                yk += pop
                                pcyk += pop
                                yk += (sum(syc) - len(syc) * cl if c2 == reg - 1 else 0)
                                syc2.pop()
                                syc2.pop(o2)
                            else:
                                yk += (len(syc) * cl - sum(syc) if c2 == reg - 1 else 0)
                        elif same[1] < 0:
                            if syc2.count(syc2[-1]) >= 2 and last_cc != cc:
                                o2 = syc2.index(syc2[-1])
                                pop = syc.pop() - syc.pop(o2)
                                yk += pop
                                pcyk += pop
                                yk += (len(syc) * cl - sum(syc) if c2 == reg - 1 else 0)
                                syc2.pop()
                                syc2.pop(o2)
                            else:
                                yk += (sum(syc) - len(syc) * cl if c2 == reg - 1 else 0)
                    last_cc = cc
                    cc += same[1]
                hc['day_yk'].append(round(sum(hc['day_pcyk']) + yk, 1))

            hc['vol'].append(cc * 10)
            hc['day_pcyk'].append(round(pcyk, 1))
            hc['day_ykall'].append(round(pcyk + hc['day_ykall'][-1] if hc['day_ykall'] else pcyk, 1))
            if len(hc['day_yk']) > 1:
                max_lr = max(hc['day_yk'][:-1])
                zjhc = round((max_lr - hc['day_yk'][-1]) / (max_lr + init_money) * 100, 2)
                zjhc = zjhc if (hc['day_yk'][-1] != 0 and zjhc > 0) else 0
                hc['zjhcs'].append(zjhc if zjhc != zjhcs else 0)  # 资金回测
                zjhcs = zjhc if zjhc != 0 else zjhcs
        v = hc['day_close'][0] // 1000 * 1000
        hc['subtracted'] = int(v)
        hc['day_close'] = [i - v for i in hc['day_close']]
    except Exception as exc:
        logging.error("文件：HSD.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))

    return hc


class GXJY:
    __slots__ = ('code_name2', 'code_name', 'bs', 'code_bs')

    def __init__(self):
        self.code_name2 = {'bu1706': '石油沥青', 'rb1705': '螺纹钢', 'ru1705': '橡胶', 'j1705': '冶金焦炭',
                           'ru1709': '橡胶', 'j1709': '冶金焦炭', 'al1711': '铝', 'rb1801': '螺纹钢',
                           'j1801': '冶金焦炭', 'rb1805': '螺纹钢', 'rb1810': '螺纹钢', 'AP1810': '苹果',
                           'CF1901': '棉一号'}
        self.code_name = {'bu': '石油沥青', 'rb': '螺纹钢', 'ru': '橡胶', 'j': '冶金焦炭',
                          'al': '铝', 'AP': '苹果', 'CF': '棉花'}
        self.bs = {'bu': 10, 'rb': 10, 'ru': 10, 'j': 100, 'al': 5, 'AP': 10, 'CF': 5}
        self.code_bs = {self.code_name[i]: self.bs[i] for i in self.code_name}

    def gx_lsjl(self, folder):
        data = pd.DataFrame()
        xls = [folder + os.sep + i for i in os.listdir(folder) if '.xls' in i]
        for i in xls:
            p = pd.read_excel(i)
            data = data.append(p)
            # data = pd.concat([data,p])
        return data

    def to_sql(self, data, table):
        """ 写入数据到指定的数据表"""
        if table == 'gx_record':
            sql = "INSERT IGNORE INTO gx_record(datetime,exchange,code,busi,kp,vol,price,cost,jyf,jy_code,seat_code," \
                  "system_code,cj_code,insure,jgq,currency) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for r in data.values:
                r[0] = str(r[0])
                dt = r[0][:4] + '-' + r[0][4:6] + '-' + r[0][6:8] + ' ' + str(r[13])
                try:
                    runSqlData('carry_investment', sql, (
                        dt, r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], str(r[9]), r[10], r[11], r[12], r[14],
                        r[15],
                        r[16]))
                except Exception as exc:
                    logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))
        elif table == 'gx_entry_exit':

            sql = "INSERT IGNORE INTO gx_entry_exit(`datetime`,flow,direction,`out`,enter,currency,`type`,bank,abstract) " \
                  "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for r in data.values:
                r[0] = str(r[0])
                dt = r[0][:4] + '-' + r[0][4:6] + '-' + r[0][6:8]
                fx = '证券转出' if r[3] > 0 else '银行转入'
                try:
                    runSqlData('carry_investment', sql, (dt, r[1], fx, r[3], r[4], r[5], r[6], r[7], r[8]))
                except Exception as exc:
                    pass

    def get_gxjy_sql(self, code=None):
        """ 得到数据表 gx_record 的数据，格式化指定的格式用以 ray 函数调用 """
        sql = "SELECT datetime,code,busi,price,vol,cost FROM gx_record WHERE 1=1"
        if code and code[:-4] in self.bs:
            sql += " AND code LIKE '{}%'".format(code[:-4])
        ds = []
        data = runSqlData('carry_investment', sql)
        for i in data:
            bs = -i[4] if i[2] == '卖出' else i[4]
            price = i[3]
            sj = i[0]
            code = i[1]
            cost = i[5]
            ds.append([bs, price, sj, code, cost])

        ds = pd.DataFrame(ds, columns=['bs', 'price', 'time', 'code', 'cost'])
        return ds

    def get_gxjy_sql_all(self, code=None):
        """ 获取数据库，原始数据，并增加一列名称 """
        sql = "SELECT datetime,exchange,code,busi,kp,vol,price,cost,jyf,jy_code,seat_code," \
              "system_code,cj_code,insure,jgq,currency FROM gx_record where 1=1"
        if code and code[:-4] in self.bs:
            sql += " AND code LIKE '{}%'".format(code[:-4])
        p = runSqlData('carry_investment', sql)  # pd.read_sql(sql, conn)
        p2 = []
        for i in p:
            p2.append((str(i[0]),) + i[1:] + (self.code_name.get(i[2][:-4]),))
        return p2

    def get_dates(self):
        """ 获取所有的交易日期 """
        sql = "SELECT DATE_FORMAT(DATETIME,'%Y-%m-%d') FROM gx_record GROUP BY DATE_FORMAT(DATETIME,'%Y-%m-%d') ORDER BY DATETIME"
        dates = runSqlData('carry_investment', sql)
        return dates

    def entry_exit(self):
        """ 获取出入金信息 """
        sql = "SELECT DATE_FORMAT(datetime,'%Y-%m-%d'),direction,`out`,enter FROM gx_entry_exit;"
        ee = runSqlData('carry_investment', sql)
        return ee

    def datas(self):
        data = {
            'jy': 0,  # 交易手数
            'price': 0,  # 交易价格
            'cb': 0,  # 总成本
            'jcb': 0,  # 净会话成本
            'start_time': 0,  # 开始时间
            'end_time': 0,  # 结束时间
            'mai': 0,  # 买卖
            'kc': [],  # 开仓
            'all_kp': [],  # 当前会话所有开平仓
            'pcyl': 0,  # 平仓盈利
            'pcyl_all': 0,  # 平仓盈利汇总
            'all_price': 0,  # 叠加价格
            'all': 0,  # 总盈亏
            'sum_price': 0,  # 全天叠加价格
            'all_jy_add': 0,  # 全天所有交易手数，叠加
            'dbs': 0,  # 序号
            'wcds1': 0,
            'wcds': 0,  # 完成的单
            'cost': 0,  # 手续费，不叠加
            'ALL_JY': [],  # 所有完成的交易记录
        }
        ind = 0
        _while = 0
        while 1:
            msg = yield data, ind
            hand = abs(msg[0])
            if _while <= 0 and hand > 1:
                _while = hand
            _while -= 1
            if _while <= 0:
                ind += 1
            data['price'] = msg[1]
            if data['jy'] == 0:
                data['pcyl'] = 0
                data['wcds1'] = 0
                data['pcyl_all'] = 0
                data['all_kp'] = []

                data['cb'] = 0  # 成本
                data['jcb'] = 0  # 净成本
                data['all_price'] = 0

            if msg[0] > 0:
                data['jy'] += 1
                data['all_jy_add'] += 1
                data['mai'] = 1
                data['all_price'] += data['price']
                data['sum_price'] += data['price']
                data['kc'].append([1, data['price'], msg[2], msg[3], msg[4]])
                data['all_kp'].append([1, data['price']])

            elif msg[0] < 0:
                data['jy'] -= 1
                data['all_jy_add'] += 1
                data['mai'] = -1
                data['all_price'] -= data['price']
                data['sum_price'] -= data['price']
                data['kc'].append([-1, data['price'], msg[2], msg[3], msg[4]])
                data['all_kp'].append([-1, data['price']])

            data['cost'] = msg[4]

            if len(data['kc']) > 1 and data['kc'][-1][0] != data['kc'][-2][0]:
                if data['kc'][-1][0] < 0:
                    data['pcyl'] = (data['kc'][-1][1] - data['kc'][-2][1])
                else:
                    data['pcyl'] = (data['kc'][-2][1] - data['kc'][-1][1])
                data['pcyl_all'] += data['pcyl']
                data['all'] += data['pcyl']
                # 多空，平仓价，
                # [-1, data['price'], msg[2], msg[3], msg[4], 1, data['price'], msg[2], msg[3], msg[4]]
                p = data['kc'].pop() + data['kc'].pop()
                data['ALL_JY'].append([p[2], p[5], p[4]])
                data['wcds1'] += 1
                data['wcds'] += 1
            else:
                data['pcyl'] = 0

            if data['jy'] != 0:
                data['cb'] = data['all_price'] / data['jy']
                # jcbs = self.SXF * 2 / 50 * data['jy']
                jcbs = (data['wcds1'] + abs(data['jy'])) * data['cost'] / data['jy']
                # sum_cb = sum([cb[1] if cb[0]>0 else -cb[1] for cb in data['all_kp']]) # 净成本
                sum_cb = data['cb'] + jcbs  # (sum_cb-jcbs)/data['jy']
                data['jcb'] = int(sum_cb) if data['jy'] < 0 else (
                    int(sum_cb) + 1 if sum_cb > int(sum_cb) else int(sum_cb))

            data['time'] = msg[2]

    def transfer(self, df, structs):
        """ df:
                         bs    price                 time       code     cost
                    0    -1   10055.0   2018/06/19 14:51:12     AP1810   10.18
                    1    -2   10032.0   2018/06/19 14:52:54     AP1810  10.18
                    """
        res = []
        ind = 0
        is_ind = 0
        cbyl = 0
        dts = self.datas()
        dts.send(None)
        pz = {}  # 各品种交易单数
        while ind < len(df.values):
            msg = df.values[ind]
            data, ind = dts.send(msg)
            pri = data['price']
            code = msg[3][:-4]
            if code in pz:
                pz[code] += 1
            else:
                pz[code] = 1
            if len(data['kc']) > 0:
                yscb = round(sum(i[-1] for i in data['kc']) / len(data['kc']), 2)  # 原始成本
                cb = round(data['cb'], 2)  # 成本
            else:
                ak_k = [ak[1] for ak in data['all_kp'] if ak[0] > 0]
                ak_p = [ak[1] for ak in data['all_kp'] if ak[0] < 0]
                yscb = round(sum(ak_k) / len(ak_k), 2) if msg[0] < 0 else round(sum(ak_p) / len(ak_p), 2)
                cb = round(sum(ak_k) / len(ak_k), 2) if msg[0] > 0 else round(sum(ak_p) / len(ak_p), 2)

            jcb = data['jcb']  # 净成本
            cbyl += data['pcyl']  # 此笔盈利
            pjyl = round(data['all'] / data['wcds'], 2) if data['wcds'] > 0 else 0  # 平均盈利
            huihuapj = round(data['pcyl_all'] / data['wcds1'], 2) if data['wcds1'] > 0 else 0  # 会话平均盈利
            zcb = round(data['sum_price'] / data['jy'], 2) if data['jy'] != 0 else round(data['sum_price'], 2)  # 持仓成本
            jzcbs = (data['wcds'] + abs(data['jy'])) * data['cost'] / data['jy'] if \
                data[
                    'jy'] != 0 else 0  # self.SXF * 2 / 50 * data['jy']
            jzcb = (data['sum_price'] / data['jy'] + jzcbs) if data['jy'] != 0 else 0  # 净持仓成本
            jzcb = int(jzcb) + 1 if jzcb > int(jzcb) else int(jzcb)

            jlr = round(data['all'] * self.bs[msg[3][:-4]] - data['cost'], 2)  # 净利润
            jpjlr = round(jlr / data['wcds'], 2) if data['wcds'] > 0 else 0  # 净平均利润

            if ind != is_ind:  # or (ind == is_ind and data['jy'] == 0):
                # ['合约', '时间', '开仓', '当前价', '持仓', '原始成本', '会话成本', '净会话成本',
                # '此笔盈利', '会话盈利', '总盈利', '总平均盈利', '会话平均盈利', '持仓成本', '净持仓成本', '利润',
                # '净利润', '净平均利润', '手续费', '已平仓', '序号', '分块号', '中文名', '此笔净利润',
                # '手续费，不累积']
                dtstr = str(data['time'])
                struct = structs if (structs == 0 or structs == 1) else structs[dtstr[:10]]
                res.append(
                    [msg[3], dtstr, msg[0], pri, data['jy'], yscb, cb, jcb,
                     cbyl, data['pcyl_all'], data['all'], pjyl, huihuapj, zcb, jzcb, data['all'] * self.bs[msg[3][:-4]],
                     jlr, jpjlr, round(data['cost'] + (res[-1][18] if res else 0), 2), data['wcds'], data['dbs'],
                     struct,
                     self.code_name.get(msg[3][:-4]), cbyl * self.bs[msg[3][:-4]], data['cost']])
                cbyl = 0
            data['dbs'] += 1 if data['jy'] == 0 else 0  # 序号
            is_ind = ind
        dts.close()
        return res, pz

    def ray(self, df, group=None):
        res = []
        pzs = {}
        struct = 0
        codes = set(df.iloc[:, 3])
        codes = {i[:-4] for i in codes}
        dates = list(set(df.iloc[:, 2].apply(lambda x: str(x)[:10])))
        dates.sort()
        dates = {dates[i]: i % 2 for i in range(len(dates))}
        if len(codes) == 1:
            res = self.transfer(df, struct)
        else:
            for code in codes:
                # df2 = df[df.iloc[:, 3] == code]
                df2 = df[df.code.apply(lambda x: x[:-4]) == code]
                if group == 'date':
                    rs, pz = self.transfer(df2, dates)
                else:
                    rs, pz = self.transfer(df2, struct)
                    struct = 0 if struct == 1 else 1
                res += rs
                pzs = dict(pz, **pzs)

        columns = ['合约', '时间', '开仓', '当前价', '持仓', '原始成本', '会话成本', '净会话成本', '此笔盈利', '会话盈利', '总盈利', '总平均盈利', '会话平均盈利',
                   '持仓成本', '净持仓成本', '利润', '净利润', '净平均利润', '手续费', '已平仓', '序号']
        # res = pd.DataFrame(res, columns=columns)
        pzs = [(self.code_name.get(k), v / 2) for k, v in pzs.items()]
        pzs = sorted(pzs, key=lambda x: x[1], reverse=True)
        return res, pzs


class Cfmmc:
    __slots__ = ('host', 'start_date', 'end_date', 'sql')

    def __init__(self, host, start_date, end_date):
        self.host = host
        self.start_date = start_date
        self.end_date = end_date
        self.sql = f"帐号='{self.host}' AND 交易日期>='{self.start_date}' AND 交易日期<='{self.end_date}'"

    def varieties(self):
        """ 品种,成交量。[('石油沥青':34),...] """
        sql = f"SELECT 合约,SUM(手数) FROM cfmmc_trade_records WHERE {self.sql} GROUP BY 合约"
        d = runSqlData('carry_investment', sql)
        dc = {}
        for i in d:
            k = re.sub('\d', '', i[0])
            k = i[0]+'('+FUTURE_NAME[k]+')'
            if k not in dc:
                dc[k] = 0
            dc[k] += int(i[1])
        dc = [(i, dc[i]) for i in dc]
        dc = sorted(dc, key=lambda x: x[1], reverse=True)
        return dc

    def get_dates(self):
        """ 指定时间区间的交易日期 """
        sql = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records WHERE {self.sql} GROUP BY 交易日期"
        dates = runSqlData('carry_investment', sql)
        return dates

    def get_data2(self):
        """ 指定时间区间以日期与合约分组的，交易日期，合约，平仓盈亏，手续费 """
        sql = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d'),合约,SUM(平仓盈亏),SUM(手续费) FROM cfmmc_trade_records WHERE {self.sql} GROUP BY 交易日期,合约"
        sql2 = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d'),合约,SUM(持仓盈亏) FROM cfmmc_holding_position WHERE {self.sql} GROUP BY 交易日期,合约"
        data = runSqlData('carry_investment', sql)
        data = {i[0] + i[1]: [i[0], i[1], i[2], i[3]] for i in data}
        data2 = runSqlData('carry_investment', sql2)
        data2 = {i[0] + i[1]: i[2] for i in data2}
        res = []
        for i in data:
            v = data[i]
            v[2] = v[2] if v[2] else 0
            if i in data2:
                v[2] += data2[i]
            res.append(v)

        res = [(i[0], i[1], i[2], i[3], i[1]+'('+FUTURE_NAME[re.sub('\d', '', i[1])]+')') for i in res]
        return res

    def get_data(self):
        """ ('2018-08-31', 'J1901', 1750.0, 14.92, 'J1901(冶金焦炭)') """
        sql = f"SELECT DATE_FORMAT(C.交易日期,'%Y-%m-%d'),C.合约,C.平仓盈亏,T.手续费 FROM cfmmc_closed_position_trade as C,cfmmc_trade_records_trade as T WHERE C.成交序号=T.成交序号 AND C.帐号='{self.host}' AND C.交易日期>='{self.start_date}' AND C.交易日期<='{self.end_date}'"
        data = runSqlData('carry_investment', sql)
        data2 = {}
        for i in data:
            k = i[0]+i[1]
            if k not in data2:
                data2[k] = list(i) + [i[1]+'('+FUTURE_NAME[re.sub('\d', '', i[1])]+')']
            else:
                data2[k][2] += i[2]
                data2[k][3] += i[3]
        data = [data2[i] for i in data2]
        return data

    def get_rj(self):
        """ 获取出入金 """
        sql = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d'),当日存取合计 FROM cfmmc_daily_settlement WHERE 当日存取合计!=0 AND {self.sql}"
        data = runSqlData('carry_investment', sql)
        return data

    def init_money(self):
        """ 获取初始入金 """
        sql = f"SELECT 上日结存 FROM cfmmc_daily_settlement WHERE {self.sql} LIMIT 1"
        d = runSqlData('carry_investment', sql)
        if d:
            return d[0][0]

    def get_qy(self):
        """ 客户权益 """
        sql = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d'),客户权益 FROM cfmmc_daily_settlement WHERE {self.sql}"
        d = runSqlData('carry_investment', sql)
        if d:
            qy = {i[0]: i[1] for i in d}
            return qy

    def get_yesterday_hold(self, code):
        """ 获取指定账户、产品的 交易日期时间 每天的昨日持仓 """
        start_date = dtf(self.start_date)
        start_date = start_date - datetime.timedelta(days=10)
        sql = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d'),SUM(买持仓),SUM(卖持仓) FROM cfmmc_holding_position WHERE 合约='{code}' AND 帐号='{self.host}' AND 交易日期>='{start_date}' AND 交易日期<='{self.end_date}' GROUP BY 交易日期"
        sql_check = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_daily_settlement WHERE 帐号='{self.host}' AND 交易日期>='{start_date}' AND 交易日期<='{self.end_date}' ORDER BY 交易日期"
        d = runSqlData('carry_investment', sql)
        d = {i[0]:i for i in d}
        _check = runSqlData('carry_investment',sql_check)
        _c = [i[0] for i in _check]

        d2 = {_c[i]:((d[_c[i-1]][1] if d[_c[i-1]][1] else 0, d[_c[i-1]][2] if d[_c[i-1]][2] else 0) if _c[i-1] in d else (0, 0)) for i in range(1,len(_c))}
        return d2

    def get_bs(self, code, time_type=None):
        """ 获取指定账户、产品的 交易日期时间：（成交价，手数） """
        sql = f"SELECT CONCAT(DATE_FORMAT(交易日期,'%Y-%m-%d'),DATE_FORMAT(ADDDATE(成交时间,INTERVAL 1 MINUTE),' %H:%i:00')),手数,`买/卖`,成交价,`开/平` FROM cfmmc_trade_records WHERE 合约='{code}' AND {self.sql} ORDER BY 交易日期"
        d = runSqlData('carry_investment', sql)
        res = {}  # {i[0]: (i[3], (i[1] if i[2] == '买' else -i[1]), i[4]) for i in d}
        if d:
            return d
        if time_type == '5M':
            for i in d:
                t5 = datetime.datetime.strptime(i[0], '%Y-%m-%d %H:%M:%S')
                t5 = t5 + datetime.timedelta(minutes=5 - t5.minute % 5)
                t5 = str(t5)
                if t5 not in res:
                    res[t5] = [(i[3], (i[1] if i[2] == '买' else -i[1]), i[4])]
                else:
                    res[t5].append((i[3], (i[1] if i[2] == '买' else -i[1]), i[4]))
        elif time_type == '30M':
            for i in d:
                t30 = datetime.datetime.strptime(i[0], '%Y-%m-%d %H:%M:%S')
                t30 = t30 + datetime.timedelta(minutes=30 - t30.minute % 30)
                t30 = str(t30)
                if t30 not in res:
                    res[t30] = [(i[3], (i[1] if i[2] == '买' else -i[1]), i[4])]
                else:
                    res[t30].append((i[3], (i[1] if i[2] == '买' else -i[1]), i[4]))
        elif time_type == '1H':
            for i in d:
                h1 = datetime.datetime.strptime(i[0], '%Y-%m-%d %H:%M:%S')
                h1 = h1 + datetime.timedelta(minutes=60 - h1.minute % 60)
                h1 = str(h1)
                if h1 not in res:
                    res[h1] = [(i[3], (i[1] if i[2] == '买' else -i[1]), i[4])]
                else:
                    res[h1].append((i[3], (i[1] if i[2] == '买' else -i[1]), i[4]))
        elif time_type == '1D':
            for i in d:
                d1 = i[0][:10] + ' 00:00:00'
                if d1 not in res:
                    res[d1] = [(i[3], (i[1] if i[2] == '买' else -i[1]), i[4])]
                else:
                    res[d1].append((i[3], (i[1] if i[2] == '买' else -i[1]), i[4]))
        else:
            for i in d:
                if i[0] not in res:
                    res[i[0]] = [(i[3], (i[1] if i[2] == '买' else -i[1]), i[4])]
                else:
                    res[i[0]].append((i[3], (i[1] if i[2] == '买' else -i[1]), i[4]))
        return res

    def get_jz(self):
        """ 获取净值：
            增长率=（当日盈亏-当日手续费）/ 上日结存
            净值=（1 + 增长率）* （1 + 增长率2）* （1 + 增长率n）
        """
        sql = f"SELECT DATE_FORMAT(交易日期,'%Y-%m-%d'),上日结存,当日盈亏,当日手续费,当日存取合计 FROM cfmmc_daily_settlement WHERE {self.sql} ORDER BY 交易日期"
        d = runSqlData('carry_investment', sql)
        if not d:
            return
        s = []
        jzs = {}
        for i in d:
            try:
                jz = s[-1] if s else 1
                if i[1] > 0:
                    jz = (1 + (i[2] - i[3]) / i[1]) * jz
                else:
                    jz = (1 + (i[2] - i[3]) / (i[1] + (i[4] if i[4] > 0 else 0))) * jz
            except:
                pass
            s.append(jz)
            jzs[i[0]] = jz
        return jzs

def cfmmc_get_result(host,start_date,end_date):
    """ 获取指定帐号的交易开平仓记录 """
    sql = f"SELECT 成交序号,CONCAT(DATE_FORMAT(交易日期,'%Y-%m-%d'),DATE_FORMAT(ADDDATE(成交时间,INTERVAL 1 MINUTE),' %H:%i:%S')) FROM cfmmc_trade_records_trade WHERE 帐号='{host}'"
    dc = runSqlData('carry_investment',sql)
    dc = {i[0]: i[1] for i in dc}
    sql2 = f"SELECT 合约,原成交序号,开仓价,成交序号,成交价,平仓盈亏,`买/卖`,手数,'已平仓' FROM" \
           f" cfmmc_closed_position_trade WHERE 帐号='{host}' AND 交易日期>='{start_date}' AND 交易日期<='{end_date}'" \
           f" AND 原成交序号 in (SELECT 成交序号 from cfmmc_trade_records_trade WHERE 帐号='{host}')"
    cl = runSqlData('carry_investment',sql2)
    results2 = []
    for i in cl:
        _d = [host, i[0], dc[i[1]], i[2], dc[i[3]], i[4], i[5], '空' if '买' in i[6] else '多', i[7], i[8]]
        results2.append(_d)
    results2 = sorted(results2, key=lambda x: x[2])
    return results2

def cfmmc_huice(host):
    """ 回测 """
    # 日期时间，买卖，手数，开平，手续费，平仓盈亏，合约，交易日期 ('2017-04-11 14:37:30', ' 卖', 1, '开', 21.14, None, 'J1709', '2017-04-11')
    trade_sql = f"SELECT CONCAT(DATE_FORMAT(交易日期,'%Y-%m-%d'),DATE_FORMAT(成交时间,' %H:%i:%S')),`买/卖`,手数,`开/平`," \
          f"手续费,平仓盈亏,合约,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records_trade WHERE 帐号='{host}' order by 交易日期"
    trade = runSqlData('carry_investment',trade_sql)
    trade = list(trade)
    # 买卖，开仓价，平仓价，手数，平仓盈亏，交易日期 (' 卖', 17750.0, 14345.0, 1, -34050, '2017-04-25')
    closed_sql = f"SELECT `买/卖`,开仓价,成交价,手数,平仓盈亏,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_closed_position_trade WHERE 帐号='{host}'" # ORDER BY 实际成交日期"
    closed = runSqlData('carry_investment',closed_sql)
    money_sql = f"SELECT 上日结存,当日存取合计 FROM cfmmc_daily_settlement WHERE 帐号='{host}' AND " \
                f"(当日存取合计!=0 OR 交易日期 IN (SELECT MIN(交易日期) FROM cfmmc_daily_settlement WHERE 帐号='{host}'))"
    moneys = runSqlData('carry_investment',money_sql)
    init_money = sum(j[1] if i != 0 else j[0] for i, j in enumerate(moneys))
    hc = {'jyts': 4, 'jyys': 1, 'hlbfb': 6.82, 'dhl': 1.7, 'mhl': 6.82, 'ye': 10681.53, 'cgzd': [2, 3, 66.67],
          'cgzk': [16, 24, 66.67], 'avglr': 56.44055555555556, 'alllr': 1015.9300000000001, 'avgss': -37.15555555555555,
          'allss': -334.4, 'zzl': [2.7, 5.76, 6.91, 6.82], 'vol': [10, 5, 2, 10],
          'dayye': [10270.0, 10576.0, 10691.0, 10682.0],
          'daylr': [270, 306, 115, -9], 'zjhcs': [0, 0, 0, 0.08], 'ccsj': 11.8, 'lryz': 3.04, 'std': 48.91,
          'zjhc': 0.08,
          'jingzhi': [10270.0, 10576.0, 10691.0, 10682.0], 'max_jz': 10691.0, 'zx_x': ['0813', '0814', '0815', '0816'],
          'zx_y': [270, 576, 691, 682], 'this_day': (-0.09, -9.389999999999995, -9.389999999999995, 30.0, 10, 10, 3),
          'this_week': [6.82, 681.5300000000001, 681.5300000000001, 66.67, 27, 27, 18],
          'this_month': [6.82, 681.5300000000001, 681.5300000000001, 66.67, 27, 27, 18],
          'this_year': [6.82, 681.5300000000001, 681.5300000000001, 66.67, 27, 27, 18]}

    huizong = {'yk': 0,                     # 总盈亏
               'shenglv': 0,                # 胜率
               'zl': 0,                     # 总成交量
               'least': [0, 100000000],     # 最差交易：日期，金额
               'most': [0, -100000000],     # 最好交易：日期，金额
               'avg': 0,                    # 平均盈亏
               'avg_day': 0,                # 平均每天盈亏
               'least2': [0, 100000000],    # 最差交易：日期，点数
               'most2': [0, -100000000],    # 最好交易：日期，点数
               'kuilv': 0                   # 亏损比率
               }
    hc = {
        'jyts': 0,  # 交易天数
        'jyys': 0,  # 交易月数
        'hlbfb': 0,  # 总获利百分比
        'dhl': 0,   # 日获利百分比
        'mhl': 0,   # 月获利百分比
        'ye': 0,    # 余额
        'cgzd': [0, 0, 0],  # 成功的做多交易； 赚钱的单数，总单数，正确率
        'cgzk': [0, 0, 0],  # 成功的做空交易； 赚钱的单数，总单数，正确率
        'avglr': 0,  # 平均获利
        'alllr': 0,  # 总获利
        'avgss': 0,  # 平均损失
        'allss': 0,  # 总损失
        'zzl': [],  # 增长率
        'vol': [],  # 手数
        'dayye': [],  # 每天余额
        'daylr': [],  # 每天利润，不叠加
        'zjhcs': [0],  # 资金回测
        'ccsj': 0,  # 持仓时间
        'lryz': 0,  # 利润因子
        'std': 0,   # 标准差
        'zjhc': 0,  # 资金回测
        'jingzhi': [],  # 净值
        'max_jz': [],   # 最大净值
        'zx_x': [],     # 日期，作为X轴
        'zx_y': [],     # 总利润
        'this_day': [],     # 一天
        'this_week': [],    # 一星期
        'this_month': [],   # 一个月
        'this_year': [],    # 一年
    }
    _alllr = 0  # 所有获利的手数
    _allss = 0  # 所有亏损的手数
    _days = {}  # 所有每天的数据
    for i in closed:
        # 买卖，开仓价，平仓价，手数，平仓盈亏，交易日期 (' 卖', 17750.0, 14345.0, 1, -34050, '2017-04-25')
        _vol = i[3]  # 手数
        y_k = i[4]   # 盈亏
        if i[5] not in _days:
            _days[i[5]] = {
                'zzl': [],
                'vol': [],
                'dayye': [],
                'daylr': [],
                'zjhcs': [0],
            }
        huizong['yk'] += i[4]
        huizong['shenglv'] += i[3] if i[4] > 0 else 0
        huizong['zl'] += _vol
        huizong['least'] = [i[5], y_k / _vol] if y_k / _vol < huizong['least'][1] else huizong['least']
        huizong['most'] = [i[5], y_k / _vol] if y_k / _vol > huizong['most'][1] else huizong['most']

        if '买' in i[0]:
            hc['cgzk'][1] += _vol
            if y_k > 0:
                hc['cgzk'][0] += _vol
                hc['alllr'] += y_k
                _alllr += _vol
            else:
                hc['allss'] += y_k
                _allss += _vol
        else:
            hc['cgzd'][1] += _vol
            if y_k > 0:
                hc['cgzd'][0] += _vol
                hc['alllr'] += y_k
                _alllr += _vol
            else:
                hc['allss'] += y_k
                _allss += _vol

    huizong['shenglv'] = round(huizong['shenglv'] / huizong['zl'] * 100) if huizong['zl'] > 0 else 0
    huizong['kuilv'] = 100 - huizong['shenglv']
    huizong['least2'] = huizong['least'][1]
    huizong['most2'] = huizong['most'][1]

    hc['cgzk'][2] = round(hc['cgzk'][0] / hc['cgzk'][1] * 100, 2) if hc['cgzk'][1] > 0 else 0
    hc['cgzd'][2] = round(hc['cgzd'][0] / hc['cgzd'][1] * 100, 2) if hc['cgzd'][1] > 0 else 0
    hc['alllr'] = hc['alllr']/_alllr
    hc['allss'] = hc['allss']/_allss

    keys = list(set(i[7] for i in trade))
    keys.sort()
    jyts = len(keys)
    if not keys:
        return {}, huizong

    jyys = (int(keys[-1][:4]) * 12 + int(keys[-1][-5:-3])) - (int(keys[0][:4]) * 12 + int(keys[0][-5:-3])) + 1
    hc['jyts'] = jyts
    hc['jyys'] = jyys
    hc['hlbfb'] = round(huizong['yk'] / init_money * 100, 2)
    hc['dhl'] = round(huizong['yk'] / jyts / init_money * 100, 2)
    hc['mhl'] = round(huizong['yk'] / jyys / init_money * 100, 2)
    hc['ye'] = init_money + huizong['yk']


    trade2 = []
    for key in keys:
        ('2017-04-11 14:37:30', ' 卖', 1, '开', 21.14, None, 'J1709', '2017-04-11')


    return hc,huizong