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


config = configparser.ConfigParser()
config.read('log\\conf.conf')

logging.basicConfig(
    filename='log\\logging.log',
    filemode='a',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    format='%(filename)s[%(asctime)s][%(levelname)s]：%(message)s'
)

# 昨天收盘指数价格
YESTERDAT_PRICE = (None, None)
# 权重依次最大的股
WEIGHT = ('腾讯控股', '汇丰控股', '建设银行', '友邦保险', '工商银行', '中国移动', '中国平安', '中国银行', '香港交易所', '长和')
# 颜色
COLORS = ['#FF0000', '#00FF00', '#003300', '#FF6600', '#99CC33', '#663366', '#009966', '#0000FF', '#FF0080', '#00FFFF']

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

SQL = {
    'get_history': 'select number,name,time from weight where name in {}',
    'tongji': 'select id,trader_name,available,origin_asset,remain_asset from account_info order by available desc',
    "calculate_earn": "select F.`datetime`,F.`ticket`,O.Account_ID,F.`tickertime`,F.`tickerprice`,F.`openclose`,F.`longshort`,F.`HSI_ask`,F.`HSI_bid`,F.`MHI_ask`,F.`MHI_bid`,\
	O.Profit from futures_comparison as F,order_detail as O where F.ticket=O.Ticket and O.Status=2 and O.Symbol like 'HSENG%'",
    'get_idName': 'SELECT id,trader_name FROM account_info WHERE trader_name IS NOT NULL',
    'limit_init': 'select bazaar,code from stock_code where bazaar in ("sz","sh")',
}


def get_conn(dataName):
    return pymysql.connect(db=dataName, user=config['U']['us'], passwd=config['U']['ps'], host=config['U']['hs'],
                           charset='utf8')


def get_tcp():
    return config['U']['hs']


def get_data(url=None):
    '''获取恒生指数成分股数据，格式为：
    [(34, '腾讯控股'), (27, '香港交易所'), (21, '建设银行'), (18, '中国银行'), (16, '工商银行')...]，时间'''

    url = url if url else 'http://sc.hangseng.com/gb/www.hsi.com.hk/HSI-Net/HSI-Net?cmd=nxgenindex&index=00001&sector=00'
    req = requests.get(url).text
    req = re.sub('\s', '', req)
    # req=re.findall('<constituentscount="51">(.*?)</stock></constituents><isContingency>',req)
    com = re.compile('contribution="([+|-]*\d+)".*?<name>.*?</name><cname>(.*?)</cname></stock>')
    s = re.findall(com, req)
    rq = str(datetime.datetime.now())[:11]
    ti = re.findall('datetime="(.*?)"current', req)[0][-10:]
    s1 = rq + ti  # 时间
    # print(s)
    result = [(int(i[0]), i[1].replace('\'', '').replace('A', '')) for i in s if i[0] != '0']
    result.sort()
    result.reverse()

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
            logging.error(exc)

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


def get_history(conn):
    '''返回数据格式为：(点数，名称，时间)...'''
    cur = conn.cursor()
    cur.execute(SQL['get_history'].format(WEIGHT))
    result = cur.fetchall()
    conn.commit()
    conn.close()
    result1 = {}
    for name in WEIGHT:
        result1[name] = [[i[0], i[2]] for i in result if i[1] == name]
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
    return jjres, times


# def investement():
# conn = get_conn('carry_investment')
# cur = conn.cursor()
# sql = "select F.`datetime`,F.`ticket`,O.Account_ID,F.`tickertime`,F.`tickerprice`,F.`openclose`,F.`longshort`,F.`HSI_ask`,F.`HSI_bid`,F.`MHI_ask`,F.`MHI_bid` from futures_comparison as F,order_detail as O where F.ticket=O.Ticket and O.Symbol like 'HSENG%';"
# com = pd.read_sql(sql, con=conn)
# conn.commit()
# conn.close()
# com['isnull'] = (com['openclose'] + com['longshort']) % 2
#     com['hsi'] = (com['tickerprice'] - com['HSI_bid']).where(com['isnull'] == 0, com['HSI_ask'] - com['tickerprice'])
#     com['mhi'] = (com['tickerprice'] - com['MHI_bid']).where(com['isnull'] == 0, com['MHI_ask'] - com['tickerprice'])
#
#     com2 = pd.DataFrame(
#         [com.Account_ID, com['datetime'].apply(lambda x: datetime.datetime.strftime(x, '%y-%m-%d')), com.tickerprice,
#          com['isnull'], com.hsi, com.mhi])
#     com2 = com2.T.copy()
#     com2['Account_ID'].astype = np.int
#     '''
#     it = ['datetime', 'Account_ID', 'isnull']
#     dic = {}
#     for i in range(1, len(it) + 1):
#         u = len(dic)
#         # 获得所有组合列表，并以数字为key存储到字典dic
#         for c, j in enumerate(combinations(it, i)):
#             dic[u + c + 1] = [v for v in j]
#     '''
#
#     dic = {'1': ['datetime'],
#            '2': ['Account_ID'],
#            '3': ['isnull'],
#            '4': ['datetime', 'Account_ID'],
#            '5': ['datetime', 'isnull'],
#            '6': ['Account_ID', 'isnull'],
#            '7': ['datetime', 'Account_ID', 'isnull']}
#     herys = None
#     while 1:
#         myj = yield herys
#         if myj in dic:
#             si = com2.groupby(dic[myj]).size()
#             herys = com2.groupby(dic[myj]).apply(lambda x: np.mean(x))
#             herys['count'] = si.values
#         elif myj is '8':
#             herys = com
#         else:
#             herys = None
#
#
# def get_inv():
#     '''用来更新investement生成器'''
#     inv = investement()
#     inv.send(None)
#     return inv
#
#
# inv_times = datetime.datetime.now()  # 初始化时间
# inv = investement()
# inv.send(None)  # 初始化生成器
IDS = []  # 初始化账号列表


def get_idName(cur):
    cur.execute(SQL['get_idName'])
    id_name = cur.fetchall()
    id_name = {i: j for i, j in id_name}
    return id_name


def tongji():
    '''用来调用生成器'''
    global IDS
    # global inv_times
    # global inv
    # dates = datetime.datetime.now()  # 当前时间
    # if int(str(dates - inv_times)[2:4]) >= 10:  # 如果生成器执行时间有10分钟，则更新生成器
    #     inv_times = dates
    #     inv = get_inv()
    # results = inv.send(xz)
    # if results is not None:
    #     results.Account_ID = results.Account_ID.astype('int')
    #     IDS = list(set(results.Account_ID))  # 账号列表
    conn = get_conn('carry_investment')
    cur = conn.cursor()
    sql = SQL['tongji']
    cur.execute(sql)
    results = cur.fetchall()
    id_name = get_idName(cur)
    conn.commit()
    conn.close()
    IDS = [i[0] for i in results]
    return results, id_name


def calculate_earn(dates):
    '''计算所赚。参数：‘2018-01-01’
    返回值：[[开仓时间，单号，ID，平仓时间，价格，开仓0平仓1，做多0做空1，赚得金额，正向跟单，反向跟单]...]'''
    global IDS
    conn = get_conn('carry_investment')
    cur = conn.cursor()
    sql = SQL['calculate_earn']
    if dates:
        asd = dates.split('-')
        asd = datetime.datetime(int(asd[0]), int(asd[1]), int(asd[2])) + datetime.timedelta(days=1)
        dates1 = str(asd)[:10]
        sql += " and F.`datetime`>'{}' and F.`datetime`<'{}'".format(dates, dates1)
    cur.execute(sql)
    com = cur.fetchall()
    id_name = get_idName(cur)
    conn.commit()
    conn.close()
    result = []
    ticket = [i[1] for i in com]
    dt_tk = [(datetime.datetime.strftime(i[0], '%y-%m-%d'), i[1]) for i in com]
    ids = []
    for inz, i in enumerate(com):
        if ticket.count(i[1]) == 2:
            in1 = dt_tk.index((datetime.datetime.strftime(i[0], '%y-%m-%d'), i[1]))
            if in1 < inz:
                continue
            try:
                in2 = dt_tk[in1 + 1:].index((datetime.datetime.strftime(i[0], '%y-%m-%d'), i[1])) + in1 + 1
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
    return result, id_name


class Limit_up:
    def __init__(self):
        '''初始化，从数据库更新所有股票代码'''
        self.cdate = datetime.datetime(*time.localtime()[:3])  # 当前日期
        self.this_up = False
        try:
            self.conn = get_conn('stock_data')
            cur = self.conn.cursor()
        except Exception as exc:
            print(exc)
        time1 = -1
        # 获取存储股票代码文件的修改时间
        if os.path.isfile('log\\codes_gp.txt'):
            times = os.path.getmtime('log\\codes_gp.txt')
            time1 = time.localtime(times)[2]
        # 若不是在同一天修改的，则重新写入
        if time.localtime()[2] != time1:
            cur.execute(SQL['limit_init'])
            codes = cur.fetchall()
            self.conn.close()
            # 获取股票代码
            codes = [i for i in codes if i[1][:3] in ('600', '000', '300', '601', '002', '603')]
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
            logging.error(exc)


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

        with open('log\\dict_data.txt') as f:
            dict_data = json.loads(f.read())
        with open('log\\dict_name.txt') as f:
            dict_name = json.loads(f.read())
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
        with open('log\\dict_data.txt', 'w') as f:
            # 合并2个字典并保存
            f.write(json.dumps(dict(dict_data, **dict_data1)))
        with open('log\\dict_name.txt', 'w') as f:
            f.write(json.dumps(dict_name))

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
        #while 1:
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

    def yanzen(self):
        jyzt = []
        mx = {}
        #mx_n={}
        with open('log\\codes_gp.txt') as f:
            codes = f.read()
        codes = codes.split(',')[:-1]
        try:
            data, dict_name = self.getData()
        except Exception as exc:
            logging.error(exc)
            return
        if os.path.isfile('log\\jyzt_gp.txt'):
            times = time.localtime(os.path.getmtime('log\\jyzt_gp.txt'))
            this_t = time.localtime()
            if this_t.tm_mday == times.tm_mday and this_t.tm_hour - times.tm_hour <= 2:
                with open('log\\jyzt_gp.txt', 'r') as f:
                    jyzt = f.read()
                    jyzt = json.loads(jyzt)
                jyzt = jyzt[:10]
                jyzt = [[i[0], dict_name.get(i[0]), i[1]] for i in jyzt]
                return jyzt
        for m in os.listdir('log\\models'):
            if m[-2:] == '.m':
                mx[m[:-2]] = joblib.load('log\\models\\' + m)
                #mx_n[m[:-2]]=[]

        for code in codes:
            data2 = data.get(code)
            if not data2 or len(data2) != 18:
                continue
            for i in mx.keys():
                r = mx[i].predict([data[code]])
                if r == 1:
                    jyzt.append(code)
                    #mx_n[i].append(code)
        jyzt = Counter(jyzt).most_common()

        with open('log\\jyzt_gp.txt', 'w') as f:  # 保存
            f.write(json.dumps(jyzt))
        jyzt = jyzt[:10]
        jyzt = [[i[0], dict_name.get(i[0]), i[1]] for i in jyzt]

        return jyzt  # mx_n


class Zbjs(object):
    def get_data(self,dates):
        conn=get_conn('stock_data')
        cur=conn.cursor()
        sql="SELECT datetime,open,high,low,close,vol FROM index_min WHERE code='HSIc1' AND DATE_FORMAT(datetime,'%Y-%m-%d')='{}'".format(dates)
        cur.execute(sql)
        da=cur.fetchall()
        #df.columns=['date','open','high','low','close','vol']
        conn.close()
        return da

    def macd_to_sql(self,data):
        '''
        :param data: macd data
        :return: None,Write data to the database
        '''
        conn=get_conn('stock_data')
        cur=conn.cursor()
        cur.execute('TRUNCATE TABLE macd')
        conn.commit()
        sql="insert into macd(code,date,open,close,diff,dea,macd,ma,var,std,reg,mul) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        count=0
        for d in data:
            #print('HSIc1',str(d['datetimes']),d['open'],d['close'],d['diff'],d['dea'],d['macd'],d['ma'],d['var'],d['std'],d['reg'],d['mul'])
            try:
                cur.execute(sql,('HSIc1',str(d['datetimes']),d['open'],d['close'],d['diff'],d['dea'],d['macd'],d['ma'],d['var'],d['std'],d['reg'],d['mul']))
                count+=1
                if not count%10000:
                    conn.commit()
            except Exception as exc:
                print(exc)
                continue
        conn.commit()
        conn.close()

    def macd2(self,da,ma=60,short=12,long=26,phyd=9):
        # da格式：((datetime.datetime(2018, 3, 19, 9, 22),31329.0,31343.0,31328.0,31331.0,249)...)
        dc=[]
        co=0
        for i in range(len(da)):
            dc.append({'ema_short':0,'ema_long':0,'diff':0,'dea':0,'macd':0,'ma':0,'var':0,'std':0,'reg':0,'mul':0,'datetimes':da[i][0],'open':da[i][1],'close':da[i][4]})
            if i == long-1:
                ac = da[i - 1][4]
                this_c = da[i][4]
                dc[i]['ema_short'] = ac + (this_c - ac) * 2 / short
                dc[i]['ema_long'] = ac + (this_c - ac) * 2 / long
                #dc[i]['ema_short'] = sum([(short-j)*da[i-j][4] for j in range(short)])/(3*short)
                #dc[i]['ema_long'] = sum([(long-j)*da[i-j][4] for j in range(long)])/(3*long)
                dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
                dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
                dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
                co=1 if dc[i]['macd']>=0 else 0
            elif i>long-1:
                n_c = da[i][4]
                dc[i]['ema_short'] = dc[i-1]['ema_short'] * (short-2) / short + n_c * 2 / short
                dc[i]['ema_long'] = dc[i-1]['ema_long'] * (long-2) / long + n_c * 2 / long
                dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
                dc[i]['dea'] = dc[i-1]['dea'] * (phyd-2) / phyd + dc[i]['diff'] * 2 / phyd
                dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])

            if i>=ma-1:
                dc[i]['ma']=sum(da[i-j][4] for j in range(ma))/ma # 移动平均值 i-ma+1,i+1
                dc[i]['var']=sum((da[i-j][4]-dc[i]['ma'])**2 for j in range(ma))/ma # 方差 i-ma+1,i+1
                dc[i]['std']=float(np.sqrt(dc[i]['var'])) # 标准差

            if i>=ma-1:
                if dc[i]['macd']>=0 and dc[i-1]['macd']<0:
                    co+=1
                elif dc[i]['macd']<0 and dc[i-1]['macd']>=0:
                    co+=1
                dc[i]['reg']=co
                price=dc[i]['close']-dc[i]['open']
                std=dc[i]['std']
                if std:
                    dc[i]['mul']=round(price/std,2)

        data=None
        while 1:
            data=yield dc
            ind=len(dc)
            if isinstance(data,tuple):
                dc.append({'ema_short':0,'ema_long':0,'diff':0,'dea':0,'macd':0,'ma':0,'var':0,'std':0,'reg':0,'mul':0,'datetimes':data[0],'open':data[1],'close':data[4]})

                dc[ind]['ema_short'] = dc[ind-1]['ema_short'] * (short-2) / short + dc[ind]['close'] * 2 / short  # 当日EMA(12)
                dc[ind]['ema_long'] = dc[ind-1]['ema_long'] * (long-2) / long + dc[ind]['close'] * 2 / long  # 当日EMA(26)
                dc[ind]['diff'] = dc[ind]['ema_short'] - dc[ind]['ema_long']
                dc[ind]['dea'] = dc[ind-1]['dea'] * (phyd-2) / phyd + dc[ind]['diff'] * 2 / phyd
                dc[ind]['macd'] = 2 * (dc[ind]['diff'] - dc[ind]['dea'])

                dc[ind]['ma']=sum(dc[ind-j]['close'] for j in range(ma))/ma # 移动平均值
                dc[ind]['var']=sum((dc[ind-j]['close']-dc[ind]['ma'])**2 for j in range(ma))/ma # 方差
                dc[ind]['std']=float(np.sqrt(dc[ind]['var'])) # 标准差

                if dc[ind]['macd']>=0 and dc[ind-1]['macd']<0:
                    co+=1
                elif dc[ind]['macd']<0 and dc[ind-1]['macd']>=0:
                    co+=1
                dc[ind]['reg']=co
                price=dc[ind]['close']-dc[ind]['open']
                std=dc[ind]['std']
                if std:
                    dc[ind]['mul']=round(price/std,2)

    def main(self,_ma=60):
        res={}
        is_d=0
        is_k=0
        conn=get_conn('carry_investment')
        cur=conn.cursor()
        cur.execute('SELECT a.datetime,a.open,a.high,a.low,a.close FROM futures_min a INNER JOIN (SELECT DATETIME FROM futures_min ORDER BY DATETIME DESC LIMIT 0,{})b ON a.datetime=b.datetime'.format(_ma))
        data=cur.fetchall()
        data2=self.macd2(da=data,ma=_ma)
        dt2=data2.send(None)
        data=None
        while 1:
            data=yield res
            dates=data[0]
            res[dates]={'duo':0,'kong':0,'mony':0,'datetimes':[],'dy':0,'xy':0}
            str_time1=None if is_d==0 else str_time1
            str_time2=None if is_k==0 else str_time2
            jg_d=0 if is_d==0 else jg_d
            jg_k=0 if is_k==0 else jg_k

            # data格式：(datetime.datetime(2018, 3, 26, 20, 19), 30606.0, 30610.0, 30592.0, 30597.0)
            dt2=data2.send(data)[-1:][0]

            datetimes,clo,macd,mas,std,reg,mul=dt2['datetimes'],dt2['close'],dt2['macd'],dt2['ma'],dt2['std'],dt2['reg'],dt2['mul']
            if mul>1.5:
                res[dates]['dy']+=1
            if mul<-1.5:
                res[dates]['xy']+=1
            if clo>mas and mul>1.5 and is_d==0:
                jg_d=clo
                res[dates]['datetimes'].append([str_time1,1])
                is_d=1
            if clo<mas and mul<-1.5 and is_k==0:
                jg_k=clo
                res[dates]['datetimes'].append([str_time1,-1])
                is_k=-1
            if is_d==1 and macd<0 and clo<mas:
                res[dates]['duo']+=1
                res[dates]['mony']+=(clo-jg_d)
                res[dates]['datetimes'].append([str_time1,2])
                is_d=0
            if is_k==-1 and macd>0 and clo>mas:
                res[dates]['kong']+=1
                res[dates]['mony']+=(jg_k-clo)
                res[dates]['datetimes'].append([str_time1,-2])
                is_k=0


    def main2(self,_ma,_dates, _ts):
        res={}
        is_d=0
        is_k=0
        i=0
        send_nan=0
        dt3=None
        while i<_ts:
            dates=datetime.datetime.strptime(_dates,'%Y-%m-%d')+datetime.timedelta(days=i)
            if dates>datetime.datetime.now():
                break
            dates=str(dates)[:10]
            da=self.get_data(dates)
            if len(da)<1:
                i+=1
                send_nan+=1
                continue

            res[dates]={'duo':0,'kong':0,'mony':0,'datetimes':[],'dy':0,'xy':0}
            str_time1=None if is_d==0 else str_time1
            str_time2=None if is_k==0 else str_time2
            jg_d=0 if is_d==0 else jg_d
            jg_k=0 if is_k==0 else jg_k
            i+=1
            if i-send_nan==1:
                data2=self.macd2(da=da[:_ma],ma=_ma)
                dt2=data2.send(None)
                da=da[_ma:]
            for df2 in da:
                # df2格式：(Timestamp('2018-03-16 09:22:00') 31304.0 31319.0 31295.0 31316.0 275)
                dt3=data2.send(df2)
                dt2=dt3[-1:][0]
                datetimes,clo,macd,mas,std,reg,mul=dt2['datetimes'],dt2['close'],dt2['macd'],dt2['ma'],dt2['std'],dt2['reg'],dt2['mul']
                if mul>1.5:
                    res[dates]['dy']+=1
                if mul<-1.5:
                    res[dates]['xy']+=1
                if clo>mas and mul>1.5 and is_d==0:
                    jg_d=clo
                    str_time1=str(datetimes)
                    is_d=1
                if clo<mas and mul<-1.5 and is_k==0:
                    jg_k=clo
                    str_time2=str(datetimes)
                    is_k=-1
                if is_d==1 and macd<0 and clo<mas:
                    res[dates]['duo']+=1
                    res[dates]['mony']+=(clo-jg_d)
                    res[dates]['datetimes'].append([str_time1+'--'+str(datetimes),'多',clo-jg_d])
                    is_d=0
                if is_k==-1 and macd>0 and clo>mas:
                    res[dates]['kong']+=1
                    res[dates]['mony']+=(jg_k-clo)
                    res[dates]['datetimes'].append([str_time2+'--'+str(datetimes),'空',jg_k-clo])
                    is_k=0
        if dt3:
            self.macd_to_sql(dt3) # 存储到数据库

        return res

# if __name__=='__main__':
#     ma=60 # 设置均线时长
#     dates='2018-03-19' # 开始时间，这天必须有数据
#     ts=5 # 要测试的天数
#     main2(_ma=ma, _dates=dates, _ts=ts)

