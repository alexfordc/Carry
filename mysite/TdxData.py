#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/27 0027 10:31
# @Author  : Hadrianl 
# @File    : TdxData


from pytdx.exhq import TdxExHq_API, TDXParams
from pytdx.pool.ippool import AvailableIPPool
from pytdx.pool.hqpool import TdxHqPool_API
import pymongo as pmg
import pandas as pd
import re
from dateutil import parser
import datetime as dt

future_ip_list = [
    {'ip': '124.74.236.94', 'port': 7721},
    {'ip': '218.80.248.229', 'port': 7721},
    {'ip': '124.74.236.94', 'port': 7721},
    {'ip': '58.246.109.27', 'port': 7721},
    {'ip': '112.74.214.43', 'port': 7727, 'name': '扩展市场深圳双线1'},
    {'ip': '120.24.0.77', 'port': 7727, 'name': '扩展市场深圳双线2'},
    {'ip': '106.14.95.149', 'port': 7727, 'name': '扩展市场上海双线'},
    {'ip': '119.97.185.5', 'port': 7727, 'name': '扩展市场武汉主站1'},
    {'ip': '202.103.36.71', 'port': 443, 'name': '扩展市场武汉主站2'},
    {'ip': '59.175.238.38', 'port': 7727, 'name': '扩展市场武汉主站3'},
    {'ip': '113.105.142.136', 'port': 443, 'name': '扩展市场东莞主站'},
    # {'ip': '61.152.107.141', 'port': 7727, 'name': '扩展市场上海主站1'},
    # {'ip': '61.152.107.171', 'port': 7727, 'name': '扩展市场上海主站2'},
    # {'ip': '119.147.86.171', 'port': 7727, 'name': '扩展市场深圳主站'},
    {'ip': '47.92.127.181', 'port': 7727, 'name': '扩展市场北京主站'},
]


class Future:
    """  获取 通达信--国内期货数据"""
    _singleton = None
    _is_connect = False

    def __new__(cls, *args, **kwargs):
        if not cls._singleton:
            cls._singleton = super(Future, cls).__new__(cls)
        return cls._singleton

    def __init__(self, default_ips=None, ip_list=future_ip_list, host='192.168.2.226', port=27017):
        '''
        :param default_ips: (('112.74.214.43', 7727), ('120.24.0.77', 7727))  前者指主站IP，后者为热备IP, None为自动寻址
        :param ip_list: 所有的连接IP
        :param host: 本地mongodb数据库地址
        :param port: 本地mongodb数据库端口
        '''
        if not self._is_connect:
            self._is_connect = True
            self._init_(default_ips, ip_list, host, port)

    def _init_(self, default_ips=None, ip_list=future_ip_list, host='192.168.2.226', port=27017):
        ips = [(i['ip'], i['port']) for i in ip_list]
        self._ippool = AvailableIPPool(TdxExHq_API, ips)
        if default_ips is None:
            self._primary_ip, self._hot_backup_ip = self._ippool.sync_get_top_n(2)
        else:
            self._primary_ip, self._hot_backup_ip = default_ips
        print(f'初始化主连IP:{self._primary_ip}, 热备IP:{self._hot_backup_ip}')
        self._exapi = TdxHqPool_API(TdxExHq_API, self._ippool)
        self._mongodb_host = host
        self._mongodb_port = port
        self._conn = pmg.MongoClient(f'mongodb://{host}:{port}/')
        print(f'连接mongodb://{host}:{port}/')
        self._db = self._conn.get_database('Future')
        self._col = self._db.get_collection('future_1min')

    def refresh_available_ips(self, ips=None):  # 寻找最快连接主站和热备IP
        '''

        :param ips: (('112.74.214.43', 7727), ('120.24.0.77', 7727))  前者指主站IP，后者为热备IP
        :return:
        '''
        if ips is None:
            self._primary_ip, self._hot_backup_ip = self._ippool.sync_get_top_n(2)
        else:
            self._primary_ip, self._hot_backup_ip = ips

    def get_local_code_list(self):  # 获取本地合约列表
        code_list = self._col.distinct('code')
        return code_list

    def get_all_dominant(self):  # 获得服务器中所有的主力合约
        dominant_code_list = []
        with self._exapi.connect(self._primary_ip, self._hot_backup_ip):
            for md in [28, 29, 30, 47]:
                i = 0
                m = []
                while True:
                    ret = self._exapi.get_instrument_quote_list(md, 3, start=i, count=80)
                    if ret:
                        m_c = [(r['market'], r['code']) for r in ret if 'L8' in r['code']]
                        m.extend(m_c)
                        i += len(ret)
                    else:
                        dominant_code_list.extend(m)
                        break

        return dominant_code_list

    def get_all_futures(self):  # 获得服务器中所有的期货合约
        futures_list = []
        with self._exapi.connect(self._primary_ip, self._hot_backup_ip):
            for md in [28, 29, 30, 47]:
                i = 0
                m = []
                while True:
                    ret = self._exapi.get_instrument_quote_list(md, 3, start=i, count=80)
                    if ret:
                        m_c = [(r['market'], r['code']) for r in ret]
                        m.extend(m_c)
                        i += len(ret)
                    else:
                        futures_list.extend(m)
                        break

        return futures_list

    def get_bar(self, code, start=None, end=None, ktype='1m'):
        '''
        获取k线数据，历史k线从数据库拿，当日从tdx服务器提取,按天提取数据
        :param code: 代码
        :param start: None为从最开始查询
        :param end: None为查询到最新的bar
        :param ktype: [1, 5, 15, 30, 60]分钟m或者min,或者1D
        :return:
        '''
        if isinstance(start, str):
            start = parser.parse(start)

        if isinstance(end, str):
            end = parser.parse(end)
            end = end + dt.timedelta(days=1)

        ktype = self.__check_ktype(ktype)
        fields = ['datetime', 'open', 'high', 'low', 'close', 'position', 'trade', 'price', 'code', 'market']
        cursor = self._col.find(
            {'code': code, 'datetime': {'$gte': start if start is not None else dt.datetime(1970, 1, 1),
                                        '$lt': end if end is not None else dt.datetime(2999, 1, 1)}}, fields)
        history_bar = [d for d in cursor]  # 从本地服务器获取历史数据
        df = pd.DataFrame(history_bar, columns=fields)

        if (end is None) or (end.date() >= dt.date.today()):  # 从TDX服务器获取当日数据
            try:
                market = self._col.find_one({'code': code}, {'market': 1})['market']
                with self._exapi.connect(self._primary_ip, self._hot_backup_ip):
                    ret = self._exapi.get_instrument_bars(TDXParams.KLINE_TYPE_EXHQ_1MIN, market, code, 0, 800)
            except:
                self._init_(ip_list=future_ip_list, host='192.168.2.226', port=27017)
                market = self._col.find_one({'code': code}, {'market': 1})['market']
                with self._exapi.connect(self._primary_ip, self._hot_backup_ip):
                    ret = self._exapi.get_instrument_bars(TDXParams.KLINE_TYPE_EXHQ_1MIN, market, code, 0, 800)
            if ret:
                df_latest_800bars = pd.DataFrame(ret, columns=fields)
                df_latest_800bars['market'] = market
                df_latest_800bars['code'] = code
                df_latest_800bars['datetime'] = pd.to_datetime(df_latest_800bars['datetime'])

            df = df.append(df_latest_800bars, ignore_index=True)
            df.drop_duplicates('datetime', inplace=True)

        df.set_index('datetime', drop=False, inplace=True)
        _t = df['datetime'].apply(self.__sort_bars)
        df['_t'] = _t
        df.sort_values('_t', inplace=True)

        def _resample(x):
            _dt = pd.date_range(x.index[0].date(), x.index[0].date() + dt.timedelta(days=1), freq='T')[:len(x)]
            x['_t_temp'] = _dt
            apply_func_dict = {'datetime': 'last',
                               'open': 'first',
                               'high': 'max',
                               'low': 'min',
                               'close': 'last',
                               'position': 'last',
                               'trade': 'sum',
                               'price': 'mean',
                               'code': 'first',
                               'market': 'first',
                               '_t': 'last'
                               }
            resampled = x.resample(ktype, on='_t_temp').apply(apply_func_dict)
            if ktype == '1D':
                resampled.index = resampled['datetime'].apply(lambda x: x.date())
            else:
                resampled.set_index('datetime', drop=False, inplace=True)
            return resampled

        resampled_df = df.groupby(by=lambda x: x.date()).apply(_resample)
        if isinstance(resampled_df.index, pd.MultiIndex):
            resampled_df.reset_index(0, drop=True, inplace=True)

        return resampled_df

    @staticmethod
    def __sort_bars(_dt):
        if _dt.time() > dt.time(18, 0):
            _dt = _dt - dt.timedelta(days=1)
        return _dt.timestamp()

    @staticmethod
    def __check_ktype(ktype):
        _ktype = re.findall(r'^(\d+)([a-zA-Z]+)$', ktype)[0]
        if _ktype:
            _n = int(_ktype[0])
            _t = _ktype[1].lower()
            if _t in ['m', 'min']:
                _t = 'T'
                if _n not in [1, 5, 15, 30, 60]:
                    raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')
            elif _t in ['d', 'day']:
                _t = 'D'
                if _n not in [1]:
                    raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')
            else:
                raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')
        else:
            raise Exception(f'不支持{ktype}类型, 请输入正确的ktype!')

        return f'{_n}{_t}'


if __name__ == '__main__':
    future = Future()
    # 获取60分钟的数据
    data = future.get_bar('RB1810', start='2018-08-10', end='2018-08-27', ktype='60m')
    print(data)