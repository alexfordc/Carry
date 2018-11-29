import json
import requests
import pandas as pd
import numpy as np
import re
import time
import datetime
import xlrd
import sys
import math
import os
import pickle

from django.core.cache import cache
from io import BytesIO
from sqlalchemy import create_engine
from pyquery import PyQuery as pq
from threading import Thread
from collections import defaultdict
from contextlib import contextmanager

from mysite import HSD
from mysite.mycaptcha import Captcha
from mysite import pypass


def asyncs(func):
    """ 执行不需要返回结果的程序，每执行一次程序添加一个线程
    """

    def wrapper(*args, **kwargs):
        t = Thread(target=func, args=args, kwargs=kwargs)
        t.start()

    return wrapper


def caches(func):
    """ 缓存装饰器 """
    data = {}

    def wrapper(*args, **kwargs):
        key = f'{func.__name__}{args}{kwargs}'
        if key not in data:
            data[key] = func(*args, **kwargs)
        return data[key]

    return wrapper


@asyncs
def record_log(files, info, types):
    """ 写入文件 """
    if types == 'w':
        with open(files, 'w') as f:
            f.write(json.dumps(info))
    elif types == 'a':
        info = f'{datetime.datetime.now()}----{info}'
        with open(files, 'a') as f:
            f.write(info)


@contextmanager
def errors(*fun_name):
    """ 处理异常 """
    try:
        yield
    except Exception as exc:
        record_log('log\\error_log\\err.txt', f'{fun_name}{exc}\n', 'a')


@caches
class Dict(dict):
    """ 有超时处理的字典 """

    def __init__(self, expiry=7200):
        """ expiry: 保存时间，默认7200秒(2小时) """
        self.data = {}
        self.expiry = expiry

    def get(self, key):
        if key not in self.data:
            return None
        if time.time() - self.data[key][1] < self.data[key][2]:
            return self.data[key][0]

    def __getitem__(self, key):
        if key not in self.data:
            return None
        if time.time() - self.data[key][1] < self.data[key][2]:
            return self.data[key][0]

    def __setitem__(self, key, value):
        self.data[key] = (value, time.time(), self.expiry)

    def setdefault(self, key, value):
        self.data[key] = (value, time.time(), self.expiry)

    def delete(self, key):
        if key in self.data:
            del self.data[key]


@caches
def get_dirsize(dir, task):
    """ 获取文件夹大小(bytes) """
    size = 0
    for root, dirs, files in os.walk(dir):
        size += sum([os.path.getsize(os.path.join(root, name)) for name in files])
    return size


def error_log(files, line, exc):
    """ 错误日志 """
    HSD.logging.error("文件：{} 第{}行报错： {}".format(files, line, exc))


def tongji_huice(res, huizong):
    """ 模拟 与 实盘的回测 """
    all_price = []
    res_key = list(res.keys())
    for i in res_key:
        mony = res[i]['mony']
        huizong['yk'] += mony
        huizong['zl'] += (res[i]['duo'] + res[i]['kong'])

        mtsl = [j[3] for j in res[i]['datetimes']]
        all_price += mtsl
        if 'ylds' not in res[i]:
            res[i]['ylds'] = 0
        if mtsl:
            ylds = len([sl for sl in mtsl if sl > 0])
            res[i]['ylds'] += ylds  # 盈利单数
            res[i]['shenglv'] = round(ylds / len(mtsl) * 100, 2)  # 每天胜率
        else:
            res[i]['shenglv'] = 0

    huizong['shenglv'] += len([p for p in all_price if p > 0])
    huizong['shenglv'] = int(huizong['shenglv'] / huizong['zl'] * 100) if huizong['zl'] > 0 else 0  # 胜率
    huizong['avg'] = huizong['yk'] / huizong['zl'] if huizong['zl'] > 0 else 0  # 平均每单盈亏
    res_size = len(res)
    huizong['avg_day'] = huizong['yk'] / res_size if res_size > 0 else 0  # 平均每天盈亏
    # huizong['least2'] = min(all_price)
    # huizong['most2'] = max(all_price)
    return res, huizong


@caches
def thisday_transaction(thisday):
    """ 查看当天期货是否有交易，返回当天的日期是否不等于最后的交易日期 """
    """
        http://blog.sina.com.cn/s/blog_404ee5f90102xxbs.html  # 接口讲解地址
        以豆粕为例(MO)
        实时数据：http://hq.sinajs.cn/list=MO
        5分钟：http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=M0
        15分钟：http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine15m?symbol=M0
        30分钟：http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine30m?symbol=M0
        60分钟：http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine60m?symbol=M0
        日线：http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol=M0
    """
    url = "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=M0"
    data = requests.get(url).text
    data = json.loads(data)
    date = data[0][0][:10]
    return date != thisday


def tongji_ud(page, rq_date, end_date):
    """ 交易统计表上一天、下一天计算"""
    if page == 'up' and rq_date and end_date:
        rq_date = datetime.datetime.strptime(rq_date, '%Y-%m-%d')
        rq_week = rq_date.weekday()
        if rq_week == 0:
            rq_date = str(rq_date + datetime.timedelta(days=-3))[:10]
        elif rq_week == 6:
            rq_date = str(rq_date + datetime.timedelta(days=-2))[:10]
        else:
            rq_date = str(rq_date + datetime.timedelta(days=-1))[:10]
        end_date = rq_date
    elif page == 'down' and rq_date and end_date:
        rq_date = datetime.datetime.strptime(rq_date, '%Y-%m-%d')
        rq_week = rq_date.weekday()
        if rq_week == 4:
            rq_date = str(rq_date + datetime.timedelta(days=3))[:10]
        elif rq_week == 5:
            rq_date = str(rq_date + datetime.timedelta(days=2))[:10]
        else:
            rq_date = str(rq_date + datetime.timedelta(days=1))[:10]
        end_date = rq_date
    return rq_date, end_date


@asyncs
def gxjy_refresh(h, folder1, folder2):
    """ 国信国内期货交易刷新 """
    data = h.gx_lsjl(folder1)
    data = data.fillna('')
    data = data.sort_values(['日期', '成交时间'])
    h.to_sql(data, 'gx_record')
    data = h.gx_lsjl(folder2)
    h.to_sql(data, 'gx_entry_exit')


def get_cfmmc_trade(host=None, start_date=None, end_date=None):
    """ 国内期货数据，交易记录 """
    # 合约, 成交序号, 成交时间, 买/卖, 投机/套保, 成交价, 手数, 成交额, 开/平, 手续费, 平仓盈亏, 实际成交日期, 帐号, 交易日期
    if host is None:
        # sql = "SELECT 合约,成交序号,DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,
        # 手续费,平仓盈亏,DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_
        # records WHERE 交易日期 IN (SELECT MAX(交易日期) FROM cfmmc_trade_records GROUP BY 帐号) GROUP BY 帐号"
        sql = (
            "SELECT 合约,(CASE WHEN LENGTH(成交序号)>8 THEN SUBSTR(成交序号,9,16) ELSE 成交序号 END),"
            "DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,"
            "DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') "
            "FROM cfmmc_trade_records GROUP BY 交易日期,帐号 ORDER BY 交易日期 DESC"
        )
    elif start_date and end_date:
        sql = (
            "SELECT 合约,(CASE WHEN LENGTH(成交序号)>8 THEN SUBSTR(成交序号,9,16) ELSE 成交序号 END),"
            "DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,"
            "DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records "
            f"WHERE 帐号='{host}' AND 实际成交日期>='{start_date}' AND 实际成交日期<='{end_date}' "
            "ORDER BY 实际成交日期 DESC,成交时间 DESC"
        )
    else:
        # end_date = datetime.datetime.now()
        # start_date = end_date - datetime.timedelta(days=6)
        # end_date = str(end_date)[:10]
        # start_date = str(start_date)[:10]
        sql = (
            "SELECT 合约,(CASE WHEN LENGTH(成交序号)>8 THEN SUBSTR(成交序号,9,16) ELSE 成交序号 END),"
            "DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,"
            "DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records "
            f"WHERE 帐号='{host}' ORDER BY 实际成交日期 DESC,成交时间 DESC limit 30"
        )
        # sql = f"SELECT 合约,成交序号,DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,
        # `开/平`,手续费,平仓盈亏,DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM
        # cfmmc_trade_records WHERE 帐号='{host}' AND 实际成交日期>='{start_date}' AND 实际成交日期<='{end_date}'
        # ORDER BY 实际成交日期 DESC,成交时间 DESC"
    data = HSD.runSqlData('carry_investment', sql)
    return data


def get_sqlalchemy_conn():
    """ 返回sqlalchemy MySQL数据库连接 """
    us = HSD.get_config("U", "us")
    ps = HSD.get_config("U", "ps")
    hs = HSD.get_config("U", "hs")
    _conn = create_engine(f'mysql+pymysql://{us}:{ps}@{hs}:3306/carry_investment?charset=utf8')
    return _conn


class Cfmmc:
    """ 期货监控系统，登录，下载数据，保存数据 """
    __slots__ = ('session', '_login_url', '_vercode_url', '_conn', '_not_trade_list', '_userID', '_password', 'key')

    def __init__(self, key):
        # requests.packages.urllib3.disable_warnings()  # 禁用警告：正在进行未经验证的HTTPS请求。 强烈建议添加证书验证。
        self.session = requests.session()
        self.key = key
        red = HSD.RedisPool()
        ses = red.get(key)
        if ses:
            [self.session.cookies.set(k, v) for k, v in ses.items()]
        self._login_url = 'https://investorservice.cfmmc.com/login.do'
        self._vercode_url = 'https://investorservice.cfmmc.com/veriCode.do?t='

    def getToken(self, url):
        """获取token"""
        token_name = "org.apache.struts.taglib.html.TOKEN"
        ret = self.session.get(self._login_url)
        ret_text = pq(ret.text)
        red = HSD.RedisPool()
        red.set(self.key, {k: v for k, v in self.session.cookies.items()}, 600)
        for x in ret_text('input'):
            if x.name == token_name:
                return x.value

    def getCode(self):
        """ 获取验证码 """
        t = int(datetime.datetime.now().timestamp() * 1000)
        vercode_url = f'{self._vercode_url}{t}'
        response = self.session.get(vercode_url)
        # from PIL import Image
        # image = Image.open(BytesIO(response.content))
        # image.show()
        return response.content

    def _get_not_trade_date(self):
        """获取非交易日"""
        t = int(datetime.datetime.now().timestamp() * 1000)
        url = f'https://investorservice.cfmmc.com/script/tradeDateList.js?t={t}'
        ret = self.session.get(url)
        if ret.ok:
            # print('获取非交易日成功')
            self._not_trade_list = eval(re.search('\[.*\]', ret.content.decode())[0])
        else:
            self._not_trade_list = []

    def login(self, userID, password, token, vercode):
        """ 用户登录 """
        self._userID = userID
        self._password = password
        data = {
            "org.apache.struts.taglib.html.TOKEN": token,
            "userID": userID,
            "password": password,
            "vericode": vercode,
        }
        # headers = {
        #      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        #      'Accept-Encoding': 'gzip, deflate, br',
        #      'Accept-Language': 'zh-CN,zh;q=0.9',
        #      'Cache-Control': 'max-age=0',
        #      'Connection': 'keep-alive',
        #      'Host': 'investorservice.cfmmc.com',
        #      'Upgrade-Insecure-Requests': '1',
        #      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        # }
        ret = self.session.post(self._login_url, data=data, verify=False, timeout=5)
        if ret.ok:
            # print('成功登录')
            self._get_not_trade_date()
        successful_landing = False
        d = ret.text
        p = pq(d)
        v = p('.formtext>font')
        if not v:
            successful_landing = True
        else:
            with errors('Cfmmc', 'login'):
                successful_landing = v[0].text.replace('\r', '').replace('\n', '').replace('\t', '').strip()

        return successful_landing

    def logout(self):
        """ 退出登录 """
        logout_url = 'https://investorservice.cfmmc.com/logout.do'
        data = {
            "deleteCookies": 'N',
            "logout": "退出系统"}
        ret = self.session.post(logout_url, data=data, verify=False, timeout=5)
        if ret.ok:
            # print('成功登出')
            return True

    def read_name(self, ret_content):
        """ 获取 xls 表里面的名字 """
        # newwb = xlrd.open_workbook(ret_content)
        # table = newwb.sheets()[0]  # 第一张表
        # rows = table.nrows  # 行数
        # print_name = False
        # for i in range(rows):
        #     for j in table.row_values(i):
        #         if print_name and j.strip():
        #             return j
        #         if j == '客户名称':
        #             print_name = True
        with errors('Cfmmc', 'read_name'):
            p = pd.read_excel(BytesIO(ret_content))
            name = p[p.ix[:, 0] == '客户名称'].ix[:, 2].values[0]
            return name

    def save_settlement(self, tradeDate, byType, name):
        """ 请求并保存某一天（tradeDate：2018-08-08），"""
        daily = 'https://investorservice.cfmmc.com/customer/setupViewCustomerDetailFromCompanyWithExcel.do'
        month = 'https://investorservice.cfmmc.com/customer/setupViewCustomerMonthDetailFromCompanyWithExcel.do'
        sp = 'https://investorservice.cfmmc.com/customer/setParameter.do'
        _t = self.getToken(sp)
        self.session.post(sp,
                          data={"org.apache.struts.taglib.html.TOKEN": _t, 'tradeDate': tradeDate, 'byType': byType})
        if len(tradeDate) == 10:
            if tradeDate in self._not_trade_list:
                # raise Exception(f'{tradeDate}为未非交易日')
                return '_fail'
            ret = self.session.post(daily)
        elif len(tradeDate) == 7:
            ret = self.session.post(month)
        else:
            # raise Exception('请输入正确的tradeDate')
            return '_fail'
        if ret.status_code != 200:
            # raise Exception('请求错误')
            return '_fail'
        else:
            try:
                f_name = ret.headers['Content-Disposition'].strip('attachment; filename=')
                r_name, _ = f_name.split('.')
                _account, _tradedate = r_name.split('_')
                #                 with open(f_name, 'wb') as f:
                #                     f.write(ret.content)
                #                 print(f'{tradeDate}的{byType}数据下载成功')

                ret_content = ret.content

                # 查找用户的真实名称
                if name is None:
                    name = self.read_name(ret_content)
                else:
                    name = None

                trade_records = pd.read_excel(BytesIO(ret_content), sheetname='成交明细', header=9, na_values='--',
                                              dtype={'成交序号': np.str_, '平仓盈亏': np.float})
                trade_records.drop([trade_records.index[-1]], inplace=True)
                closed_position = pd.read_excel(BytesIO(ret_content), sheetname='平仓明细', header=9,
                                                dtype={'成交序号': np.str_, '原成交序号': np.str_})
                closed_position.drop([closed_position.index[-1]], inplace=True)
                holding_position = pd.read_excel(BytesIO(ret_content), sheetname='持仓明细', header=9,
                                                 dtype={'成交序号': np.str_, '交易编码': np.str_})
                holding_position.drop([holding_position.index[-1]], inplace=True)

                # 客户交易结算日报
                account_info = pd.read_excel(BytesIO(ret.content), sheetname='客户交易结算日报', header=4)
                # _i = account_info[account_info.iloc[:, 0] == '期货期权账户资金状况'].index[0]
                _i = account_info.index[account_info.iloc[:, 0] == '期货期权账户资金状况'][0]
                account_info_1 = account_info.iloc[_i + 1:_i + 7, [0, 2]]
                account_info_1.columns = ['field', 'info']
                account_info_2 = account_info.iloc[_i + 1:_i + 10, [5, 7]]
                account_info_2.columns = ['field', 'info']
                account_info = account_info_1.append(account_info_2).set_index('field').T
                account_info['风险度'] = account_info['风险度'].apply(lambda x: float(x.strip('%')) / 100)

                for df in [trade_records, closed_position, holding_position, account_info]:
                    df['帐号'] = _account
                    df['交易日期'] = _tradedate

                excs = ''
                _conn = get_sqlalchemy_conn()  # 获取数据库连接
                if byType == 'date':
                    try:
                        account_info.to_sql('cfmmc_daily_settlement', _conn, schema='carry_investment',
                                            if_exists='append',
                                            index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        trade_records.to_sql('cfmmc_trade_records', _conn, schema='carry_investment',
                                             if_exists='append',
                                             index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        closed_position.to_sql('cfmmc_closed_position', _conn, schema='carry_investment',
                                               if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        holding_position.to_sql('cfmmc_holding_position', _conn, schema='carry_investment',
                                                if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)
                elif byType == 'trade':
                    try:
                        account_info.to_sql('cfmmc_daily_settlement_trade', _conn, schema='carry_investment',
                                            if_exists='append',
                                            index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        trade_records.to_sql('cfmmc_trade_records_trade', _conn, schema='carry_investment',
                                             if_exists='append',
                                             index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        closed_position.to_sql('cfmmc_closed_position_trade', _conn, schema='carry_investment',
                                               if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        holding_position.to_sql('cfmmc_holding_position_trade', _conn, schema='carry_investment',
                                                if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)

                # print(f'{tradeDate}的{byType}数据下载成功')
                if excs == '':
                    sql = 'insert into cfmmc_insert_date(host,date,type) values(%s,%s,%s)'
                    tt = 0 if byType == 'date' else (1 if byType == 'trade' else -1)
                    HSD.runSqlData('carry_investment', sql, (_account, _tradedate, tt))
                else:
                    record_log('log\\error_log\\err.txt', f'[viewUtil | Cfmmc.save_settlement]{tradeDate}{excs}\n',
                               'a')
                return name
            except Exception as e:
                # print(f'{tradeDate}的{byType}数据下载失败\n{e}')
                record_log('log\\error_log\\err.txt', f'{tradeDate}的{byType}数据下载失败\n{e}\n', 'a')
                return '_fail'

    @asyncs
    def down_day_data_sql(self, host, start_date, end_date, password=None, createTime=None):
        """ 下载啄日数据并保存到MySQL """
        cache.set('cfmmc_status' + host, 'start')
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        days = (end_date - start_date).days + 1
        name = None
        is_success = False
        is_run = False
        if password and createTime:
            try:
                sql = f"SELECT name FROM cfmmc_user WHERE host='{host}'"
                name = HSD.runSqlData('carry_investment', sql)
                name = name[0][0]
            except:
                name = None
        for byType in ['date', 'trade']:
            try:
                types = 0 if byType == 'date' else 1
                sql = "SELECT date FROM cfmmc_insert_date WHERE host='{}' AND type={}".format(host, types)
                dates = HSD.runSqlData('carry_investment', sql)
                dates = [str(i[0]) for i in dates]
            except:
                dates = []
            with errors('Cfmmc', 'down_day_data_sql'):
                for d in range(days):
                    date = start_date + datetime.timedelta(d)
                    date = str(date)[:10]
                    if date not in self._not_trade_list and date not in dates:
                        name = self.save_settlement(date, byType, name)
                        is_run = True
                        if name and name != '_fail':
                            sql = (
                                "INSERT INTO cfmmc_user(host,password,cookie,download,name,creationTime) "
                                f"VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE name='{name}'"
                            )
                            HSD.runSqlData('carry_investment', sql, (host, password, '', 1, name, createTime))
                            is_success = True
                            cache.set('cfmmc_status' + host, f"{date}日前的数据更新成功！")
                        time.sleep(0.1)

        s = ('True' if is_success else ('False' if is_run else 'not_run'))
        cache.set('cfmmc_status' + host, s)


def get_Cfmmc(red, cd_):
    cf = Cfmmc()
    red.set(cd_, cf, _object=True)
    return cf


class Automatic:
    _singleton = None
    _no_start = True

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super(Automatic, cls).__new__(cls)
        return cls._singleton

    def __init__(self):
        if self._no_start:
            self._no_start = False
            self.cfmmc_dsqd()

    @asyncs
    def cfmmc_dsqd(self):
        """ 自动运行下载数据 期货监控系统数据 """
        sql = 'SELECT HOST,PASSWORD,creationTime FROM cfmmc_user WHERE download=1'
        while 1:
            with errors('Automatic', 'cfmmc_dsqd'):
                last_date = cache.get('cfmmc_Automatic_download')
                break

            time.sleep(10)
        last_date = last_date if last_date else 0
        print(f'自动下载开始运行...{datetime.datetime.now()}')
        computer_name = os.environ['COMPUTERNAME'].upper()
        if computer_name == 'DOC':
            model_path = r'D:\tools\Tools\Carry\mysite\myfile'
        else:
            model_path = r'D:\Carry\mysite\myfile'
        with open(model_path + '\\' + 'cfmmc_dsqd_log.txt', 'a') as f:
            f.write(f'自动下载开始运行...{datetime.datetime.now()}\n')
        while 1:
            t = time.localtime()
            d = datetime.datetime.now()
            if d.weekday() < 5:
                n = (60 - t.tm_min) * 60
            else:
                n = 60 * 60 * 6
            if t.tm_hour == 18 and last_date != t.tm_yday:
                last_date = t.tm_yday
                ca = Captcha(model_path + '\\' + 'captcha_model95')
                cfmmc_login_d = Cfmmc('_Automatic_')
                data = HSD.runSqlData('carry_investment', sql)
                is_rest = thisday_transaction(str(d)[:10])  # 今天是否休市
                for da in data:
                    sql_date = (
                        "SELECT DATE_FORMAT(DATE,'%Y-%m-%d') FROM cfmmc_insert_date "
                        f"WHERE HOST='{da[0]}' ORDER BY DATE DESC LIMIT 1"
                    )
                    is_down_date = HSD.runSqlData('carry_investment', sql_date)
                    if d.weekday() > 4 or (is_down_date and is_down_date[0][0] == str(d)[:10]) or is_rest:
                        continue
                    if computer_name == 'DOC':  # 防止重复登录，在本机上不执行
                        continue
                    for i in range(20):  # 每个账号最多尝试登录20次
                        with errors('Automatic', 'cfmmc_dsqd'):
                            token = cfmmc_login_d.getToken(cfmmc_login_d._login_url)  # 获取token
                            code = cfmmc_login_d.getCode()  # 获取验证码
                            code = ca.check.send(BytesIO(code))  # 验证码
                            password = pypass.cfmmc_decode(da[1], da[2])  # 密码
                            success = cfmmc_login_d.login(da[0], password, token, code)  # 登录，返回成功与否
                            if success is True:
                                print(f"{da[0]} 第{i}次登录成功！")
                                with open(model_path + '\\' + 'cfmmc_dsqd_log.txt', 'a') as f:
                                    f.write(f"{da[0]} 第{i}次登录成功！{d}\n")
                                trade = get_cfmmc_trade(host=da[0])
                                if trade:  # 若已经有下载过数据，则下载3天之内的
                                    # start_date = str(trade[-1][11])
                                    _start_date = HSD.get_date(-3)
                                    end_date = trade[0][13]
                                    start_date = _start_date if _start_date < end_date else end_date
                                    end_date = HSD.get_date()
                                    cfmmc_login_d.down_day_data_sql(da[0], start_date, end_date, password, da[2])
                                    for run_time in range(300):
                                        s = cache.get('cfmmc_status' + da[0])
                                        if s in ('True', 'False', 'not_run'):
                                            break
                                        time.sleep(1)
                                else:  # 若没下载过数据，则下载300天之内的
                                    start_date = HSD.get_date(-300)
                                    end_date = HSD.get_date()
                                    cfmmc_login_d.down_day_data_sql(da[0], start_date, end_date, password, da[2])
                                break
                            if i > 17:
                                with open(model_path + '\\' + 'cfmmc_dsqd_log.txt', 'a') as f:
                                    f.write(f"{da[0]} 第{i}次登录失败！{d}\n")

                        time.sleep(0.2)
                    time.sleep(5)
                cache.set('cfmmc_Automatic_download', last_date)
            time.sleep(n)


def cfmmc_data_page(rq, start_date=None, end_date=None):
    """ 期货监控系统 展示页面 数据返回"""
    if start_date is None or end_date is None:
        start_date = rq.GET.get('start_date')
        end_date = rq.GET.get('end_date')
    try:
        host = rq.session['user_cfmmc']['userID']

        if start_date and end_date and '20' in start_date and '20' in end_date:
            trade = get_cfmmc_trade(host, start_date, end_date)
        else:
            trade = get_cfmmc_trade(host=host)
            end_date = str(trade[0][11])  # HSD.get_date()
        start_date = str(trade[-1][11])
    except:
        trade = []
        start_date = ''
        end_date = ''
    return trade, start_date, end_date


def cfmmc_id_hostName():
    """ 期货监控系统 id：host，host：id 双向字典 """
    sql = "SELECT id,host,name FROM cfmmc_user"
    hosts = HSD.runSqlData('carry_investment', sql)
    id_host = {}
    for i in hosts:
        id_host[str(i[0])] = i[1]
        id_host[i[1]] = i[0]
        id_host[i[1] + '_name'] = i[2]
    return id_host


def cfmmc_code_name(codes):
    """ 期货监控系统 获取产品代码对应的中文名称 """
    _code = lambda c: re.search('\d+', c)[0] if re.search('\d+', c) else ''
    codes = [(re.sub('\d', '', i), _code(i)) for i in codes]
    code_name = {''.join(n): HSD.FUTURE_NAME.get(n[0], n[0]) + n[1] for n in codes}
    return code_name


def user_work_log(rq, table, user=None, size=5):
    """ 工作日志分页
        rq：页面请求，
        table：models的表对象，
        user：用户，
        size：没有显示的条目，默认5条
     """
    try:
        curPage = int(rq.GET.get('curPage', '1'))  # 第几页
        allPage = int(rq.GET.get('allPage', '0'))  # 总页数
        pageType = str(rq.GET.get('pageType', ''))  # 上/下页
    except:
        curPage = 1
        allPage = 0
        pageType = ''  # 若有误,则给其默认值
    if curPage == 1 and allPage == 0:  # 只在第一次查询商品总条数
        goodCount = table.objects.count() if user is None else table.objects.filter(belonged=user).count()
        allPage = goodCount // size if goodCount % size == 0 else goodCount // size + 1
    if pageType == 'pageDown':  # 下一页
        curPage += 1
    elif pageType == 'pageUp':  # 上一页
        curPage -= 1
    if curPage < 1:
        curPage = 1  # 如果小于最小则等于1
    elif curPage > allPage:
        curPage = allPage  # 若大于最大则等于最大页
    startGood = (curPage - 1) * size  # 切片开始处
    endGood = startGood + size  # 切片结束处
    if allPage < 1:
        return [], 0, 0
    if user is None:
        work = table.objects.all()[startGood:endGood]
    else:
        work = table.objects.filter(belonged=user)[startGood:endGood]
    return work, allPage, curPage


def cfmmc_hc_data(host, rq_date, end_date):
    """ 期货监控系统 回测数据 """
    results2 = HSD.cfmmc_get_result(host, rq_date, end_date)
    # results2：[['016681702757', 'RB1810', '2018-06-01 11:05:07', 3749.0, '2018-06-04 21:32:58', 3754.0, -250, '空', 5, '已平仓'],...]
    res = {}
    huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
               'avg_day': 0, 'least2': [0, 1000, 0, 0], 'most2': [0, -1000, 0, 0]}
    pinzhong = []  # 所有品种
    min_date = ''
    max_date = ''
    for i0, i1, i2, i3, i4, i5, i6, i7, i8, i9 in results2:
        if not i5:
            continue
        if i1 not in pinzhong:
            pinzhong.append(i1)
        dt = i4[:10]
        if min_date == '' or i2[:10] < min_date:
            min_date = i2[:10]
        if max_date == '' or dt > max_date:
            max_date = dt
        if dt not in res:
            res[dt] = {'duo': 0, 'kong': 0, 'mony': 0, 'shenglv': 0, 'ylds': 0, 'datetimes': []}
        if i7 == '多':
            res[dt]['duo'] += 1
            _ykds = i5 - i3  # 盈亏点数
        elif i7 == '空':
            res[dt]['kong'] += 1
            _ykds = i3 - i5  # 盈亏点数
        res[dt]['mony'] += i6
        xx = [i2, i4, i7, i6, i3, i5, i8, i1]
        res[dt]['datetimes'].append(xx)

        huizong['least'] = [dt, i6] if i6 < huizong['least'][1] else huizong['least']
        huizong['least2'] = [dt, _ykds] if _ykds < huizong['least2'][1] else huizong['least2']
        huizong['most'] = [dt, i6] if i6 > huizong['most'][1] else huizong['most']
        huizong['most2'] = [dt, _ykds] if _ykds > huizong['most2'][1] else huizong['most2']
    money_sql = (
        f"SELECT 上日结存,当日存取合计 FROM cfmmc_daily_settlement WHERE 帐号='{host}' AND "
        f"(当日存取合计!=0 OR 交易日期 IN (SELECT MIN(交易日期) FROM cfmmc_daily_settlement WHERE 帐号='{host}'))"
    )
    moneys = HSD.runSqlData('carry_investment', money_sql)
    init_money = sum(j1 if i != 0 else j0 for i, (j0, j1) in enumerate(moneys))
    init_money = init_money if init_money and init_money > 10000 else 10000  # 入金
    hcd = None
    # if rq_date == end_date:  # 暂时取消一天的，或需要完善
    #     hcd = HSD.huice_day(res, init_money, real=True)

    _re = re.compile(r'[A-z]+')
    _pz = set(re.search(_re, i)[0] for i in pinzhong)
    _pz = [i + 'L8' for i in _pz]
    _redis = HSD.RedisPool()
    pinzhong = _redis.get('cfmmc_hc_data_pinzhong')
    if not pinzhong:
        pinzhong = {}
    mongo = HSD.MongoDBData()
    is_cache_set = False
    for p in _pz:
        if p not in pinzhong or pinzhong[p]['min_date'] > min_date or pinzhong[p]['max_date'] < max_date:
            pzs = mongo.get_data(p, min_date, max_date)
            pzs = {str(i)[:-3]: j for j, (i, *_) in enumerate(pzs)}
            pzs['min_date'] = min_date
            pzs['max_date'] = max_date
            pinzhong[p] = pzs
            is_cache_set = True
    if is_cache_set:
        _redis.set('cfmmc_hc_data_pinzhong', pinzhong)
    res, huizong = tongji_huice(res, huizong)
    hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date, pinzhong)

    return hc, hcd, huizong, init_money


def future_data_cycle(data, bs, cycle):
    """ data：精确到分钟的行情数据，bs：精确到秒的交易数据，
        cycle：若等于1D就是日线，其他为数字（以分钟为单位）"""
    bs0 = defaultdict(list)
    [bs0[i[0][:-3]].append(list(i)) for i in bs]
    bs = bs0
    bs2 = {}
    _bs = []
    _ts = set()
    if cycle == '1D':  # 日线
        for t0, o1, c2, l3, h4, v5 in data:
            j0 = str(t0)
            ts = j0[:10]
            if ts not in _ts:
                if _ts:
                    if _bs:
                        bs2[t] = _bs
                        _bs = []
                    yield [t, o, c, l, h, v], bs2
                _ts.add(ts)
                t = j0[:10] + ' 00:00:00'
                o = o1
                l = l3
                h = h4
                v = v5
                j0_3 = j0[:-3]
                if j0_3 in bs:
                    _bs += bs[j0_3]
            else:
                l = l3 if l3 < l else l
                h = h4 if h4 > h else h
                c = c2
                v += v5
                j0_3 = j0[:-3]
                if j0_3 in bs:
                    _bs += bs[j0_3]
        else:
            try:
                j0_3 = j0[:-3]
                if j0_3 in bs:
                    _bs += bs[j0_3]
                if _bs:
                    bs2[t] = _bs
                yield [t, o, c, l, h, v], bs2
            except:
                "没有数据"
    elif cycle == 1:  # 一分钟线
        bs2 = {}
        for t0, o1, c2, l3, h4, v5 in data:
            j0 = str(t0)
            j0_3 = j0[:-3]
            if j0_3 in bs:
                bs2[j0] = bs[j0_3]
            yield [j0, o1, c2, l3, h4, v5], bs2
    else:  # 其它分钟线 5分钟，30分钟，60分钟
        _init = True  # 是否需要初始化
        _is_last_init = False  # 是否刚刚初始化
        i = 1
        for t0, o1, c2, l3, h4, v5 in data:
            # [datetime.datetime(2018, 9, 14, 14, 52), 7340.0, 7338.0, 7334.0, 7340.0, 2922]
            j0 = str(t0)
            ts = j0[:10]
            if _init or ts not in _ts:
                _ts.add(ts)
                o = o1
                l = l3
                h = h4
                v = v5
                i = 1
                _init = False
                _is_last_init = True
                j0_3 = j0[:-3]
                if j0_3 in bs:
                    _bs += bs[j0_3]
            if i % cycle:
                l = l3 if l3 < l else l
                h = h4 if h4 > h else h
                i += 1
                if not _is_last_init:
                    v += v5
                    if j0[:-3] in bs:
                        _bs += bs[j0[:-3]]
            else:
                l = l3 if l3 < l else l
                h = h4 if h4 > h else h
                v += v5
                j0_3 = j0[:-3]
                if j0_3 in bs:
                    _bs += bs[j0_3]
                if _bs:
                    bs2[j0] = _bs
                    _bs = []
                yield [j0, o, c2, l, h, v], bs2
                _init = True
                i = 1
            _is_last_init = False
        else:
            try:
                yield [j0, o, c2, l, h, v], bs2
            except:
                "没有数据"


def future_macd(short=12, long=26, phyd=9):
    """ macd指标计算 """
    # da格式：((datetime.datetime(2018, 3, 19, 9, 22),31329.0,31343.0,31328.0,31331.0,249)...)
    dc = []
    da2 = []
    da = []
    i = 0
    while 1:
        _da = yield da2
        da.append(_da)
        _t = _da[0]  # 时间
        _o = _da[1]  # 开盘价
        _c = _da[2]  # 收盘价
        _l = _da[3]  # 最低价
        _h = _da[4]  # 最高价

        dc.append(
            {'ema_short': 0, 'ema_long': 0, 'diff': 0, 'dea': 0, 'macd': 0, 'datetimes': _t,
             'open': _o, 'high': _h, 'low': _l, 'close': _c})
        if i == 1:
            ac = da[i - 1][4]
            this_c = da[i][4]
            dc[i]['ema_short'] = ac + (this_c - ac) * 2 / short
            dc[i]['ema_long'] = ac + (this_c - ac) * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
        elif i > 1:
            n_c = da[i][4]
            dc[i]['ema_short'] = dc[i - 1]['ema_short'] * (short - 2) / short + n_c * 2 / short
            dc[i]['ema_long'] = dc[i - 1]['ema_long'] * (long - 2) / long + n_c * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i - 1]['dea'] * (phyd - 2) / phyd + dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])

        da2 = [_t, _o, _c, _l, _h, da[i][5], 0, round(dc[i]['macd'], 2), round(dc[i]['diff'], 2),
               round(dc[i]['dea'], 2)]
        i += 1


def this_day_week_month_year(when):
    """ 当日、当周、当月、当年的开始与结束时间计算"""
    this_d = datetime.datetime.now()
    this_t = time.localtime()
    this_day = str(this_d)[:10]
    if when == 'd':  # 当日
        start_date, end_date = this_day, this_day
    elif when == 'w':  # 当周
        start_date, end_date = HSD.get_date(-this_d.weekday()), this_day
    elif when == 'm':  # 当月
        start_date, end_date = HSD.get_date(-this_d.day + 1), this_day
    elif when == 'y':  # 当年
        start_date, end_date = HSD.get_date(-this_t.tm_yday + 1), this_day
    else:
        start_date, end_date = None, None
        when = '0'
    return when, start_date, end_date


class MyThread(Thread):
    """ 有返回值的多线程 """
    __slots__ = ('func', 'args', 'kwargs', 'f_name', 'result')

    def __init__(self, target, *args, **kwargs):
        super(MyThread, self).__init__()
        self.func = target
        self.args = args
        self.kwargs = kwargs
        self.f_name = target.__name__

    def run(self):
        self.result = self.func(*self.args, **self.kwargs)

    def get_result(self):
        try:
            return self.f_name, self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None


def runThread(*funcs):
    """ 执行多线程 """
    _funcs = [MyThread(*i) for i in funcs]
    [i.start() for i in _funcs]
    [i.join() for i in _funcs]
    return dict(i.get_result() for i in _funcs)


@asyncs
def cfmmc_huice(data, host, start_date, end_date, hc_name, red_key):
    """
    期货回测，绘图
    :param data:        数据对象 yield
    :param host:        期货帐号
    :param start_date:  开始时间
    :param end_date:    结束时间
    :param hc_name:     用户名称
    :param red_key:     Redis数据库所在的key
    :return:            结果写入Redis
    """
    cfmmc = HSD.Cfmmc(host, start_date, end_date)
    # _results = runThread((cfmmc.varieties,), (cfmmc.get_qy,), (cfmmc.init_money,))
    # pzs = _results['varieties']
    # _qy = _results['get_qy']
    # base_money = _results['init_money']  # 初始总资金

    pzs = cfmmc.varieties()
    _qy = cfmmc.get_qy()
    base_money = cfmmc.init_money()  # 初始总资金
    init_money = 0
    jzs = cfmmc.get_jz()
    jz = 1  # 初始净值
    jzq = 0  # 初始净值权重
    allje = 0  # init_money  # 总金额
    eae = []  # 出入金
    zx_x, prices = [], []
    hc, hcd, huizong, init_money = cfmmc_hc_data(host, start_date, end_date)
    # print(hc['allcchz'])
    all_ccsjy, all_ccsjk = [], []  # 持仓时间，盈利、亏损
    all_ccsjss, all_ykss = [], []  # 持仓时间、手数，盈亏、手数
    all_pcsj = []  # 平仓时间，盈亏、亏损
    all_pcsjn = []
    all_pcsjs = {}
    all_ccsjyl = defaultdict(int)  # 持仓时间，盈利
    all_ccsjks = defaultdict(int)  # 持仓时间，亏损
    all_ccsjylks = []  # 持仓时间
    all_pcsj_max = 0  # 平仓时间，手数的最大值
    all_ylje = [  # 做多、空，盈利、亏损金额
        {'value': 0, 'name': '做多盈利金额'},
        {'value': 0, 'name': '做空盈利金额'},
        {'value': 0, 'name': '做多亏损金额'},
        {'value': 0, 'name': '做空亏损金额'}
    ]
    all_ylss = [  # 做多、空，盈利、亏损手数
        {'value': 0, 'name': '做多盈利手数'},
        {'value': 0, 'name': '做空盈利手数'},
        {'value': 0, 'name': '做多亏损手数'},
        {'value': 0, 'name': '做空亏损手数'}
    ]
    all_rnje = [  # 日内、隔夜，盈利、亏损金额
        {'value': 0, 'name': '日内盈利金额'},
        {'value': 0, 'name': '隔夜盈利金额'},
        {'value': 0, 'name': '日内亏损金额'},
        {'value': 0, 'name': '隔夜亏损金额'}
    ]
    all_rnss = [  # 日内、隔夜，盈利、亏损手数
        {'value': 0, 'name': '日内盈利手数'},
        {'value': 0, 'name': '隔夜盈利手数'},
        {'value': 0, 'name': '日内亏损手数'},
        {'value': 0, 'name': '隔夜亏损手数'}
    ]
    if 'allcchz' not in hc:
        hc['allcchz'] = []
    for cc0, cc1, cc2, cc3, cc4, cc5, cc6 in hc['allcchz']:
        # cc: 持仓时间，多空(1,0），盈亏，手数，日内隔夜(1,0），平仓时间，合约
        # cc: (173, 0, 8700, 1, 1, '2018-09-03 14:09:05', 'J1901')
        cc0 = round(cc0 / 60, 2)
        if cc6 not in all_pcsjn:
            all_pcsjn.append(cc6)
            all_pcsjs[cc6] = []
        all_pcsjs[cc6].append([cc5, cc2, cc3])
        ccsj_sj = math.ceil(cc0 + 3 - cc0 % 3)
        all_ccsjylks.append(math.ceil(cc0))
        if cc2 > 0:
            all_ccsjy.append([cc0, int(cc2), cc3])
            all_pcsj.append([cc5, int(cc2), cc3])  # {value:214, name:'多'},
            all_ccsjyl[math.ceil(cc0)] += cc2
            if cc1 == 1:
                all_ylje[0]['value'] += cc2
                all_ylss[0]['value'] += cc3
            else:
                all_ylje[1]['value'] += cc2
                all_ylss[1]['value'] += cc3
            if cc4 == 1:
                all_rnje[0]['value'] += cc2
                all_rnss[0]['value'] += cc3
            else:
                all_rnje[1]['value'] += cc2
                all_rnss[1]['value'] += cc3
        else:
            all_ccsjk.append([cc0, int(cc2), cc3])
            all_pcsj.append([cc5, int(cc2), cc3])
            all_ccsjks[math.ceil(cc0)] += cc2
            if cc1 == 1:
                all_ylje[2]['value'] += -cc2
                all_ylss[2]['value'] += cc3
            else:
                all_ylje[3]['value'] += -cc2
                all_ylss[3]['value'] += cc3
            if cc4 == 1:
                all_rnje[2]['value'] += -cc2
                all_rnss[2]['value'] += cc3
            else:
                all_rnje[3]['value'] += -cc2
                all_rnss[3]['value'] += cc3
        all_pcsj_max = cc3 if cc3 > all_pcsj_max else all_pcsj_max
        all_ccsjss.append([ccsj_sj, cc3])
        all_ykss.append([cc2 + 500 - cc2 % 500, cc3])
    if all_pcsj_max % 5:
        all_pcsj_max = all_pcsj_max + (5 - all_pcsj_max % 5)

    hc_name = hc_name[0] + '*' * (len(hc_name) - 1)
    hc_name = host[:4] + '***' + host[-4:] + ' ( ' + hc_name + ' )'
    max_jz = 0  # 最大净值
    zjhc = 0  # 资金回测
    # print(all_ykss)
    all_ccsjss.sort()
    all_ykss.sort()
    all_ccsjylks = list(set(all_ccsjylks))
    all_ccsjylks.sort()
    all_ccsjss2 = defaultdict(int)
    all_ykss2 = defaultdict(int)
    for ccsj in all_ccsjss:
        all_ccsjss2[ccsj[0]] += ccsj[1]
    for ykss in all_ykss:
        all_ykss2[ykss[0]] += ykss[1]
    all_ccsjyl2 = []  # 持仓时间，盈利
    all_ccsjks2 = []  # 持仓时间，亏损
    for ccsjylks in all_ccsjylks:
        all_ccsjyl2.append(all_ccsjyl.get(ccsjylks, 0))
        all_ccsjks2.append(all_ccsjks.get(ccsjylks, 0))
    hct = {
        'allyk': [],  # 累积盈亏
        'alljz': [],  # 累积净值
        'allsxf': [],  # 累积手续费
        'pie_name': [i[0] for i in pzs],  # 成交偏好（饼图） 产品名称
        'pie_value': [{'value': i[1], 'name': i[0]} for i in pzs],  # 成交偏好 饼图的值
        'qy': [],  # 客户权益
        'bar_name': [],  # 品种盈亏 名称
        'bar_value': [],  # 品种盈亏 净利润
        'day_value': [],  # 每日盈亏 净利润
        'week_name': [],  # 每周盈亏 名称
        'week_value': [],  # 每周盈亏 净利润
        'month_name': [],  # 每月盈亏 名称
        'month_value': [],  # 每月盈亏 净利润
        'alleae': [],  # 累积出入金
        'amount': [],  # 账号总金额
        'eae': [],  # 出入金
        'all_ccsjy': all_ccsjy,
        'all_ccsjk': all_ccsjk,
        'all_pcsj': all_pcsj,
        'all_pcsj_max': all_pcsj_max,
        'all_ylje': all_ylje,
        'all_ylss': all_ylss,
        'all_rnje': all_rnje,
        'all_rnss': all_rnss,
        'all_pcsjn': all_pcsjn,
        'all_pcsjs': all_pcsjs,
        'zjhc': [],  # 资金回测
        'ccsjss_x': list(all_ccsjss2.keys()),
        'ccsjss_y': list(all_ccsjss2.values()),
        'ykss_x': list(all_ykss2.keys()),
        'ykss_y': list(all_ykss2.values()),

        'all_ccsjylks': all_ccsjylks,
        'all_ccsjyl2': all_ccsjyl2,
        'all_ccsjks2': all_ccsjks2,
    }
    name_jlr = defaultdict(float)  # 品种名称，净利润
    week_jlr = defaultdict(float)  # 每周，净利润
    month_jlr = defaultdict(float)  # 每月，净利润
    data2 = []
    dates = cfmmc.get_dates()
    ee = cfmmc.get_rj()  # 出入金
    for de, *_ in dates:
        zx_x.append(de)
        yk, sxf = 0, 0
        f_date = datetime.datetime.strptime(de, '%Y-%m-%d').isocalendar()[:2]
        week = str(f_date[0]) + '-' + str(f_date[1])  # 星期
        month = de[:7]  # 月
        for d in data:
            # d: ('2018-08-31', 'J1901', 1750.0, 14.92, 'J1901(冶金焦炭)')
            if d[0][:10] == de:
                sxf += d[3] if d[3] else 0
                if not d[2]:
                    continue
                yk += d[2]
                name_jlr[d[4]] += d[2]
                week_jlr[week] += d[2]
                month_jlr[month] += d[2]
            else:
                data2.append(d)

        hct['day_value'].append(round(yk - sxf, 2))
        yk += (prices[-1] if prices else 0)
        prices.append(yk)
        sxf += (hct['allsxf'][-1] if hct['allsxf'] else 0)
        hct['allsxf'].append(round(sxf, 1))
        rj = base_money + sum(i[1] for i in ee if i[0] <= de)
        if rj != init_money:
            # jzq = (jzq * jz + rj - init_money) / jz  # 净值权重
            init_money = rj
            hct['eae'].append(init_money - (hct['alleae'][-1] if hct['alleae'] else 0))
        else:
            hct['eae'].append('')
        amount = init_money + yk - hct['allsxf'][-1]
        hct['amount'].append(amount)
        hct['alleae'].append(init_money)
        # jz = amount / jzq if jzq != 0 else jz
        jz2 = jzs[de]
        hct['alljz'].append(round(jz2, 4))
        hct['qy'].append(_qy[de])
        max_jz = jz2 if jz2 > max_jz else max_jz
        hct['zjhc'].append(round((max_jz - jz2) / max_jz * 100, 2))
        data = data2
        data2 = []
    if hct['zjhc']:
        huizong['max_zjhc'] = max(hct['zjhc'])  # 最大回测
    red = HSD.RedisPool()
    try:
        zx_x2 = [i for i in zx_x if start_date <= i <= end_date]
        ind_s = zx_x.index(zx_x2[0])
        ind_e = zx_x.index(zx_x2[-1]) + 1
        zx_x = zx_x[ind_s:ind_e]
        prices = prices[ind_s:ind_e]
        f_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').isocalendar()[:2]
        week = str(f_date[0]) + '-' + str(f_date[1])  # 开始星期
        f_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').isocalendar()[:2]
        week2 = str(f_date[0]) + '-' + str(f_date[1])  # 结束星期
        # month_jlr = {k: v for k, v in month_jlr.items() if start_date <= k <= end_date}
        week_jlr = {k: v for k, v in week_jlr.items() if week <= k <= week2 and v != 0}
        hct['allyk'] = prices
        hct['bar_name'] = [i for i in name_jlr]
        hct['bar_value'] = [round(name_jlr[i], 1) for i in name_jlr]
        # week_jlr = {k: v for k, v in week_jlr.items() if v != 0}
        hct['week_name'] = [i for i in week_jlr]
        hct['week_value'] = [round(week_jlr[i], 1) for i in week_jlr]
        hct['month_name'] = [i for i in month_jlr if start_date <= i <= end_date]
        hct['month_value'] = [round(month_jlr[i], 1) for i in month_jlr if start_date <= i <= end_date]
        host = hc_name
        host = host[0] + '*' * len(host[1])
        hct['host'] = hc_name  # host
        # hc['zzl'] = [round((hct['alljz'][i]-hct['alljz'][i-1])/hct['alljz'][i-1]*100,3) if i!=0 else hct['alljz'][i] for i in range(len(hc['zzl']))]
        huizong['max_amount'] = round(max(hct['amount']), 2)  # 最高余额
        huizong['min_amount'] = round(min(hct['amount']), 2)  # 最低余额
        huizong['deposit'] = sum(i[1] for i in ee if i[1] > 0)  # 存款
        huizong['draw'] = sum(i[1] for i in ee if i[1] < 0)  # 取款
        huizong['ykratio'] = abs(round(hc['avglr'] / (hc['avgss'] if hc['avgss'] != 0 else 1), 2))
        huizong['huibaolv'] = hct['alljz'][-1]  # 回报率

        resp = {'zx_x': zx_x, 'hct': hct, 'start_date': start_date, 'end_date': end_date, 'hc': hc, 'huizong': huizong,
                'init_money': init_money, 'hcd': hcd, 'hc_name': hc_name}
        # write_to_cache(cfmmc_huice_key, resp, expiry_time=60 * 60 * 24)

        # return render(rq, 'cfmmc_tu.html', resp)
        red.set(red_key, resp)

    except:
        red.set(red_key, 0)


def get_cloud_file(path_root):
    """ 获取目录的文件 """
    clouds = []
    for i in os.listdir(path_root):
        try:
            ind = i.index('_+_')
        except:
            continue
        path_file = os.path.join(path_root, i)
        file_size = os.path.getsize(path_file)
        times = str(datetime.datetime.fromtimestamp(os.path.getctime(path_file)))[:19]
        if file_size >= 1024 * 1024 * 1024:
            file_size = str(round(file_size / 1024 / 1024 / 1024, 2)) + ' GB'
        elif file_size >= 1024 * 1024:
            file_size = str(round(file_size / 1024 / 1024, 2)) + ' MB'
        elif file_size >= 1024:
            file_size = str(round(file_size / 1024, 2)) + ' KB'
        else:
            file_size = str(file_size) + ' B'
        # 文件名称，文件大小，上传者，上传时间
        clouds.append([i[ind + 3:], file_size, i[:ind], times])
    clouds.sort(key=lambda x: x[3], reverse=True)
    return clouds


class FileWrapper:
    """Wrapper to convert file-like objects to iterables"""

    def __init__(self, filelike, blksize=64):
        self.filelike = filelike
        self.blksize = blksize
        if hasattr(filelike, 'close'):
            self.close = filelike.close

    def __getitem__(self, key):
        data = self.filelike.read(self.blksize)
        if data:
            return data
        raise IndexError

    def __iter__(self):
        return self

    def __next__(self):
        data = self.filelike.read(self.blksize)
        if data:
            return data
        raise StopIteration


def file_iterator(files, chunk_size=512, red=None, red_key=None):
    """ 分块下载函数 """
    f = open(files, 'rb')
    if red and red_key:
        red.set(red_key, 'read', expiry=6000)
    while True:
        try:
            c = f.read(chunk_size)
        except:
            f.close()
            break
        if c:
            if len(c) < chunk_size:
                f.close()
            yield c
        else:
            f.close()
            break
    red.set(red_key, 'ok', expiry=100)


def get_interface_datas(hc_name):
    """ 从文件夹获取序列化的数据 """
    # _folder = HSD.get_external_folder('huice')
    with open(hc_name, 'rb') as f:
        datas = pickle.loads(f.read())
    return datas


def get_interface_huice(hc_name, min_date=None, max_date=None):
    """ 回测接口"""
    datas = get_interface_datas(hc_name)
    summary = datas['summary']
    trades = datas['trades']
    init_money = summary['FUTURE']  # 入金
    results2, data_ykall = HSD.huice_order_record(hc_name, datas)
    res = {}
    pinzhong = []  # 所有品种

    huizong = {
        'yk': 0,
        'shenglv': 0,
        'zl': 0,
        'least': [0, 1000],
        'most': [0, -1000],
        'avg': 0,
        'avg_day': 0,
        'least2': [0, 1000],
        'most2': [0, -1000],
        'expect': round(datas['future_account'].daily_pnl.sum() / len(datas['future_account']), 2),  # 期望
        'commission': round(trades.commission.sum()),  # 佣金
        'sharp': summary['sharpe'],  # 夏普比率
        'sortino': summary['sortino'],  # 索提诺比率
        'information_ratio': summary['information_ratio'],  # 信息比率
        'downside_risk': summary['downside_risk'],  # 下行风险
    }
    for i in results2:
        if not i[5]:
            continue
        # if i[1] not in pinzhong:
        #     pinzhong.append(i[1])
        dt = i[2][:10]
        if dt not in res:
            res[dt] = {'duo': 0, 'kong': 0, 'mony': 0, 'shenglv': 0, 'ylds': 0, 'datetimes': []}
        if i[7] == '多':
            res[dt]['duo'] += 1
            _ykds = i[5] - i[3]  # 盈亏点数
        elif i[7] == '空':
            res[dt]['kong'] += 1
            _ykds = i[3] - i[5]  # 盈亏点数
        res[dt]['mony'] += i[6]
        xx = [i[2], i[4], i[7], i[6], i[3], i[5], i[8], i[1]]
        res[dt]['datetimes'].append(xx)

        huizong['least'] = [dt, i[6]] if i[6] < huizong['least'][1] else huizong['least']
        huizong['least2'] = [dt, _ykds] if _ykds < huizong['least2'][1] else huizong['least2']
        huizong['most'] = [dt, i[6]] if i[6] > huizong['most'][1] else huizong['most']
        huizong['most2'] = [dt, _ykds] if _ykds > huizong['most2'][1] else huizong['most2']

    # if min_date and max_date:
    #     _re = re.compile(r'[A-z]+')
    #     _pz = set(re.search(_re, i)[0] for i in pinzhong)
    #     _pz = [i + 'L8' for i in _pz]
    #     _redis = HSD.RedisPool()
    #     pinzhong = _redis.get('cfmmc_hc_data_pinzhong')
    #     if not pinzhong:
    #         pinzhong = {}
    #     mongo = HSD.MongoDBData()
    #     is_cache_set = False
    #     for p in _pz:
    #         if p not in pinzhong or pinzhong[p]['min_date'] > min_date or pinzhong[p]['max_date'] < max_date:
    #             pzs = mongo.get_data(p, min_date, max_date)
    #             pzs = {str(i)[:-3]: j for j, (i, *_) in enumerate(pzs)}
    #             pzs['min_date'] = min_date
    #             pzs['max_date'] = max_date
    #             pinzhong[p] = pzs
    #             is_cache_set = True
    #     if is_cache_set:
    #         _redis.set('cfmmc_hc_data_pinzhong', pinzhong)

    res, huizong = tongji_huice(res, huizong)

    hc, huizong = HSD.huices(res, huizong, init_money, None, str(datetime.datetime.now())[:10], pinzhong)
    hc['portfolio_jz'] = list(datas['portfolio']['unit_net_value'])

    yield hc, huizong, init_money
    for i in data_ykall:
        yield data_ykall[i]


@asyncs
def moni(dates, end_date, fa, database, reverse, zsds, ydzs, zyds, cqdc, red_key):
    """ 模拟测试 """
    red = HSD.RedisPool()
    zsds, ydzs, zyds, cqdc = HSD.format_int(zsds, ydzs, zyds, cqdc) if zsds and ydzs and zyds and cqdc else (
        100, 100, 200, 6)
    reverse = True if reverse else False
    zbjs = HSD.Zbjs()
    ma = 60
    param = {'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc}
    res, huizong, first_time = zbjs.main2(_ma=ma, _dates=dates, end_date=end_date, _fa=fa, database=database,
                                          reverse=reverse, param=param, red_key=red_key)
    try:
        keys = sorted(res.keys(), reverse=True)
        res_length = len(keys)
        res = [dict(res[k], **{'time': k}) for k in keys if res[k]['datetimes']][:500]  # 限制最多显示500天交易详细记录
        fa_doc = zbjs.fa_doc
        resp = {'res': res, 'keys': keys, 'dates': dates, 'end_date': end_date, 'fa': fa, 'fas': list(zbjs.xzfa.keys()),
                'fa_doc': fa_doc, 'fa_one': fa_doc.get(fa), 'huizong': huizong, 'database': database,
                'first_time': first_time, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc,
                'res_length': res_length}
        if end_date < HSD.get_date():
            expiry = 86400
        else:
            expiry = 1800
        red.set(red_key, resp, expiry)
    except Exception as exc:
        error_log('viewUtil.py', sys._getframe().f_lineno, exc)
        red.set(red_key, 0, 300)


def get_interface_file(_folder):
    """ 获取回测文件 """
    res = []
    for i in os.listdir(_folder):
        if i in {'CTP', 'HK'}:
            fol = os.path.join(_folder, i)
            for j in os.listdir(fol):
                fol2 = os.path.join(fol, j)
                if os.path.isdir(fol2):
                    tmp = [j, '', '', '', i]
                    for k in os.listdir(fol2):
                        if k.endswith('.pkl'):
                            tmp[1] = os.path.join(i, j, k)
                        elif k.endswith('.py'):
                            tmp[2] = os.path.join(i, j, k)
                        elif k.endswith('.md'):
                            tmp[3] = os.path.join(i, j, k)
                    res.append(tmp)
    return res


def get_hqzjzb(ttype):
    """  获取某个特性的指标 """
    pnm = f'mysite\\myfile\\{ttype}.pkl'
    if os.path.isfile(pnm):
        d = pd.read_pickle(pnm)
        d = {str(i): j for i, j in d.values}
        return d
    return {}

