import sys
import json
import requests
import pandas as pd
import numpy as np
import re
import time
import datetime
import xlrd
import os

from django.core.cache import cache
from io import BytesIO
from sqlalchemy import create_engine
from pyquery import PyQuery as pq
from threading import Thread
from collections import defaultdict

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


@asyncs
def record_log(files, info, types):
    """ 访问日志 """
    if types == 'w':
        with open(files, 'w') as f:
            f.write(json.dumps(info))
    elif types == 'a':
        with open(files, 'a') as f:
            f.write(info)


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


def tongji_first():
    """ 最初进入统计的页面 """
    herys = None
    try:
        herys = HSD.tongji()
    except Exception as exc:
        HSD.logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))

    return herys


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
        # sql = "SELECT 合约,成交序号,DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records WHERE 交易日期 IN (SELECT MAX(交易日期) FROM cfmmc_trade_records GROUP BY 帐号) GROUP BY 帐号"
        sql = "SELECT 合约,(CASE WHEN LENGTH(成交序号)>8 THEN SUBSTR(成交序号,9,16) ELSE 成交序号 END)," \
              "DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records GROUP BY 交易日期,帐号 ORDER BY 交易日期 DESC"
    elif start_date and end_date:
        sql = f"SELECT 合约,(CASE WHEN LENGTH(成交序号)>8 THEN SUBSTR(成交序号,9,16) ELSE 成交序号 END)," \
              f"DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records WHERE 帐号='{host}' AND 实际成交日期>='{start_date}' AND 实际成交日期<='{end_date}' ORDER BY 实际成交日期 DESC,成交时间 DESC"
    else:
        # end_date = datetime.datetime.now()
        # start_date = end_date - datetime.timedelta(days=6)
        # end_date = str(end_date)[:10]
        # start_date = str(start_date)[:10]
        sql = f"SELECT 合约,(CASE WHEN LENGTH(成交序号)>8 THEN SUBSTR(成交序号,9,16) ELSE 成交序号 END)," \
              f"DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records WHERE 帐号='{host}' ORDER BY 实际成交日期 DESC,成交时间 DESC limit 30"
        # sql = f"SELECT 合约,成交序号,DATE_FORMAT(成交时间,' %H:%i:%S'),`买/卖`,`投机/套保`,成交价,手数,成交额,`开/平`,手续费,平仓盈亏,DATE_FORMAT(实际成交日期,'%Y-%m-%d'),帐号,DATE_FORMAT(交易日期,'%Y-%m-%d') FROM cfmmc_trade_records WHERE 帐号='{host}' AND 实际成交日期>='{start_date}' AND 实际成交日期<='{end_date}' ORDER BY 实际成交日期 DESC,成交时间 DESC"
    data = HSD.runSqlData('carry_investment', sql)
    return data


class Cfmmc:
    """ 期货监控系统，登录，下载数据，保存数据 """

    def __init__(self):
        self.session = requests.session()
        self._login_url = 'https://investorservice.cfmmc.com/login.do'
        self._vercode_url = 'https://investorservice.cfmmc.com/veriCode.do?t='
        us = HSD.get_config("U", "us")
        ps = HSD.get_config("U", "ps")
        hs = HSD.get_config("U", "hs")
        self._conn = create_engine(f'mysql+pymysql://{us}:{ps}@{hs}:3306/carry_investment?charset=utf8')
        self.trade_records = None
        self.closed_position = None
        self.holding_position = None

    def getToken(self, url):
        """获取token"""
        token_name = "org.apache.struts.taglib.html.TOKEN"
        ret = self.session.get(self._login_url)
        ret_text = pq(ret.text)
        for x in ret_text('input'):
            if x.name == token_name:
                return x.value

    def getCode(self):
        """ 获取验证码 """
        t = int(datetime.datetime.now().timestamp() * 1000)
        vercode_url = f'{self._vercode_url}{t}'
        response = self.session.get(vercode_url)
        # image = Image.open(BytesIO(response.content))
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
            try:
                successful_landing = v[0].text.replace('\r', '').replace('\n', '').replace('\t', '').strip()
            except:
                pass
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
        try:
            p = pd.read_excel(BytesIO(ret_content))
            name = p[p.ix[:, 0] == '客户名称'].ix[:, 2].values[0]
            return name
        except:
            pass

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

                # if self.trade_records is None:
                #     self.trade_records = trade_records
                # else:
                #     self.trade_records = self.trade_records.append(trade_records, ignore_index=True)
                #
                # if self.closed_position is None:
                #     self.closed_position = closed_position
                # else:
                #     self.closed_position = self.closed_position.append(closed_position, ignore_index=True)
                #
                # if self.holding_position is None:
                #     self.holding_position = holding_position
                # else:
                #     self.holding_position = self.holding_position.append(holding_position, ignore_index=True)
                excs = ''
                if byType == 'date':
                    try:
                        account_info.to_sql('cfmmc_daily_settlement', self._conn, schema='carry_investment',
                                            if_exists='append',
                                            index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        trade_records.to_sql('cfmmc_trade_records', self._conn, schema='carry_investment',
                                             if_exists='append',
                                             index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        closed_position.to_sql('cfmmc_closed_position', self._conn, schema='carry_investment',
                                               if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        holding_position.to_sql('cfmmc_holding_position', self._conn, schema='carry_investment',
                                                if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)
                elif byType == 'trade':
                    try:
                        account_info.to_sql('cfmmc_daily_settlement_trade', self._conn, schema='carry_investment',
                                            if_exists='append',
                                            index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        trade_records.to_sql('cfmmc_trade_records_trade', self._conn, schema='carry_investment',
                                             if_exists='append',
                                             index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        closed_position.to_sql('cfmmc_closed_position_trade', self._conn, schema='carry_investment',
                                               if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)
                    try:
                        holding_position.to_sql('cfmmc_holding_position_trade', self._conn, schema='carry_investment',
                                                if_exists='append', index=False)
                    except Exception as exc:
                        excs += str(exc)

                # print(f'{tradeDate}的{byType}数据下载成功')
                if excs == '':
                    sql = 'insert into cfmmc_insert_date(host,date,type) values(%s,%s,%s)'
                    tt = 0 if byType == 'date' else (1 if byType == 'trade' else -1)
                    HSD.runSqlData('carry_investment', sql, (_account, _tradedate, tt))
                else:
                    record_log('log\\error_log\\err.txt',excs+f'[viewUtil | Cfmmc.save_settlement]{tradeDate}\n','a')
                return name
            except Exception as e:
                # print(f'{tradeDate}的{byType}数据下载失败\n{e}')
                record_log('log\\error_log\\err.txt', f'{tradeDate}的{byType}数据下载失败\n{e}\n', 'a')
                return '_fail'

    @asyncs
    def down_day_data_sql(self, host, start_date, end_date, password=None, createTime=None):
        """ 下载啄日数据并保存到SQL """
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
            try:
                for d in range(days):
                    date = start_date + datetime.timedelta(d)
                    date = str(date)[:10]
                    if date not in self._not_trade_list and date not in dates:
                        name = self.save_settlement(date, byType, name)
                        is_run = True
                        if name and name != '_fail':
                            sql = f"INSERT INTO cfmmc_user(host,password,cookie,download,name,creationTime) VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE name='{name}'"
                            HSD.runSqlData('carry_investment', sql, (host, password, '', 1, name, createTime))
                            is_success = True
                            cache.set('cfmmc_status' + host, f"{date}日前的数据更新成功！")
                        time.sleep(0.1)
            except:
                pass
        s = ('True' if is_success else ('False' if is_run else 'not_run'))
        cache.set('cfmmc_status' + host, s)


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
            try:
                last_date = cache.get('cfmmc_Automatic_download')
                break
            except:
                "Redis 未启动"
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
                cfmmc_login_d = Cfmmc()
                data = HSD.runSqlData('carry_investment', sql)
                for da in data:
                    sql_date = f"SELECT DATE_FORMAT(DATE,'%Y-%m-%d') FROM cfmmc_insert_date WHERE HOST='{da[0]}' ORDER BY DATE DESC LIMIT 1"
                    is_down_date = HSD.runSqlData('carry_investment', sql_date)
                    if d.weekday() > 4 or (is_down_date and is_down_date[0][0] == str(d)[:10]):
                        continue
                    if computer_name == 'DOC':  # 防止重复登录，在本机上不执行
                        continue
                    for i in range(20):  # 每个账号最多尝试登录20次
                        try:
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
                        except Exception as exc:
                            print(exc)
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
            end_date = str(trade[0][11]) # HSD.get_date()
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
    # rq_date = results2[0][2][:10]
    # end_date = results2[-1][4][:10]

    # results2[0]：['016681702757', 'J1901', '2018-08-30 09:08:12', 2544.5, '2018-08-30 10:06:14', 2528.0, -1650, '多', 1, '已平仓']
    res = {}
    huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
               'avg_day': 0, 'least2': [0, 1000, 0, 0], 'most2': [0, -1000, 0, 0]}
    pinzhong = []  # 所有品种
    min_date = ''
    max_date = ''
    for i in results2:
        if not i[5]:
            continue
        if i[1] not in pinzhong:
            pinzhong.append(i[1])
        dt = i[4][:10]
        if min_date == '' or i[2][:10]<min_date:
            min_date = i[2][:10]
        if max_date == '' or dt>max_date:
            max_date = dt
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
    money_sql = f"SELECT 上日结存,当日存取合计 FROM cfmmc_daily_settlement WHERE 帐号='{host}' AND " \
                f"(当日存取合计!=0 OR 交易日期 IN (SELECT MIN(交易日期) FROM cfmmc_daily_settlement WHERE 帐号='{host}'))"
    moneys = HSD.runSqlData('carry_investment', money_sql)
    init_money = sum(j[1] if i != 0 else j[0] for i, j in enumerate(moneys))
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
        if p not in pinzhong or pinzhong[p]['min_date']>min_date or pinzhong[p]['max_date']<max_date:
            pzs = mongo.get_data(p,min_date,max_date)
            pzs = {i[0][:-3]: j for j, i in enumerate(pzs)}
            pzs['min_date'] = min_date
            pzs['max_date'] = max_date
            pinzhong[p] = pzs
            is_cache_set = True
    if is_cache_set:
        _redis.set('cfmmc_hc_data_pinzhong',pinzhong)
    res, huizong = tongji_huice(res, huizong)
    hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date, pinzhong)

    return hc, hcd, huizong, init_money


def future_data_cycle(data, bs, cycle):
    """ data：精确到分钟的行情数据，bs：精确到秒的交易数据，
        cycle：若等于1D就是日线，其他为数字（以分钟为单位）"""
    bs0 = defaultdict(list)
    [bs0[i[0][:-3]].append(list(i)) for i in bs]
    bs = bs0
    data2 = []
    bs2 = {}
    _bs = []
    _ts = set()
    if cycle == '1D':  # 日线
        for j in data:
            ts = j[0][:10]
            if ts not in _ts:
                if _ts:
                    # data2.append([t, o, c, l, h, v])
                    if _bs:
                        bs2[t] = _bs
                        _bs = []
                    yield [t, o, c, l, h, v],bs2
                _ts.add(ts)
                t = j[0][:10] + ' 00:00:00'
                o = j[1]
                l = j[3]
                h = j[4]
                v = j[5]
                if j[0][:-3] in bs:
                    _bs += bs[j[0][:-3]]
            else:
                l = j[3] if j[3] < l else l
                h = j[4] if j[4] > h else h
                c = j[2]
                v += j[5]
                if j[0][:-3] in bs:
                    _bs += bs[j[0][:-3]]
        else:
            # data2.append([t, o, c, l, h, v])
            if _bs:
                bs2[t] = _bs
            yield [t, o, c, l, h, v], bs2
    elif cycle == 1:  # 一分钟线
        data2 = data
        bs2 = {} # {j[0]: bs[j[0][:-3]] for j in data if j[0][:-3] in bs}
        for j in data:
            # data2.append([j[0], j[1], j[2], j[3], j[4]])
            if j[0][:-3] in bs:
                bs2[j[0]] = bs[j[0][:-3]]
            yield [j[0], j[1], j[2], j[3], j[4]], bs2
    else:  # 其它分钟线 5分钟，30分钟，60分钟
        _init = True  # 是否需要初始化
        _is_last_init = False  # 是否刚刚初始化
        i = 1
        for j in data:
            ts = j[0][:10]
            if _init or ts not in _ts:
                _ts.add(ts)
                o = j[1]
                l = j[3]
                h = j[4]
                v = j[5]
                i = 1
                _init = False
                _is_last_init = True
                if j[0][:-3] in bs:
                    _bs += bs[j[0][:-3]]
            if i % cycle:
                l = j[3] if j[3] < l else l
                h = j[4] if j[4] > h else h
                i += 1
                if not _is_last_init:
                    v += j[5]
                    if j[0][:-3] in bs:
                        _bs += bs[j[0][:-3]]
            else:
                l = j[3] if j[3] < l else l
                h = j[4] if j[4] > h else h
                v += j[5]
                # data2.append([j[0], o, j[2], l, h, v])
                if _bs:
                    bs2[j[0]] = _bs
                    _bs = []
                yield [j[0], o, j[2], l, h, v], bs2
                _init = True
                i = 1
            _is_last_init = False
        else:
            # data2.append([j[0], o, j[2], l, h, v])
            yield [j[0], o, j[2], l, h, v], bs2
    # return data2, bs2


def future_macd(short=12, long=26, phyd=9):
    """ macd指标计算 """
    # da格式：((datetime.datetime(2018, 3, 19, 9, 22),31329.0,31343.0,31328.0,31331.0,249)...)
    dc = []
    da2 = []
    da = []
    # da2格式：time0 open1 close2 min3 max4 vol5 tag6 macd7 dif8 dea9  # tag：为涨跌趋势的标签（0或1）
    # ['2015-10-16',18.4,18.58,18.33,18.79,67.00,1,0.04,0.11,0.09]
    # for i in range(len(da)):
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
            # dc[i]['ema_short'] = sum([(short-j)*da[i-j][4] for j in range(short)])/(3*short)
            # dc[i]['ema_long'] = sum([(long-j)*da[i-j][4] for j in range(long)])/(3*long)
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
            co = 1 if dc[i]['macd'] >= 0 else 0
        elif i > 1:
            n_c = da[i][4]
            dc[i]['ema_short'] = dc[i - 1]['ema_short'] * (short - 2) / short + n_c * 2 / short
            dc[i]['ema_long'] = dc[i - 1]['ema_long'] * (long - 2) / long + n_c * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i - 1]['dea'] * (phyd - 2) / phyd + dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])

        da2 = [_t, _o, _c, _l, _h, da[i][5], 0, round(dc[i]['macd'], 2), round(dc[i]['diff'], 2), round(dc[i]['dea'], 2)]
        i += 1
    # return da2


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
