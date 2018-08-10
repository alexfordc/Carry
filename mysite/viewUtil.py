import sys
import datetime
import json

from threading import Thread

from mysite import HSD


def asyncs(func):
    """ 执行不需要返回结果的程序 """

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
    huizong['least2'] = min(all_price)
    huizong['most2'] = max(all_price)
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


def get_cfmmc_trade(host=None,start_date=None,end_date=None):
    """ 国内期货数据，交易记录 """
    # 合约, 成交序号, 成交时间, 买/卖, 投机/套保, 成交价, 手数, 成交额, 开/平, 手续费, 平仓盈亏, 实际成交日期, 帐号, 交易日期
    if host is None:
        sql = "SELECT * FROM cfmmc_trade_records GROUP BY 帐号"
    elif start_date and end_date:
        sql = f"SELECT * FROM cfmmc_trade_records WHERE 帐号='{host}' AND 实际成交日期>='{start_date}' AND 实际成交日期<='{end_date}' ORDER BY 实际成交日期 DESC,成交时间 DESC"
    else:
        sql = "SELECT * FROM cfmmc_trade_records WHERE 帐号='{}' ORDER BY 实际成交日期 DESC,成交时间 DESC limit 30".format(host)
    data = HSD.runSqlData('carry_investment', sql)
    return data


import urllib3
import requests
import pandas as pd
import numpy as np
import re
import time
import datetime
from PIL import Image
from io import BytesIO
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from pyquery import PyQuery as pq


class Cfmmc:
    def __init__(self):
        self.session = requests.session()
        self._login_url = 'https://investorservice.cfmmc.com/login.do'
        self._vercode_url = 'https://investorservice.cfmmc.com/veriCode.do?t='
        us = HSD.get_config("U", "us")
        ps = HSD.get_config("U", "ps")
        hs = HSD.get_config("U", "hs")
        self._conn = create_engine(f'mysql+pymysql://{us}:{ps}@{hs}:3306/carry_investment?charset=utf8')
        self._conn = create_engine('')
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

    def save_settlement(self, tradeDate, byType):
        """ 请求并保存某一天（tradeDate：2018-08-08），"""
        daily = 'https://investorservice.cfmmc.com/customer/setupViewCustomerDetailFromCompanyWithExcel.do'
        month = 'https://investorservice.cfmmc.com/customer/setupViewCustomerMonthDetailFromCompanyWithExcel.do'
        sp = 'https://investorservice.cfmmc.com/customer/setParameter.do'
        _t = self.getToken(sp)
        self.session.post(sp,
                          data={"org.apache.struts.taglib.html.TOKEN": _t, 'tradeDate': tradeDate, 'byType': byType})
        if len(tradeDate) == 10:
            if tradeDate in self._not_trade_list:
                raise Exception(f'{tradeDate}为未非交易日')
            ret = self.session.post(daily)
        elif len(tradeDate) == 7:
            ret = self.session.post(month)
        else:
            raise Exception('请输入正确的tradeDate')

        if ret.status_code != 200:
            raise Exception('请求错误')
        else:
            try:
                f_name = ret.headers['Content-Disposition'].strip('attachment; filename=')
                r_name, _ = f_name.split('.')
                _account, _tradedate = r_name.split('_')
                #                 with open(f_name, 'wb') as f:
                #                     f.write(ret.content)
                #                 print(f'{tradeDate}的{byType}数据下载成功')
                trade_records = pd.read_excel(BytesIO(ret.content), sheetname='成交明细', header=9, na_values='--',
                                              dtype={'成交序号': np.str_, '平仓盈亏': np.float})
                trade_records.drop([trade_records.index[-1]], inplace=True)
                closed_position = pd.read_excel(BytesIO(ret.content), sheetname='平仓明细', header=9,
                                                dtype={'成交序号': np.str_, '原成交序号': np.str_})
                closed_position.drop([closed_position.index[-1]], inplace=True)
                holding_position = pd.read_excel(BytesIO(ret.content), sheetname='持仓明细', header=9,
                                                 dtype={'成交序号': np.str_, '交易编码': np.str_})
                holding_position.drop([holding_position.index[-1]], inplace=True)
                for df in [trade_records, closed_position, holding_position]:
                    df['帐号'] = _account
                    df['交易日期'] = _tradedate

                if self.trade_records is None:
                    self.trade_records = trade_records
                else:
                    self.trade_records = self.trade_records.append(trade_records, ignore_index=True)

                if self.closed_position is None:
                    self.closed_position = closed_position
                else:
                    self.closed_position = self.closed_position.append(closed_position, ignore_index=True)

                if self.holding_position is None:
                    self.holding_position = holding_position
                else:
                    self.holding_position = self.holding_position.append(holding_position, ignore_index=True)

                trade_records.to_sql('cfmmc_trade_records', self._conn, schema='carry_investment', if_exists='append',
                                     index=False)
                closed_position.to_sql('cfmmc_closed_position', self._conn, schema='carry_investment',
                                       if_exists='append', index=False)
                holding_position.to_sql('cfmmc_holding_position', self._conn, schema='carry_investment',
                                        if_exists='append', index=False)
                sql = 'insert into cfmmc_insert_date(host,date,type) values(%s,%s,%s)'
                HSD.runSqlData('carry_investment', sql, (_account, _tradedate, 0))
                # print(f'{tradeDate}的{byType}数据下载成功')
                return True
            except Exception as e:
                print(f'{tradeDate}的{byType}数据下载失败')

    @asyncs
    def down_day_data_sql(self,host, start_date, end_date):
        """ 下载啄日数据并保存到SQL """
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        days = (end_date - start_date).days + 1
        try:
            sql = "SELECT date FROM cfmmc_insert_date WHERE host='{}'".format(host)
            dates = HSD.runSqlData('carry_investment', sql)
            dates = [str(i[0]) for i in dates]
        except:
            dates = []
        for d in range(days):
            date = start_date+datetime.timedelta(d)
            date = str(date)[:10]
            if date not in self._not_trade_list and date not in dates:
                self.save_settlement(date,'date')
                time.sleep(0.1)

def cfmmc_data_page(rq):
    """ 期货监控系统 展示页面 数据返回"""
    start_date = rq.GET.get('start_date')
    end_date = rq.GET.get('end_date')
    try:
        host = rq.session['user_cfmmc']['userID']
        if start_date and end_date and '20' in start_date and '20' in end_date:
            trade = get_cfmmc_trade(host, start_date, end_date)
        else:
            trade = get_cfmmc_trade(host=host)
        start_date = str(trade[-1][11])
        end_date = str(trade[0][11])
    except:
        trade = None
        start_date = ''
        end_date = ''
    return trade,start_date,end_date
    #            print(f'{tradeDate}的{byType}数据下载失败，{e}')
