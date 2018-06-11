from django.shortcuts import render, redirect
from django.http import JsonResponse
import json
import urllib
import h5py
import numpy as np
from django.conf import settings
from dwebsocket.decorators import accept_websocket, require_websocket
from django.http import HttpResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from pytdx.hq import TdxHq_API
# from selenium import webdriver
# from selenium.webdriver import ActionChains
import time, base64
from django.http import HttpResponse
import datetime
import random
import zmq
from zmq import Context
import socket
import requests
import pyquery
import urllib.request as request
import redis
import sys

from mysite import HSD
from mysite import models
from mysite.sub_client import sub_ticker, getTickData


# 分页的一页数量
PAGE_SIZE = 28


# 从缓存读数据
def read_from_cache(user_name):
    key = 'user_id_of_' + user_name
    try:
        value = cache.get(key)
    except Exception as exc:
        HSD.logging.error("文件：views.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))
        value = None
    if value:
        data = json.loads(value)
    else:
        data = None
    return data


# 写数据到缓存
def write_to_cache(user_name, data):
    key = 'user_id_of_' + user_name
    try:
        cache.set(key, json.dumps(data), settings.NEVER_REDIS_TIMEOUT)
    except Exception as exc:
        HSD.logging.error("文件：views.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))


# 更新缓存
def redis_update(rq):
    cache.delete('user_id_of_' + 'data_house')
    cache.delete('user_id_of_' + 'stock_code')

    return render(rq, 'index.html')


def record_from(rq):
    pass  # HSD.logging.info('访问者：'+rq.META.get('REMOTE_ADDR')+'访问页面：'+rq.META.get('HTTP_HOST') + rq.META.get('PATH_INFO'))


def index(rq):
    record_from(rq)
    return render(rq, 'index.html')


# def page_not_found(rq):  # 404页面
#     return render(rq,'page/404page.html')


def stockData(rq):
    '''指定股票历史数据，以K线图显示'''
    code = rq.GET.get('code')
    data = read_from_cache(code)  # 从Redis读取数据
    if not data:
        '''
        conn = get_conn('stockDate')
        #conn = pymysql.connect(db='', user='', passwd='')
        cur = conn.cursor()
        cur.execute('select date,open,close,low,high from transaction_data WHERE code="%s" ORDER BY DATE' % code)
        data = np.array(cur.fetchall())
        conn.close()
        data[:, 0] = [i.strftime('%Y/%m/%d') for i in data[:, 0]]
        data=data.tolist()
        '''
        if socket.gethostname() != 'doc':
            h5 = h5py.File(r'E:\黄海军\资料\Carry\mysite\stock_data.hdf5', 'r')  # r'D:\tools\Tools\stock_data.hdf5'
        else:
            h5 = h5py.File(r'D:\tools\Tools\stock_data.hdf5')
        data1 = h5['stock/' + code + '.day'][:].tolist()
        data = []
        for i in range(len(data1)):
            d = str(data1[i][0])
            data.append(
                [d[:4] + '/' + d[4:6] + '/' + d[6:8]] + [data1[i][1]] + [data1[i][4]] + [data1[i][3]] + [data1[i][2]])

        write_to_cache(code, data)  # 写入数据到Redis

    return render(rq, 'stockData.html', {'data': json.dumps(data), 'code': code})


def stockDatas(rq):
    '''历史股票数据分页显示'''
    conn = HSD.get_conn('stockDate')
    conn1 = HSD.get_conn('stock_data')
    cur1 = conn1.cursor()
    cur = conn.cursor()
    rq_data = rq.GET.get('code')
    dinamic = rq.GET.get('dinamic')
    code_data = read_from_cache('stock_code')
    if not code_data:
        cur1.execute('SELECT * FROM STOCK_CODE')
        code_data = cur1.fetchall()
        write_to_cache('stock_code', code_data)
    if dinamic and rq_data:
        rq_data = rq_data.upper()
        isPage = False
        api = TdxHq_API()
        data = list()
        res_data = [i for i in code_data if
                    rq_data in i[0] or rq_data in i[1] or rq_data in i[2] or (
                        rq_data in i[3] if i[3] else None) or rq_data in i[4]]
        try:
            res_code = {i[-1] + i[0]: i[1] for i in res_data}

            # cur.execute('select date,open,high,low,close,amout,vol,code from moment_hours WHERE amout>0 and code="%s"'%rq_data)
            # data = np.array(cur.fetchall())
            # data[:, 0] = [i.strftime('%Y-%m-%d') for i in data[:, 0]]
            # data = data.tolist()

            with api.connect('119.147.212.81', 7709) as apis:
                xd = 0
                for i in res_code:
                    if i[1] == 'z':
                        data1 = apis.get_security_bars(3, 0, i[2:], 0, 1)
                    elif i[1] == 'h':
                        data1 = apis.get_security_bars(3, 1, i[2:], 0, 1)
                    else:
                        break
                    data.append(
                        [data1[0]['datetime'], data1[0]['open'], data1[0]['high'], data1[0]['low'], data1[0]['close'],
                         data1[0]['amount'], data1[0]['vol'], i])
                    if xd > 28:  # 限定显示的条数
                        break
                    xd += 1
        except:
            pass
    elif rq_data:
        rq_data = rq_data.upper()
        isPage = False
        res_data = [i for i in code_data if
                    rq_data in i[0] or rq_data in i[1] or rq_data in i[2] or (
                        rq_data in i[3] if i[3] else None) or rq_data in i[4]]
        try:
            res_code = {i[-1] + i[0]: i[1] for i in res_data}
            cur1.execute(
                'select date,open,high,low,close,amout,vol,code from moment_hours WHERE amout>0 AND code in (%s) limit 0,100' % str(
                    [i for i in res_code])[1:-1])
            # cur.execute('select date,open,high,low,close,amout,vol,code from moment_hours WHERE amout>0 and code="%s"'%rq_data)
            data = np.array(cur1.fetchall())
            data[:, 0] = [i.strftime('%Y-%m-%d') for i in data[:, 0]]
            data = data.tolist()
        except:
            data = None
    else:
        isPage = True
        try:
            curPage = int(rq.GET.get('curPage', '1'))
            allPage = int(rq.GET.get('allPage', '1'))
            pageType = rq.GET.get('pageType')
        except:
            curPage, allPage = 1, 1
        if curPage == 1 and allPage == 1:
            cur1.execute('select COUNT(1) from moment_hours WHERE amout>0')
            count = cur1.fetchall()[0][0]
            allPage = int(count / PAGE_SIZE) if count % PAGE_SIZE == 0 else int(count / PAGE_SIZE) + 1
        if pageType == 'up':
            curPage -= 1
        elif pageType == 'down':
            curPage += 1
        if curPage < 1:
            curPage = 1
        if curPage > allPage:
            curPage = allPage
        data = read_from_cache('data_house' + str(curPage))
        if not data:
            cur1.execute(
                'select date,open,high,low,close,amout,vol,code from moment_hours WHERE amout>0 limit %s,%s' % (
                    curPage - 1, PAGE_SIZE))
            data = np.array(cur1.fetchall())
            data[:, 0] = [i.strftime('%Y-%m-%d') for i in data[:, 0]]
            data = data.tolist()
            write_to_cache('data_house' + str(curPage), data)

        res_code = {i[-1] + i[0]: i[1] for i in code_data}
        # print(res_code.get('sz000001'))

    conn.close()
    conn1.close()
    # {'data': data,'curPage':curPage,'allPage':allPage}
    data = [i + [res_code.get(i[7])] for i in data] if data else None
    return render(rq, 'stockDatas.html', locals())


def showPicture(rq):
    '''获取指定代码的K线图，显示到页面上'''
    code = rq.GET.get('code')
    if code:
        # d1=urllib.request.urlopen('http://image.sinajs.cn/newchart/daily/n/%s.gif'%code).read()
        # with open('D:\\b.gif','wb') as f:
        #    f.write(d1)
        d = 'http://image.sinajs.cn/newchart/daily/n/%s.gif' % code
        return render(rq, 'stock_min.html', {'picture': d})
    else:
        return redirect('stockDatas')


def is_time(data, minutes):
    '''判断是否在指定时间内'''
    if not data:
        return False
    sj = data[-1] if isinstance(data, list) else data['times']
    if data and datetime.datetime.now() - datetime.datetime.strptime(sj,
                                                                     '%Y-%m-%d %H:%M:%S') < datetime.timedelta(
        minutes=minutes):
        return True
    else:
        return False


def get_zx_zt(zt=False, zx=False, status=None):
    zt_data, zx_data = None, None
    if zt:  # 柱状图
        data = read_from_cache('weight')
        tm = time.localtime()
        is_sj = (tm.tm_hour >= 9 and tm.tm_hour < 16) or (tm.tm_hour == 16 and tm.tm_min < 15)
        if is_sj and is_time(data, 0.15):
            data, times = data[:-2], data[-2]
        elif is_sj:
            data, times = HSD.get_price()
            data1 = data.copy()
            data1.append(times)
            data1.append(str(datetime.datetime.now()).split('.')[0])
            write_to_cache('weight', data1)
        elif data:
            data, times = data[:-2], data[-2]
        else:  # 若 到非日盘时间 则直接获取点数数据
            data, times = HSD.get_data()

        dt = [{
            "AREA": {
                'value': '\n'.join(d[-1]),
                'textStyle': {
                    'fontSize': 12,
                    'color': "red"
                }
            } if d[-1] in HSD.WEIGHT else '\n'.join(d[-1]),
            "LANDNUM": {
                'value': d[0],
                'itemStyle': {
                    'normal': {
                        'color': "green"
                    }
                }
            } if d[0] < 0 else d[0]
        } for d in data]
        counts = sum(i[0] for i in data)
        zt_data = {"jinJian": dt, 'times': times[10:], 'counts': counts}

    if zx:  # 折线图
        result = read_from_cache('history_weight' + str(status))
        if not is_time(result, 0.15):
            result = HSD.get_min_history() if status else HSD.get_history(HSD.get_conn('stock_data'))
            # result = HSD.get_min_history()
            result['times'] = str(datetime.datetime.now()).split('.')[0]
            write_to_cache('history_weight' + str(status), result)
        del result['times']
        names = HSD.WEIGHT
        data = [
            {
                'name': name,
                'type': 'line',
                'tiled': '点数',
                'data': [i[0] for i in result[name]],
                'symbol': 'circle',
                'symbolSize': 8,
                'itemStyle': {
                    'normal': {
                        'lineStyle': {
                            'width': 3 if name in HSD.WEIGHT[:4] else 1,
                            'color': HSD.COLORS[ind],
                            'type': 'dotted' if name in HSD.WEIGHT[:1] else 'solid'
                        }
                    }
                }
            }
            for ind, name in enumerate(names)
        ]
        times2 = [str(i[1]) for i in result[names[0]]]
        zx_data = {'data': data, 'names': list(names), 'times': times2, 'colors': HSD.COLORS[:len(names)]}
    return zt_data, zx_data


def getData(rq):
    '''ajax请求数据'''
    record_from(rq)
    types = rq.GET.get('types')
    if rq.method == 'GET' and rq.is_ajax():
        dt, _ = get_zx_zt(zt=True)
        # data, times = data[:-2], data[-2]

        # for i in data:
        #     counts += i[0]
        # HSD.logging.info(counts)
        # dt = {"jinJian": dt, 'times': times[10:], 'counts': counts}  # "dt1":dt1,
        if types == '2':
            conn = HSD.get_conn('stock_data')
            cur = conn.cursor()
            cur.execute('SELECT TIME,SUM(number) FROM weight GROUP BY TIME')
            wei_sum = cur.fetchall()
            conn.close()
            dt1 = [{'ZX': str(i[0])[10:], 'ZY': i[1]} for i in wei_sum]
            dt['dt1'] = dt1
        return JsonResponse(dt, safe=False)


def zhutu(rq):
    '''柱状图'''
    record_from(rq)
    return render(rq, 'zhutu.html')


def zhutu_zhexian(rq):
    return render(rq, 'zt_zx.html')


def zhutu_zhexian_ajax(rq):
    status = rq.GET.get('s')
    tm = time.localtime()
    tm = tm.tm_sec > 45  # 每分钟清除折线图缓存一次
    if status and status is not '1':
        return redirect('index')
    if rq.method == 'GET' and rq.is_ajax():
        zt, zx = get_zx_zt(zt=True, zx=True, status=status)
        return JsonResponse({'zt': zt, 'zx': zx, 'tm': tm}, safe=False)


def zhexian(rq):
    '''折线图'''
    record_from(rq)
    status = rq.GET.get('s')
    if status and status is not '1':
        return redirect('index')
    _, result = get_zx_zt(zx=True, status=status)

    # names = [i.strip() for i in names]

    return render(rq, 'zhexian.html', result)


def zhutu2(rq):
    return render(rq, 'zhutu2.html')


def tongji(rq):
    dates = HSD.get_date()
    id_name = HSD.get_idName()
    messages = ''
    if rq.method == 'POST':
        id = rq.POST.get('id')
        name = rq.POST.get('name')
        en = rq.POST.get('en')
        passwd = rq.POST.get('pass')
        types = rq.POST.get('types')
        if passwd == HSD.get_ud() + dates[-2:] and id:
            ys = {'YES': '1', 'NO': '0', '1': '1', '0': '0'}
            en = ys.get(en.upper())
            try:
                conn = HSD.get_conn('carry_investment')
                cur = conn.cursor()
                if types == 'update' and name and en and name != 'None':
                    if en:
                        sql = "UPDATE account_info SET trader_name='{}',available='{}' WHERE id={}".format(name, en, id)
                        cur.execute(sql)
                        conn.commit()
                        messages = '修改成功'
                elif types == 'delete' and id and en == '0':
                    sql = "delete from account_info where id={}".format(id)
                    cur.execute(sql)
                    conn.commit()
                    messages = '删除成功'
                else:
                    messages = '操作失败'
            except:
                conn.rollback()
                messages = '操作失败'
            finally:
                conn.close()
        else:
            messages = '验证码错误！'

    rq_date = rq.GET.get('datetimes')
    end_date = rq.GET.get('end_date')
    rq_id = rq.GET.get('id')
    rq_type = rq.GET.get('type')
    try:
        rq_id = int(rq_id)
    except:
        rq_id = 0

    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10] if not end_date else end_date

    if rq_type == '2' and rq_date and end_date and rq_id != 0:
        results2, price = HSD.order_detail(rq_date, end_date)
        res = {}
        all_price = []
        results2 = [result for result in results2 if rq_id == result[0]]
        huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
                   'avg_day': 0, 'least2': 0, 'most2': 0}
        for i in results2:
            dt = str(i[1])[:10]
            if dt not in res:
                res[dt] = {'duo': 0, 'kong': 0, 'mony': 0, 'shenglv':0,'ylds':0, 'datetimes': []}
            if i[8] == 2:
                if i[6] == 0:
                    res[dt]['duo'] += 1
                elif i[6] == 1:
                    res[dt]['kong'] += 1
                res[dt]['mony'] += i[5]
                xx = [str(i[1]), str(i[3]), '空', i[5]] if i[6] == 1 else [str(i[1]), str(i[3]), '多', i[5]]
                res[dt]['datetimes'].append(xx)

                huizong['least'] = [dt, i[5]] if i[5] < huizong['least'][1] else huizong[
                    'least']
                huizong['most'] = [dt, i[5]] if i[5] > huizong['most'][1] else huizong[
                    'most']


        res_key = list(res.keys())
        for i in res_key:
            mony = res[i]['mony']
            huizong['yk'] += mony
            huizong['zl'] += (res[i]['duo'] + res[i]['kong'])

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
        huizong['shenglv'] += len([p for p in all_price if p > 0])
        huizong['shenglv'] = int(huizong['shenglv'] / huizong['zl'] * 100) if huizong['zl'] > 0 else 0  # 胜率
        huizong['avg'] = huizong['yk'] / huizong['zl'] if huizong['zl'] > 0 else 0  # 平均每单盈亏
        res_size = len(res)
        huizong['avg_day'] = huizong['yk'] / res_size if res_size > 0 else 0  # 平均每天盈亏
        huizong['least2'] = min(all_price)
        huizong['most2'] = max(all_price)
        conn = HSD.get_conn('carry_investment')
        sql = "SELECT origin_asset FROM account_info WHERE id={}".format(rq_id)
        init_money = HSD.getSqlData(conn,sql)
        conn.close()
        init_money = init_money[0][0]
        hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date)
        fa = '0'
        return render(rq, 'hc.html', {'hc': hc, 'huizong': huizong, 'fa': fa, 'init_money':init_money,})


        # hc, huizong = HSD.huices(res,huizong,init_money,dates,end_date)
    if rq_type == '1' and rq_date:
        dates = rq_date
        results2, price = HSD.order_detail(rq_date, end_date)
        huizong = []
        if results2:
            for i in HSD.IDS:
                hz0 = min([j[1] for j in results2 if j[0] == i])  # 开始时间
                hz1 = sum(
                    [j[5] if j[8] == 2 else ((price - j[2] if j[6] == 0 else j[2] - price) if j[8] == 1 else j[5]) for j
                     in results2 if j[0] == i])  # 盈亏
                hz2 = len([j[6] for j in results2 if j[0] == i and j[6] == 0])  # 多单数量
                hz3 = len([j[7] for j in results2 if j[0] == i and j[6] == 1])  # 空单数量
                hz4 = len([j[5] for j in results2 if j[0] == i and j[5] > 0])  # 赢利单数
                hz5 = int(hz4 / (hz2 + hz3) * 100) if hz2 + hz3 != 0 else 0  # 正确率
                huizong.append([str(hz0)[:10], i, hz1, hz2, hz3, hz4, hz5])
            results = np.array(results2)
            id_count = {i: len(results[np.where(results[:, 0] == i)]) for i in HSD.IDS}
            # results = list(results)
        if rq_id != 0:
            results2 = [result for result in results2 if rq_id == result[0]]

        ids = HSD.IDS
        results2 = tuple(reversed(results2))
        if results2:
            return render(rq, 'tongji.html', locals())
    if rq_date:
        results = HSD.calculate_earn(rq_date, end_date)
        huizong = []
        if results:
            for i in HSD.IDS:
                hz1 = sum([j[6] for j in results if j[2] == i])
                hz2 = sum([j[7] for j in results if j[2] == i])
                hz3 = sum([j[8] for j in results if j[2] == i])
                huizong.append([rq_date, i, hz1, hz2, hz3])

            results = np.array(results)
            id_count = {i: len(results[np.where(results[:, 2] == i)]) for i in HSD.IDS}
            results = list(results)
        if rq_id != 0:
            results = [result for result in results if rq_id == result[2]]

        ids = HSD.IDS
        dates = rq_date
        # {'results':results,'dates':rq_date,'ids':HSD.IDS,'huizong':huizong,'id_name':id_name,'id_count':id_count}
        if results:
            return render(rq, 'tongji.html', locals())

    #rq_id = rq_id if rq_id else 0
    herys = None
    try:
        herys = HSD.tongji()
    except Exception as exc:
        HSD.logging.error("文件：views.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))
    if not herys:
        return redirect('index')
    ids = HSD.IDS
    return render(rq, 'tongji.html', locals())


def tools(rq):
    cljs = models.Clj.objects.all()
    return render(rq, 'tools.html', {'cljs': cljs})


def kline(rq):
    date = rq.GET.get('date', HSD.get_date())
    write_to_cache('kline_date', date)
    database = rq.GET.get('database', '1')
    write_to_cache('kline_database', database)
    return render(rq, 'kline.html', {'date': date})


def getList():
    # 时间,开盘价,最高价,最低价,收盘价,成交量
    data_dict = {'1': ['carry_investment', 'futures_min'], '2': ['stock_data', 'index_min']}
    dates = read_from_cache('kline_date')
    database = read_from_cache('kline_database')
    if dates and database:
        conn = HSD.get_conn(data_dict[database][0])
        cur = conn.cursor()
        if len(dates) == 10:
            dates2 = HSD.dtf(dates) + datetime.timedelta(days=1)
        else:
            dates2 = HSD.dtf(dates)
            dates = dates2 - datetime.timedelta(minutes=20)
            dates2 = dates2 + datetime.timedelta(days=1)
            dates2 = str(dates2)[:10]

        if database == '1':
            sql = 'SELECT datetime,open,high,low,close,vol FROM %s WHERE datetime>="%s" AND datetime<"%s"' % (
                data_dict[database][1], dates, dates2)
        elif database == '2':
            sql = 'SELECT datetime,open,high,low,close,vol FROM %s WHERE code="HSIc1" AND datetime>="%s" AND datetime<"%s"' % (
                data_dict[database][1], dates, dates2)
        cur.execute(sql)
        res = list(cur.fetchall())
        conn.commit()
        conn.close()
        if len(res) > 0:
            res = [
                [int(time.mktime(time.strptime(str(i[0]), "%Y-%m-%d %H:%M:%S")) * 1000), i[1], i[2], i[3], i[4], i[5]]
                for i in res]
            _ch = []
            return res, _ch

    if len(res) > 0:
        res = [[int(time.mktime(time.strptime(str(i[0]), "%Y-%m-%d %H:%M:%S")) * 1000), i[1], i[2], i[3], i[4], i[5]]
               for i in res]
    data2 = HSD.Zbjs().vis(res)
    dc = data2.send(None)
    data2.send(None)
    _ch = [d['cd'] for d in dc]
    return res, _ch


def GetRealTimeData(times, price, amount):
    '''得到推送点数据'''
    amount = amount
    is_time = cache.get('is_time')
    objArr = cache.get("objArr")
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
        cache.set("objArr", objArr, 60)
    else:
        objArr = [
            times * 1000,  # 时间
            price,  # 开盘价
            price,  # 高
            price,  # 低
            price,  # 收盘价
            amount  # 量
        ]
        cache.set('is_time', times, 60)
        cache.set("objArr", objArr, 60)


@csrf_exempt  # 取消csrf验证
def getkline(rq):
    size = rq.POST.get('size')
    size = int(size) if size else 0
    types = rq.POST.get('type')  # 获取分钟类型
    # print('.....................',types,size)
    # 1min 5min 15min 30min 1hour 1day 1week
    if rq.is_ajax() and size > 0:
        lists, _ch = getList()
        # _ch = [random.choice([0, 0, 0, 0, 0, 0]) for i in range(len(lists))]
        data = {
            'des': "注释",
            'isSuc': True,  # 状态
            'datas': {
                'USDCNY': 6.83,  # RMB汇率
                'contractUnit': "BTC",
                'data': lists,
                'marketName': "凯瑞投资",
                'moneyType': "CNY",
                'symbol': 'carry',
                'url': '官网地址',  # （选填）
                'topTickers': [],  # （选填）
                # '_ch': _ch
            }
        }
        return HttpResponse(json.dumps(data), content_type="application/json")
    elif rq.is_ajax() and size == 0:
        lists, _ch = getList()
        # _ch = [random.choice([0, 0, 0, 0, 0, 0]) for i in range(len(lists))]
        data = {
            'des': "注释",
            'isSuc': True,  # 状态
            'datas': {
                'USDCNY': 6.83,  # RMB汇率
                'contractUnit': "BTC",
                'data': lists,
                'marketName': "凯瑞投资",
                'moneyType': "CNY",
                'symbol': 'carry',
                'url': '官网地址',  # （选填）
                'topTickers': [],  # （选填）
                # '_ch': _ch
            }
        }
        return HttpResponse(json.dumps(data), content_type="application/json")

    else:
        return redirect('index')


@accept_websocket
def getwebsocket(rq):
    zbjs = HSD.Zbjs().main()
    zs = zbjs.send(None)
    if rq.is_websocket():
        tcp = HSD.get_tcp()
        poller = zmq.Poller()
        ctx1 = Context()
        sub_socket = ctx1.socket(zmq.SUB)
        sub_socket.connect('tcp://{}:6868'.format(tcp))
        sub_socket.setsockopt_unicode(zmq.SUBSCRIBE, '')
        poller.register(sub_socket, zmq.POLLIN)
        for message in rq.websocket:
            while 1:  # 循环推送数据
                ticker = sub_socket.recv_pyobj()
                this_time = ticker.TickerTime
                objArr = cache.get("objArr")
                times, opens, high, low, close, vol = objArr if objArr else (
                    ticker.TickerTime * 1000, ticker.Price, ticker.Price, ticker.Price, ticker.Price, ticker.Qty)
                GetRealTimeData(ticker.TickerTime, ticker.Price, ticker.Qty)
                zs = 0
                if time.localtime(this_time).tm_min != time.localtime(times / 1000).tm_min:
                    tm = time.localtime(times / 1000)
                    tm = datetime.datetime(tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min)
                    zs = zbjs.send((tm, opens, high, low, close))
                    zs = zs[tm]['datetimes'][-1][1] if zs[tm]['datetimes'] else 0
                if this_time * 1000 != times:
                    data = {'times': str(times), 'opens': str(opens), 'high': str(high), 'low': str(low),
                            'close': str(close), 'vol': str(vol), 'zs': str(zs)}  # ,'_ch':0
                    data = json.dumps(data).encode()
                    rq.websocket.send(data)
        zbjs.send(None)
    else:
        return redirect('index')


def zhangting(rq, t):
    dates = HSD.get_date()
    ZT = HSD.Limit_up()
    rq_date = rq.GET.get('date', dates)
    if t == 'today':
        zt = ZT.read_code()
        zt = sorted(zt, key=lambda x: x[2])  # 以第3个参数排序
        zt.reverse()
        return render(rq, 'zhangting.html', {'zt_today': zt, 'dates': dates})
    if not rq_date:
        return render(rq, 'zhangting.html', {'jyzt': False, 'dates': dates})
    if t == 'tomorrow':
        datet = HSD.dtf(rq_date)
        day = datet.weekday()
        day_up = 1 if 6 > day > 0 else (3 if day == 0 else 2)
        day_down = 1 if day < 4 or day == 6 else (3 if day == 4 else 2)
        date_up = str(datet - datetime.timedelta(days=day_up))[:10]
        date_down = str(datet + datetime.timedelta(days=day_down))[:10]
        zt_tomorrow = ZT.yanzen(rq_date=rq_date)
        if zt_tomorrow:
            for i in range(len(zt_tomorrow)):
                zt_tomorrow[i].append(zt_tomorrow[i][0])
                zt_tomorrow[i][0] = zt_tomorrow[i][0][2:]
                zt_tomorrow[i][2] = range(zt_tomorrow[i][2])  # ['★' for j in range(zt_tomorrow[i][2])]
        return render(rq, 'zhangting.html',
                      {'jyzt': True, 'zt_tomorrow': zt_tomorrow, 'dates': rq_date, 'up': date_up, 'down': date_down})

    return redirect('index')


def moni(rq):
    dates = rq.GET.get('dates')
    end_date = rq.GET.get('end_date')
    fa = rq.GET.get('fa')
    database = rq.GET.get('database', '1')
    reverse = rq.GET.get('reverse')
    zsds = rq.GET.get('zsds')  # 止损
    ydzs = rq.GET.get('ydzs')  # 移动止损
    zyds = rq.GET.get('zyds')  # 止盈
    cqdc = rq.GET.get('cqdc')  # 点差
    # zsds, ydzs, zyds, cqdc
    zsds, ydzs, zyds, cqdc = HSD.format_int(zsds, ydzs, zyds, cqdc) if zsds and ydzs and zyds and cqdc else (
    100, 100, 200, 6)
    reverse = True if reverse else False
    zbjs = HSD.Zbjs()
    ma = 60
    if dates and end_date and fa:
        try:
            param = {'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc}
            res, huizong, first_time = zbjs.main2(_ma=ma, _dates=dates, end_date=end_date, _fa=fa, database=database,
                                                  reverse=reverse, param=param)
            keys = sorted(res.keys())
            keys.reverse()
            res = [dict(res[k], **{'time': k}) for k in keys]
            fa_doc = zbjs.fa_doc
            return render(rq, 'moni.html',
                          {'res': res, 'keys': keys, 'dates': dates, 'end_date': end_date, 'fa': fa, 'fas': zbjs.xzfa,
                           'fa_doc': fa_doc, 'fa_one': fa_doc.get(fa), 'huizong': huizong, 'database': database,
                           'first_time': first_time, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc})
        except Exception as exc:
            print ('Err: moni ', exc)
    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    return render(rq, 'moni.html', {'dates': dates, 'end_date': end_date, 'fas': zbjs.xzfa, 'database': database,
                                    'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc})


def newMoni(rq):
    dates = rq.GET.get('dates')
    end_date = rq.GET.get('end_date')
    database = rq.GET.get('database', '1')
    reverse = rq.GET.get('reverse')
    zsds = rq.GET.get('zsds')  # 止损
    ydzs = rq.GET.get('ydzs')  # 移动止损
    zyds = rq.GET.get('zyds')  # 止盈
    cqdc = rq.GET.get('cqdc')  # 点差
    # zsds, ydzs, zyds, cqdc
    zsds, ydzs, zyds, cqdc = HSD.format_int(zsds, ydzs, zyds, cqdc) if zsds and ydzs and zyds and cqdc else (
    100, 80, 200, 6)
    reverse = True if reverse else False
    # duo_macd, duo_avg, duo_yidong, duo_chonghes, duo_chonghed, kong_macd, kong_avg, kong_yidong, kong_chonghes, kong_chonghed, pdd_macd, pdd_avg, pdd_yidong, pdd_chonghes, pdd_chonghed, pkd_macd, pkd_avg, pkd_yidong, pkd_chonghes, pkd_chonghed
    duo_macd = rq.GET.get("duo_macd")  # macd小于大于零
    duo_avg = rq.GET.get("duo_avg")  # 收盘价小于大于60均线
    duo_yidong = rq.GET.get("duo_yidong")  # 异动小于大于1.5倍
    duo_chonghes = rq.GET.get("duo_chonghes")  # 阴线阳线重合
    duo_chonghed = rq.GET.get("duo_chonghed")  # 前阳后阴 前阴后阳重合
    kong_macd = rq.GET.get("kong_macd")
    kong_avg = rq.GET.get("kong_avg")
    kong_yidong = rq.GET.get("kong_yidong")
    kong_chonghes = rq.GET.get("kong_chonghes")
    kong_chonghed = rq.GET.get("kong_chonghed")
    pdd_macd = rq.GET.get("pdd_macd")
    pdd_avg = rq.GET.get("pdd_avg")
    pdd_yidong = rq.GET.get("pdd_yidong")
    pdd_chonghes = rq.GET.get("pdd_chonghes")
    pdd_chonghed = rq.GET.get("pdd_chonghed")
    pkd_macd = rq.GET.get("pkd_macd")
    pkd_avg = rq.GET.get("pkd_avg")
    pkd_yidong = rq.GET.get("pkd_yidong")
    pkd_chonghes = rq.GET.get("pkd_chonghes")
    pkd_chonghed = rq.GET.get("pkd_chonghed")

    duo = duo_macd or duo_avg or duo_yidong or duo_chonghes or duo_chonghed
    kong = kong_macd or kong_avg or kong_yidong or kong_chonghes or kong_chonghed
    pdd = pdd_macd or pdd_avg or pdd_yidong or pdd_chonghes or pdd_chonghed
    pkd = pkd_macd or pkd_avg or pkd_yidong or pkd_chonghes or pkd_chonghed

    zbjs = HSD.Zbjs()
    ma = 60
    if dates and end_date and (duo and pdd or kong and pkd):
        try:
            param = {
                'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc,
                "duo_macd": duo_macd, "duo_avg": duo_avg, "duo_yidong": duo_yidong,
                "duo_chonghes": duo_chonghes, "duo_chonghed": duo_chonghed, "kong_macd": kong_macd,
                "kong_avg": kong_avg, "kong_yidong": kong_yidong, "kong_chonghes": kong_chonghes,
                "kong_chonghed": kong_chonghed, "pdd_macd": pdd_macd, "pdd_avg": pdd_avg,
                "pdd_yidong": pdd_yidong, "pdd_chonghes": pdd_chonghes, "pdd_chonghed": pdd_chonghed,
                "pkd_macd": pkd_macd, "pkd_avg": pkd_avg, "pkd_yidong": pkd_yidong,
                "pkd_chonghes": pkd_chonghes, "pkd_chonghed": pkd_chonghed,
                "duo": duo, "kong": kong, "pdd": pdd, "pkd": pkd,
            }
            res, huizong, first_time = zbjs.main_new(_ma=ma, _dates=dates, end_date=end_date, database=database,
                                                     reverse=reverse, param=param)
            keys = sorted(res.keys())
            keys.reverse()
            res = [dict(res[k], **{'time': k}) for k in keys]
            fa_doc = zbjs.fa_doc
            return render(rq, 'new_moni.html',
                          {'res': res, 'keys': keys, 'dates': dates, 'end_date': end_date, 'fas': zbjs.xzfa,
                           'fa_doc': fa_doc, 'fa_one': 'fa_doc.get(fa)', 'huizong': huizong, 'database': database,
                           'first_time': first_time, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc,

                           "duo_macd": duo_macd, "duo_avg": duo_avg, "duo_yidong": duo_yidong,
                           "duo_chonghes": duo_chonghes, "duo_chonghed": duo_chonghed, "kong_macd": kong_macd,
                           "kong_avg": kong_avg, "kong_yidong": kong_yidong, "kong_chonghes": kong_chonghes,
                           "kong_chonghed": kong_chonghed, "pdd_macd": pdd_macd, "pdd_avg": pdd_avg,
                           "pdd_yidong": pdd_yidong, "pdd_chonghes": pdd_chonghes, "pdd_chonghed": pdd_chonghed,
                           "pkd_macd": pkd_macd, "pkd_avg": pkd_avg, "pkd_yidong": pkd_yidong,
                           "pkd_chonghes": pkd_chonghes, "pkd_chonghed": pkd_chonghed,
                           })
        except Exception as exc:
            print ('Err: new_moni ', exc)
    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    return render(rq, 'new_moni.html', {'dates': dates, 'end_date': end_date, 'fas': zbjs.xzfa, 'database': database,
                                        'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc})


def moni_all(rq):
    dates = rq.GET.get('dates')
    end_date = rq.GET.get('end_date')
    database = rq.GET.get('database', '1')
    reverse = rq.GET.get('reverse')
    zsds = rq.GET.get('zsds')  # 止损
    ydzs = rq.GET.get('ydzs')  # 移动止损
    zyds = rq.GET.get('zyds')  # 止盈
    cqdc = rq.GET.get('cqdc')  # 点差
    # zsds, ydzs, zyds, cqdc
    zsds, ydzs, zyds, cqdc = HSD.format_int(zsds, ydzs, zyds, cqdc) if zsds and ydzs and zyds and cqdc else (
        100, 80, 200, 6)
    reverse = True if reverse else False
    zbjs = HSD.Zbjs()
    if dates and end_date:
        try:
            param = {'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc}
            res, huizong = zbjs.main_all(_dates=dates, end_date=end_date, database=database, reverse=reverse,
                                         param=param)
            dt = list(set([j for i in res for j in res[i]]))
            dt.sort()
            dt.reverse()
            ress = {}
            this_date = str(datetime.datetime.now())[:16]
            for i in dt:
                gdzds = read_from_cache('gdzds')
                gdzds = gdzds if gdzds else {}
                if i in gdzds and i == this_date[:10] and this_date == gdzds.get('first_time'):
                    ress[i] = {'hq': gdzds[i]}  # 行情
                elif i in gdzds and i != this_date[:10]:
                    ress[i] = {'hq': gdzds[i]}  # 行情
                else:
                    gd, zd, gs, zs = zbjs.get_future(i)
                    gd = gd * 30 if gd > 0 else 10
                    zd = zd * 30 if zd > 0 else 10

                    gdzds[i] = [gd, zd, gs, zs]
                    gdzds['first_time'] = this_date
                    write_to_cache('gdzds', gdzds)
                    ress[i] = {'hq': gdzds[i]}  # 行情
                ind = 0
                for fa in res:
                    fav = res[fa].get(i)
                    if not fav:
                        continue
                    yks = res[fa][i]['mony']
                    if ind == 0:
                        max_yk, min_yk = [fa, yks], [fa, yks]  # 最大盈利，最大亏损
                        ind += 1
                    elif fa != '5':  # 方案五不在范围内
                        max_yk = [fa, yks] if yks > max_yk[1] else max_yk
                        min_yk = [fa, yks] if yks < min_yk[1] else min_yk

                    yk = [m[3] for m in fav['datetimes']]
                    ress[i][fa] = {
                        'sl': fav['shenglv'],  # 胜率
                        'yk': yks,  # 盈亏
                        'duo': fav['duo'],  # 多单数量
                        'kong': fav['kong'],  # 空单数量
                        'maxk': min(yk),  # 最大亏损
                        'maxy': max(yk),  # 最大盈利
                    }
                ress[i][max_yk[0]]['max_yk'] = 1
                ress[i][min_yk[0]]['min_yk'] = 1
            return render(rq, 'moniAll.html',
                          {'ress': ress, 'huizongs': huizong, 'fa_doc': zbjs.fa_doc, 'dates': dates,
                           'end_date': end_date,
                           'database': database, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc})
        except Exception as exc:
            HSD.logging.error("文件：views.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))
    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    return render(rq, 'moniAll.html',
                  {'dates': dates, 'end_date': end_date, 'database': database, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds,
                   'cqdc': cqdc})


def bfsy(rq):
    if rq.method == 'GET' and rq.is_ajax():
        msg = rq.GET.get('msg')
        if msg:
            appkey = "14283e062e694e7398453680634cbcfc"
            url = "http://www.tuling123.com/openapi/api?key=%s&info=%s" % (appkey, msg)
            content = requests.get(url).text
            data = json.loads(content)
            answer = data['text']
            return HttpResponse(answer)

    return redirect('index')


def gdzd(rq):
    gdzds = read_from_cache('gdzds')
    gdzds = gdzds if gdzds else {}
    i = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    if i in gdzds:
        gd, zd, gs, zs = gdzds[i]
    else:
        zbjs = HSD.Zbjs()
        gd, zd, gs, zs = zbjs.get_future(str(datetime.datetime.now())[:10])
        gd = gd * 30 if gd > 0 else 10
        zd = zd * 30 if zd > 0 else 10
    return render(rq, 'zdzd.html', {'gd': gd, 'zd': zd})


def huice(rq):
    dates = rq.GET.get('dates')
    end_date = rq.GET.get('end_date')
    fa = rq.GET.get('fa')
    database = rq.GET.get('database', '1')
    reverse = rq.GET.get('reverse')
    zsds = rq.GET.get('zsds')  # 止损
    ydzs = rq.GET.get('ydzs')  # 移动止损
    zyds = rq.GET.get('zyds')  # 止盈
    cqdc = rq.GET.get('cqdc')  # 点差
    # zsds, ydzs, zyds, cqdc
    zsds, ydzs, zyds, cqdc = HSD.format_int(zsds, ydzs, zyds, cqdc) if zsds and ydzs and zyds and cqdc else (
        100, 80, 200, 6)
    reverse = True if reverse else False
    zbjs = HSD.Zbjs()
    ma = 60
    init_money = 5000
    if dates and end_date and fa:
        try:
            param = {'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc}
            res, huizong, first_time = zbjs.main2(_ma=ma, _dates=dates, end_date=end_date, _fa=fa, database=database,
                                                  reverse=reverse, param=param)
            hc, huizong = HSD.huices(res,huizong,init_money,dates,end_date)
        except Exception as exc:
            HSD.logging.error("文件：views.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))

        return render(rq, 'hc.html', {'hc': hc, 'huizong': huizong, 'fa': fa})

    return render(rq, 'hc.html')


def account_info_update(rq):
    ''' 刷新交易统计表 '''
    conn = HSD.get_conn('carry_investment')
    cur = conn.cursor()
    cur.execute("CALL account_info_update")
    conn.commit()
    conn.close()
    return redirect('tongji')


def journalism(rq):
    if rq.is_ajax:
        d = read_from_cache("journalism")
        if not d or not is_time(d, 0.25):
            url = 'https://www.jin10.com'
            d = request.urlopen(url).read()
            d = d.decode('utf-8')
            d = pyquery.PyQuery(d)
            d = d.find('.jin-flash_b')
            d = d.text()
            d = d.split('。 ')[:10]
            d = [i for i in d if '金十' not in i][:5]
            d.append(str(datetime.datetime.now())[:19])
            write_to_cache("journalism", d)
        d = {str(i): d[i] for i in range(len(d) - 1)}
        return JsonResponse(d)
    return redirect('index')


def liaotianshiList(rq):
    r = redis.Redis(host='localhost')
    ltsName = r.get('liaotianshiList')
    ltsName = json.loads(ltsName)
    return JsonResponse(ltsName)


def websocket_test(rq):
    s = rq.POST.get('inputText')
    if s:
        r = HSD.RedisHelper()
        r.main()
    return render(rq, "main.html")
