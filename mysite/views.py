from django.shortcuts import render, redirect
from django.http import JsonResponse
import json, urllib, h5py
import numpy as np
from django.conf import settings
from dwebsocket.decorators import accept_websocket,require_websocket
from django.http import HttpResponse
from django.core.cache import cache
from django.views.decorators.csrf import csrf_exempt
from pytdx.hq import TdxHq_API
# from selenium import webdriver
# from selenium.webdriver import ActionChains
import time, base64
from django.http import HttpResponse
import datetime
import logging
import random
import zmq
from zmq import Context
import socket
import requests
import pyquery
import urllib.request as request

from mysite import HSD
from mysite import models
from mysite.sub_client import sub_ticker,getTickData


logging.basicConfig(
    filename='log\\logging.log',
    filemode='a',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    format='%(filename)s[%(asctime)s][%(levelname)s]：%(message)s'
)

# 分页的一页数量
PAGE_SIZE = 28


# 从缓存读数据
def read_from_cache(user_name):
    key = 'user_id_of_' + user_name
    try:
        value = cache.get(key)
    except Exception as exc:
        logging.error(exc)
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
        logging.error(exc)


# 更新缓存
def redis_update(rq):
    cache.delete('user_id_of_' + 'data_house')
    cache.delete('user_id_of_' + 'stock_code')

    return render(rq, 'index.html')


def record_from(rq):
    pass  # logging.info('访问者：'+rq.META.get('REMOTE_ADDR')+'访问页面：'+rq.META.get('HTTP_HOST') + rq.META.get('PATH_INFO'))


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
        if socket.gethostname()!='doc':
            h5 = h5py.File(r'E:\黄海军\资料\Carry\mysite\stock_data.hdf5', 'r') #r'D:\tools\Tools\stock_data.hdf5'
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


def is_time(data,minutes):
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


def getData(rq):
    '''ajax请求数据'''
    record_from(rq)
    types=rq.GET.get('types')
    if rq.method == 'GET' and rq.is_ajax():
        data = read_from_cache('weight')
        if is_time(data,0.15):
            data, times = data[:-2], data[-2]
        else:
            # 若时间到15点则直接获取点数数据
            if int(str(datetime.datetime.now())[11:13]) >= 17:
                data, times = HSD.get_data()
            else:
                data, times = HSD.get_price()
            # data, times = HSD.get_price()
            # if not data:
            #    data, times = HSD.get_data()
            # data5, times5 = HSD.get_data()
            # for i in data5:
            #     for j in data:
            #         if j[1]==i[1] and j[0]!=i[0]:
            #             print(i,j)
            data1 = data.copy()
            data1.append(times)
            data1.append(str(datetime.datetime.now()).split('.')[0])
            write_to_cache('weight', data1)

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

        counts = 0
        for i in data:
            counts += i[0]
        # logging.info(counts)
        dt = {"jinJian": dt, 'times': times[10:], 'counts': counts}  # "dt1":dt1,
        if types=='2':
            conn=HSD.get_conn('stock_data')
            cur=conn.cursor()
            cur.execute('SELECT TIME,SUM(number) FROM weight GROUP BY TIME')
            wei_sum=cur.fetchall()
            conn.close()
            dt1=[{'ZX':str(i[0])[10:],'ZY':i[1]} for i in wei_sum]
            dt['dt1']=dt1
        return JsonResponse(dt, safe=False)


def zhutu(rq):
    '''柱状图'''
    record_from(rq)
    return render(rq, 'zhutu.html')


def zhexian(rq):
    '''折线图'''
    record_from(rq)
    status = rq.GET.get('s')
    if status and status is not '1':
        return redirect('index')
    result = read_from_cache('history_weight' + str(status))
    if not is_time(result,0.15):
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
            'symbolSize': 10,
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

    times = [str(i[1]) for i in result[names[0]]]
    # names = [i.strip() for i in names]

    return render(rq, 'zhexian.html',
                  {'data': data, 'names': list(names), 'times': times, 'colors': json.dumps(HSD.COLORS)})


def zhutu2(rq):
    return render(rq, 'zhutu2.html')


def tongji(rq):
    dates = HSD.get_date()
    id_name = HSD.get_idName()
    messages=''
    if rq.method=='POST':
        id=rq.POST.get('id')
        name=rq.POST.get('name')
        en=rq.POST.get('en')
        passwd=rq.POST.get('pass')
        types=rq.POST.get('types')
        if passwd==HSD.get_ud()+dates[-2:] and id:
            ys = {'YES': '1', 'NO': '0', '1': '1', '0': '0'}
            en = ys.get(en.upper())
            try:
                conn = HSD.get_conn('carry_investment')
                cur = conn.cursor()
                if types=='update' and name and en and name != 'None':
                    if en:
                        sql = "UPDATE account_info SET trader_name='{}',available='{}' WHERE id={}".format(name, en, id)
                        cur.execute(sql)
                        conn.commit()
                        messages='修改成功'
                elif types=='delete' and id and en == '0':
                    sql="delete from account_info where id={}".format(id)
                    cur.execute(sql)
                    conn.commit()
                    messages='删除成功'
                else:
                    messages = '操作失败'
            except:
                conn.rollback()
                messages = '操作失败'
            finally:
                conn.close()
        else:
            messages = '验证码错误！'
    try:
        rq_date = rq.GET.get('datetimes')
        rq_ts = int(rq.GET.get('rq_ts','1'))
        rq_id=rq.GET.get('id')
        rq_type=rq.GET.get('type')
    except:
        pass
    if rq_type=='1' and rq_date:
        dates = rq_date
        results2=HSD.order_detail(rq_date,rq_ts)
        huizong = []
        if results2:
            for i in HSD.IDS:
                hz1 = sum([j[5] for j in results2 if j[0] == i])  # 盈亏
                hz2 = len([j[6] for j in results2 if j[0] == i and j[6] == 0]) # 多单数量
                hz3 = len([j[7] for j in results2 if j[0] == i and j[6] == 1]) # 空单数量
                hz4 = len([j[5] for j in results2 if j[0] == i and j[5] > 0]) # 赢利单数
                hz5 = int(hz4/(hz2+hz3)*100)  # 正确率
                huizong.append([rq_date, i, hz1, hz2, hz3, hz4, hz5])
            results = np.array(results2)
            id_count = {i: len(results[np.where(results[:, 0] == i)]) for i in HSD.IDS}
            # results = list(results)
        if rq_id and rq_id.isdecimal() and rq_id is not '1':
            ind=len(results2)
            ind1=0
            rq_id=int(rq_id)
            results1=[]
            while ind1<ind:
                if rq_id == results2[ind1][0]:
                    results1.append(results2[ind1])
                ind1+=1
            results2=results1
        else:
            rq_id='1'
        ids = HSD.IDS
        results2=tuple(reversed(results2))
        return render(rq, 'tongji.html',locals())
    if rq_date:
        results=HSD.calculate_earn(rq_date,rq_ts)
        huizong=[]
        if results:
            for i in HSD.IDS:
                hz1 = sum([j[6] for j in results if j[2] == i])
                hz2 = sum([j[7] for j in results if j[2] == i])
                hz3 = sum([j[8] for j in results if j[2] == i])
                huizong.append([rq_date,i,hz1,hz2,hz3])

            results=np.array(results)
            id_count={i:len(results[np.where(results[:,2]==i)]) for i in HSD.IDS}
            results=list(results)
        if rq_id and rq_id.isdecimal() and rq_id is not '1':
            ind=len(results)
            ind1=0
            rq_id=int(rq_id)
            results1=[]
            while ind1<ind:
                if rq_id == results[ind1][2]:
                    results1.append(results[ind1])
                    #ind-=1
                    #ind1-=1
                ind1+=1
            results=results1
        else:
            rq_id='1'
        ids=HSD.IDS
        dates=rq_date
        #{'results':results,'dates':rq_date,'ids':HSD.IDS,'huizong':huizong,'id_name':id_name,'id_count':id_count}
        return render(rq,'tongji.html',locals())
    else:
        rq_id='1'
    herys = None
    try:
        herys = HSD.tongji()
    except Exception as exc:
        logging.error(exc)
    if not herys:
        return redirect('index')
    ids=HSD.IDS
    return render(rq, 'tongji.html', locals())


def tools(rq):
    cljs = models.Clj.objects.all()
    return render(rq, 'tools.html', {'cljs': cljs})

def kline(rq):
    date = rq.GET.get('date',HSD.get_date())
    write_to_cache('kline_date',date)
    database=rq.GET.get('database','1')
    write_to_cache('kline_database', database)
    return render(rq,'kline.html',{'date':date})

def getList():
    # 时间,开盘价,最高价,最低价,收盘价,成交量
    data_dict={'1':['carry_investment','futures_min'],'2':['stock_data','index_min']}
    dates = read_from_cache('kline_date')
    database=read_from_cache('kline_database')
    if dates and database:
        conn = HSD.get_conn(data_dict[database][0])
        cur = conn.cursor()
        if len(dates)==10:
            dates2=datetime.datetime.strptime(dates, '%Y-%m-%d') + datetime.timedelta(days=1)
        else:
            dates2 = datetime.datetime.strptime(dates, '%Y-%m-%d %H:%M:%S')
            dates = dates2 - datetime.timedelta(minutes=20)
            dates2 = dates2 + datetime.timedelta(days=1)
            dates2 = str(dates2)[:10]

        if database=='1':
            sql = 'SELECT datetime,open,high,low,close,vol FROM %s WHERE datetime>="%s" AND datetime<"%s"' % (
                data_dict[database][1], dates, dates2)
        elif database=='2':
            sql = 'SELECT datetime,open,high,low,close,vol FROM %s WHERE code="HSIc1" AND datetime>="%s" AND datetime<"%s"' % (
                data_dict[database][1], dates, dates2)
        cur.execute(sql)
        res = list(cur.fetchall())
        conn.commit()
        conn.close()
        if len(res) > 0:
            res = [[int(time.mktime(time.strptime(str(i[0]), "%Y-%m-%d %H:%M:%S")) * 1000), i[1], i[2], i[3], i[4], i[5]] for i in res]
            _ch = []
            return res,_ch

    if len(res)>0:
        res=[[int(time.mktime(time.strptime(str(i[0]), "%Y-%m-%d %H:%M:%S"))*1000),i[1],i[2],i[3],i[4],i[5]] for i in res]
    data2 = HSD.Zbjs().vis(res)
    dc = data2.send(None)
    data2.send(None)
    _ch = [d['cd'] for d in dc]
    return res,_ch

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
        #print('if................')
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

@csrf_exempt #取消csrf验证
def getkline(rq):
    size=rq.POST.get('size')
    size=int(size) if size else 0
    #types=rq.POST.get('type') # 获取分钟类型
    if rq.is_ajax() and size>0:
        lists, _ch=getList()
        #_ch = [random.choice([0, 0, 0, 0, 0, 0]) for i in range(len(lists))]
        data={
            'des' : "注释",
            'isSuc' : True, #状态
            'datas' : {
                    'USDCNY' : 6.83, #RMB汇率
                    'contractUnit' : "BTC",
                    'data' : lists,
                    'marketName' : "凯瑞投资",
                    'moneyType' : "CNY",
                    'symbol' : 'btc38btccny',
                    'url' : '官网地址', #（选填）
                    'topTickers' : [], #（选填）
               # '_ch': _ch
            }
        }
        return HttpResponse(json.dumps(data),content_type="application/json")
    elif rq.is_ajax() and size==0:
        lists, _ch=getList()
        #_ch = [random.choice([0, 0, 0, 0, 0, 0]) for i in range(len(lists))]
        data={
            'des' : "注释",
            'isSuc' : True, #状态
            'datas' : {
                    'USDCNY' : 6.83, #RMB汇率
                    'contractUnit' : "BTC",
                    'data' : lists,
                    'marketName' : "凯瑞投资",
                    'moneyType' : "CNY",
                    'symbol' : 'carry',
                    'url' : '官网地址', #（选填）
                    'topTickers' : [], #（选填）
               # '_ch': _ch
            }
        }
        return HttpResponse(json.dumps(data),content_type="application/json")

    else:
        return redirect('index')

@accept_websocket
def getwebsocket(rq):
    zbjs=HSD.Zbjs().main()
    zs=zbjs.send(None)
    if rq.is_websocket():
        tcp=HSD.get_tcp()
        poller = zmq.Poller()
        ctx1 = Context()
        sub_socket = ctx1.socket(zmq.SUB)
        sub_socket.connect('tcp://{}:6868'.format(tcp))
        sub_socket.setsockopt_unicode(zmq.SUBSCRIBE, '')
        poller.register(sub_socket, zmq.POLLIN)
        for message in rq.websocket:
            while 1:  # 循环推送数据
                ticker=sub_socket.recv_pyobj()
                this_time=ticker.TickerTime
                objArr = cache.get("objArr")
                times,opens,high,low,close,vol=objArr if objArr else (ticker.TickerTime*1000,ticker.Price,ticker.Price,ticker.Price,ticker.Price,ticker.Qty)
                GetRealTimeData(ticker.TickerTime,ticker.Price,ticker.Qty)
                #print(times,opens,high,low,close)
                zs=0
                if time.localtime(this_time).tm_min!=time.localtime(times/1000).tm_min:
                    tm=time.localtime(times/1000)
                    tm=datetime.datetime(tm.tm_year,tm.tm_mon,tm.tm_mday,tm.tm_hour,tm.tm_min)
                    zs=zbjs.send((tm,opens,high,low,close))
                    zs=zs[tm]['datetimes'][-1][1] if zs[tm]['datetimes'] else 0
                #print('zs.............',zs)
                if this_time*1000!=times:
                    data={'times':str(times),'opens':str(opens),'high':str(high),'low':str(low),'close':str(close),'vol':str(vol),'zs':str(zs)} # ,'_ch':0
                    data=json.dumps(data).encode()
                    rq.websocket.send(data)
        zbjs.send(None)
    else:
        return redirect('index')

def zhangting(rq,t):
    dates = HSD.get_date()
    ZT = HSD.Limit_up()
    rq_date=rq.GET.get('date',dates)
    if t == 'today':
        zt=ZT.read_code()
        zt=sorted(zt,key=lambda x:x[2]) # 以第3个参数排序
        zt.reverse()
        return render(rq,'zhangting.html',{'zt_today':zt,'dates':dates})
    if not rq_date:
        return render(rq,'zhangting.html',{'jyzt':False,'dates':dates})
    if t == 'tomorrow':
        datet=datetime.datetime.strptime(rq_date, '%Y-%m-%d')
        day=datet.weekday()
        day_up=1 if 6>day>0 else (3 if day==0 else 2)
        day_down=1 if day<4 or day==6 else (3 if day==4 else 2)
        date_up = str(datet - datetime.timedelta(days=day_up))[:10]
        date_down=str(datet + datetime.timedelta(days=day_down))[:10]
        zt_tomorrow = ZT.yanzen(rq_date=rq_date)
        if zt_tomorrow:
            print(zt_tomorrow)
            for i in range(len(zt_tomorrow)):
                zt_tomorrow[i].append(zt_tomorrow[i][0])
                zt_tomorrow[i][0]=zt_tomorrow[i][0][2:]
                zt_tomorrow[i][2]=range(zt_tomorrow[i][2])#['★' for j in range(zt_tomorrow[i][2])]
        return render(rq,'zhangting.html',{'jyzt':True,'zt_tomorrow':zt_tomorrow,'dates':rq_date,'up':date_up,'down':date_down})

    return redirect('index')

def moni(rq):
    dates=rq.POST.get('dates')
    ts=rq.POST.get('ts')
    fa=rq.POST.get('fa')
    database=rq.POST.get('database','1')
    zbjs=HSD.Zbjs()
    ma=60
    if dates and ts and fa:
        try:
            res,huizong,first_time=zbjs.main2(_ma=ma, _dates=dates, _ts=int(ts),_fa=fa,database=database)
            keys=sorted(res.keys())
            keys.reverse()
            res=[dict(res[k],**{'time':k}) for k in keys]
            fa_doc=zbjs.fa_doc
            return render(rq,'moni.html',{'res':res,'keys':keys,'dates':dates,'ts':ts,'fa':fa,'fas':zbjs.xzfa,'fa_doc':fa_doc,'fa_one':fa_doc.get(fa),'huizong':huizong,'database':database,'first_time':first_time})
        except Exception as exc:
            print (exc)
    dates=datetime.datetime.now()
    day=dates.weekday()+3
    dates=str(dates-datetime.timedelta(days=day))[:10]
    ts=day+1
    return render(rq,'moni.html',{'dates':dates,'ts':ts,'fas':zbjs.xzfa,'database':database})

def bfsy(rq):
    if rq.method=='GET' and rq.is_ajax():
        msg=rq.GET.get('msg')
        if msg:
            appkey="14283e062e694e7398453680634cbcfc"
            url = "http://www.tuling123.com/openapi/api?key=%s&info=%s"%(appkey,msg)
            content = requests.get(url).text
            data = json.loads(content)
            answer = data['text']
            return HttpResponse(answer)

    return redirect('index')

def gdzd(rq):
    zbjs = HSD.Zbjs()
    gd,zd=zbjs.get_future()
    gd=gd*30 if gd>0 else 10
    zd=zd*30 if zd>0 else 10
    return render(rq,'zdzd.html',{'gd':gd,'zd':zd})

def huice(rq):
    dates = rq.GET.get('dates')
    ts = rq.GET.get('ts')
    fa = rq.GET.get('fa')
    database = rq.GET.get('database','2')
    init_money=5000
    zbjs = HSD.Zbjs()
    ma = 60
    if dates and ts and fa:
        try:
            res, huizong, first_time = zbjs.main2(_ma=ma, _dates=dates, _ts=int(ts), _fa=fa, database=database)
            keys=[i for i in res if res[i]['datetimes']]
            keys.sort()
            jyts=len(keys)
            jyys=int(keys[-1][-5:-3])-int(keys[0][-5:-3])+1
            hc = {
                'jyts':jyts,  # 交易天数
                'jyys':jyys,  # 交易月数
                'hlbfb':round(huizong['yk']/init_money*100,2), # 总获利百分比
                'dhl':round(huizong['yk']/jyts/init_money*100,2),  # 日获利百分比
                'mhl': round(huizong['yk'] / jyys/init_money*100, 2),  # 月获利百分比
                'ye': init_money+huizong['yk'], # 余额
                'cgzd':[0,0,0], # 成功的做多交易
                'cgzk':[0,0,0], # 成功的做空交易
            }
            jingzhi=[]
            zx_x=[]
            zx_y=[]
            zdhc=0
            ccsj=0  # 总持仓时间
            count_yl=0 # 总盈利
            count_ks=0 # 总亏损
            avg=huizong['yk']/huizong['zl']  # 平均盈亏
            count_var=0
            for i in keys:
                je = res[i]['mony']
                jingzhi.append(jingzhi[-1] + je if jingzhi else init_money + je)
                zx_x.append(i[-5:-3] + i[-2:])
                zx_y.append(zx_y[-1] + je if zx_y else je)
                if len(jingzhi)>1:
                    max_jz = max(jingzhi[:-1])
                    zdhc2=round((max_jz-jingzhi[-1])/max_jz*100,2)
                    zdhc=zdhc2 if zdhc2>zdhc else zdhc
                for j in res[i]['datetimes']:
                    if j[2]=='多':
                        hc['cgzd'][1]+=1
                        if j[-1]>0:
                            hc['cgzd'][0]+=1
                    elif j[2]=='空':
                        hc['cgzk'][1]+=1
                        if j[-1]>0:
                            hc['cgzk'][0]+=1
                    # 计算持仓时间
                    start_d = j[0].replace(':', '-').replace(' ', '-')
                    start_d=start_d.split('-') + [0, 0, 0]
                    start_d=[int(i) for i in start_d]
                    end_d = j[1].replace(':', '-').replace(' ', '-')
                    end_d = end_d.split('-') + [0, 0, 0]
                    end_d = [int(i) for i in end_d]
                    ccsj+=time.mktime(tuple(end_d))-time.mktime(tuple(start_d))
                    # 计算利润因子
                    if j[-1]>0:
                        count_yl+=j[-1]
                    else:
                        count_ks+=-j[-1]
                    # 方差
                    count_var+=(j[-1]-avg)**2

            hc['cgzd'][2]=round(hc['cgzd'][0]/hc['cgzd'][1]*100,2)
            hc['cgzk'][2] = round(hc['cgzk'][0] / hc['cgzk'][1] * 100, 2)
            hc['ccsj']=round(ccsj/60/huizong['zl'],1)  # 平均持仓时间
            hc['lryz']=round(count_yl/count_ks,2)
            count_var=count_var/huizong['zl']
            hc['std']=round(count_var**0.5,2)

            '''{'2018-04-27': {'duo': 1, 'kong': 2, 'mony': -89.0, 'datetimes': [['2018-04-27 11:08:00', '2018-04-27 13:00:00', '多', -1.0], 
            ['2018-04-27 13:09:00', '2018-04-27 13:38:00', '空', -100], ['2018-04-27 14:32:00', '2018-04-27 14:52:00', '空', 12.0]], 'dy': 27, 'xy': 23, 'ch': 1}'''
            hc['zjhc']=zdhc  # 最大回测
            hc['zjhc']=hc['zjhc'] if hc['zjhc']>=0 else 0
            hc['jingzhi'] = jingzhi # 每天净值
            hc['max_jz']=max(jingzhi) # 最高
            hc['zx_x']=zx_x  # 折线图x轴
            hc['zx_y'] = zx_y # 折线图y轴

            def get_jy(jg):
                jy1 = round(jg['mony'] / init_money * 100, 2)  # 获利
                jy2 = jg['mony']  # 利润
                jy3 = jg['mony']  # 点
                jy4 = jg['shenglv']  # 每天胜率
                jy5 = jg['duo'] + jg['kong']  # 开的单数
                jy6 = jy5  # 手
                return jy1, jy2, jy3, jy4, jy5, jy6
            try:
                this_date=datetime.datetime.strptime(dates,'%Y-%m-%d')
                this_date+=datetime.timedelta(days=int(ts))
                this_date=datetime.datetime.now() if this_date>datetime.datetime.now() else this_date
                week = [str(this_date - datetime.timedelta(days=i))[:10] for i in range(this_date.weekday() + 1)] # 这个星期的日期
                month = [str(this_date - datetime.timedelta(days=i))[:10] for i in range(this_date.day)]  # 这个月的日期
                time_date=time.strptime(dates, '%Y-%m-%d')
                year = [str(this_date - datetime.timedelta(days=i))[:10] for i in range(time_date.tm_yday)] # 这一年的日期
                year.sort()
                this_week=[0,0,0,0,0,0,0]
                this_month=[0,0,0,0,0,0,0]
                this_year=[0,0,0,0,0,0,0]

                for i in year:
                    jg=res.get(i)
                    if not jg or len(jg['datetimes'])<1:
                        continue
                    jys=get_jy(jg)
                    if i == str(this_date)[:10]:
                        hc['this_day']=jys
                    if i in week:
                        this_week[0] += jys[0]
                        this_week[1] += jys[1]
                        this_week[2] += jys[2]
                        this_week[3] += jys[3]
                        this_week[4] += jys[4]
                        this_week[5] += jys[5]
                        this_week[-1] += 1
                    if i in month:
                        this_month[0] += jys[0]
                        this_month[1] += jys[1]
                        this_month[2] += jys[2]
                        this_month[3] += jys[3]
                        this_month[4] += jys[4]
                        this_month[5] += jys[5]
                        this_month[-1] += 1
                    this_year[0] += jys[0]
                    this_year[1] += jys[1]
                    this_year[2] += jys[2]
                    this_year[3] += jys[3]
                    this_year[4] += jys[4]
                    this_year[5] += jys[5]
                    this_year[-1] += 1
                this_week[0]=round(this_week[0],2)
                this_week[3]=round(this_week[3]/this_week[-1],2)
                this_month[0]=round(this_month[0],2)
                this_month[3] = round(this_month[3] / this_month[-1],2)
                this_year[0]=round(this_year[0],2)
                this_year[3] = round(this_year[3] / this_year[-1],2)
            except Exception as exc:
                print(exc)
            hc['this_week']=this_week
            hc['this_month']=this_month
            hc['this_year']=this_year
            huizong['kuilv']=100-huizong['shenglv']


            """
            {'2018-04-27': {'duo': 1, 'kong': 2, 'mony': -89.0, 'datetimes': [['2018-04-27 11:08:00', '2018-04-27 13:00:00', '多', -1.0], 
            ['2018-04-27 13:09:00', '2018-04-27 13:38:00', '空', -100], ['2018-04-27 14:32:00', '2018-04-27 14:52:00', '空', 12.0]], 'dy': 27, 'xy': 23, 'ch': 1}, 
            '2018-04-28': {'duo': 0, 'kong': 0, 'mony': 0, 'datetimes': [], 'dy': 0, 'xy': 0, 'ch': 0}, 
            '2018-05-02': {'duo': 2, 'kong': 2, 'mony': -48.0, 'datetimes': [['2018-05-02 11:08:00', '2018-05-02 11:40:00', '多', -16.0], 
            ['2018-05-02 11:53:00', '2018-05-02 11:56:00', '空', 3.0], ['2018-05-02 13:55:00', '2018-05-02 15:13:00', '空', -37.0], 
            ['2018-05-02 15:23:00', '2018-05-02 15:53:00', '多', 2.0]], 'dy': 25, 'xy': 17, 'ch': 2}, 
            '2018-05-03': {'duo': 2, 'kong': 3, 'mony': 8.0, 'datetimes': [['2018-05-03 11:17:00', '2018-05-03 11:59:00', '空', 21.0], 
            ['2018-05-03 11:59:00', '2018-05-03 13:06:00', '多', 25.0], ['2018-05-03 13:20:00', '2018-05-03 13:32:00', '空', -100], 
            ['2018-05-03 14:54:00', '2018-05-03 15:11:00', '多', 21.0], ['2018-05-03 15:19:00', '2018-05-03 16:01:00', '空', 41.0]], 'dy': 21, 'xy': 25, 'ch': 4}}

            In [87]: print(huizong)
            {'yk': -129.0, 'shenglv': 58, 'zl': 12, 'least': ['2018-04-27', -89.0, 9.0, 291.0], 'most': ['2018-05-03', 8.0, -6.0, 459.0], 'avg': -10.75, 'avg_day': -32.25, 'least2': -100, 'most2': 41.0}
            """
        except Exception as exc:
            print (exc)
        return render(rq, 'hc.html', {'hc': hc, 'huizong': huizong, 'fa': fa})

    return render(rq,'hc.html')

def account_info_update(rq):
    ''' 刷新交易统计表 '''
    conn=HSD.get_conn('carry_investment')
    cur=conn.cursor()
    cur.execute("CALL account_info_update")
    conn.commit()
    conn.close()
    return redirect('tongji')

def journalism(rq):
    if rq.is_ajax:
        d=read_from_cache("journalism")
        if not is_time(d,0.25):
            url='https://www.jin10.com'
            d = request.urlopen(url).read()
            d = d.decode('utf-8')
            d = pyquery.PyQuery(d)
            d = d.find('.jin-flash_b')
            d = d.text()
            d = d.split('。 ')[:5]
            d.append(str(datetime.datetime.now())[:19])
            write_to_cache("journalism",d)
        d = {str(i): d[i] for i in range(len(d)-1)}
        return JsonResponse(d)
    return redirect('index')