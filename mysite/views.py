import pyquery
import urllib.request as request
import redis
import sys
import psutil
import json
import h5py
import numpy as np
import time, base64
import datetime
import random
import zmq
import socket
import requests

from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.conf import settings
from dwebsocket.decorators import accept_websocket, require_websocket
from django.http import HttpResponse
from django.core.cache import cache
from django.core import serializers
from django.views.decorators.csrf import csrf_exempt
from pytdx.hq import TdxHq_API
# from selenium import webdriver
# from selenium.webdriver import ActionChains
from django.http import HttpResponse
from zmq import Context
from collections import defaultdict
from hashlib import md5

from mysite import forms
from mysite import HSD
from mysite.HSD import get_ip_name
from mysite import models
from mysite import viewUtil
# from mysite.tasks import record_from_a,record_from_w
from mysite.sub_client import sub_ticker, getTickData

# * * * * * * * * * * * * * * * * * * * * * * * *  ^_^ Util function ^_^ * * * * * * * * * * * * * * * * * * * * * * * *

# 分页的一页数量
PAGE_SIZE = 28


# 从缓存读数据
def read_from_cache(user_name):
    key = 'user_id_of_' + user_name
    try:
        value = cache.get(key)
    except Exception as exc:
        viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)
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
        viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)


# 更新缓存
def redis_update(rq):
    cache.delete('user_id_of_' + 'data_house')
    cache.delete('user_id_of_' + 'stock_code')

    return render(rq, 'index.html')


def getLogin(ses, uid=False):
    """ 返回用户名与权限 """
    if 'users' in ses:
        if not uid:
            name, qx = ses['users']['name'], ses['users']['jurisdiction']
            return name, qx
        name, qx, id = ses['users']['name'], ses['users']['jurisdiction'], ses['users']['id']
        return name, qx, id
    return None, None


def record_from(rq):
    """ 访客登记 """
    if rq.is_ajax():
        return
    dt = str(datetime.datetime.now())
    ip = rq.META.get('REMOTE_ADDR')
    files = 'log\\visitor\\IP_NAME.txt'
    IP_NAME = get_ip_name(files)
    if ip not in IP_NAME:
        address = HSD.get_ip_address(ip)
        IP_NAME[ip] = address
        viewUtil.record_log(files, IP_NAME, 'w')
    info = f"{dt}----{IP_NAME[ip]}----{ip}----{rq.META.get('HTTP_HOST')}{rq.META.get('PATH_INFO')}\n"
    viewUtil.record_log('log\\visitor\\log-%s.txt' % dt[:9], info, 'a')


def get_zx_zt(zt=False, zx=False, status=None):
    """
    :param zt: 是否返回柱状图数据
    :param zx: 是否返回折线图数据
    :param status: 数据库名称，如不为None则从数据库获取折线图数据，否则从网络获取
    :return: 柱图 折线图 的数据
    """
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
        name_code = {j: i[2:] for i, j in HSD.CODE_NAME.items()}
        zt_data = {"jinJian": dt, 'times': times[10:], 'counts': counts, 'name_code': name_code}

    if zx:  # 折线图
        result = read_from_cache('history_weight' + str(status))
        if not is_time(result, 0.15):
            result = HSD.get_min_history() if status else HSD.get_history('stock_data')
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


def tongji_adus(rq):
    """ 修改与删除交易统计表 """
    user_name, qx = getLogin(rq.session)
    id = rq.POST.get('id')
    name = rq.POST.get('name')
    en = rq.POST.get('en')
    passwd = rq.POST.get('pass')
    types = rq.POST.get('types')
    if (qx == 3 or passwd == eval(HSD.get_config('U', 'ud'))) and id:
        ys = {'YES': '1', 'NO': '0', '1': '1', '0': '0'}
        en = ys.get(en.upper())
        try:
            if types == 'update' and name and en and name != 'None':
                name = name.strip()
                if en:
                    sql = "UPDATE account_info SET trader_name='{}',available={} WHERE id={}".format(name, en, id)
                    HSD.runSqlData('carry_investment', sql)
                    messages = '修改成功'
            elif types == 'delete' and id and en == '0':
                sql = "delete from account_info where id={}".format(id)
                HSD.runSqlData('carry_investment', sql)
                messages = '删除成功'
            else:
                messages = '操作失败'
        except:
            messages = '操作失败'
    else:
        messages = '验证码错误！'
    return messages


# ^v^ ^v^ ^v^ ^v^ ^v^ ^v^ ^v^  ^v^ ^v^ ^v^  Request and response ^v^ ^v^ ^v^ ^v^ ^v^ ^v^ ^v^  ^v^ ^v^ ^v^

def index(rq, logins=''):
    """ 主页面 """
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    logins = "请先登录再访问您需要的页面！" if logins is False else logins
    return render(rq, 'index.html', {'user_name': user_name, 'logins': logins})


def login(rq):
    """ 用户登录 """
    if rq.method == 'POST' and rq.is_ajax():
        username = rq.POST.get('user_name')
        password = rq.POST.get('user_password')
        message = "登录失败！请确认用户名与密码是否输入正确！"
        try:
            user = models.Users.objects.get(name=username)
            timestamp = user.creationTime
            ups = HSD.get_config('U', 'userps')
            password = eval(ups)
            password = md5(password.encode()).hexdigest()
            if user.password != password:
                pass
            elif user.password == password and user.enabled == 1:
                rq.session['users'] = {"name": user.name, "jurisdiction": user.jurisdiction, "id": user.id}
                return JsonResponse({"result": "yes", "users": username})
            elif user.enabled != 1:
                message = "登录失败！您的账户尚未启用！"
        except:
            pass
    return JsonResponse({"result": message})


def logout(rq):
    """ 登出"""
    if rq.is_ajax() and 'users' in rq.session:
        del rq.session['users']
    return HttpResponse('yes')


def update_data(rq):
    """ 修改账号资料 """
    pass


def user_information(rq):
    """ 用户资料信息 """
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    if user_name and qx <= 2 and rq.method == 'GET':
        user = models.Users.objects.get(name=user_name)
        work = models.WorkLog.objects.filter(belonged=user)
        week = ["一", "二", "三", "四", "五", "六", "日"]
        work = [[user_name, i.date, week[i.date.weekday()], i.title, i.body, i.id] for i in work]
        real_account = models.TradingAccount.objects.filter(belonged=user)
        account = models.SimulationAccount.objects.filter(belonged=user)
        return render(rq, 'user_information.html',
                      {"user_name": user_name, "work": work, "real_account": real_account, 'account': account})
    elif user_name and qx == 3 and rq.method == 'GET':
        users = models.Users.objects.all()
        users = {i.id: i.name for i in users}
        work = models.WorkLog.objects.all()
        week = ["一", "二", "三", "四", "五", "六", "日"]
        work = [[users[i.belonged_id], i.date, week[i.date.weekday()], i.title, i.body, i.id] for i in work]
        real_account = models.TradingAccount.objects.all()
        account = models.SimulationAccount.objects.all()
        return render(rq, 'user_information.html',
                      {"user_name": user_name, "work": work, "real_account": real_account, 'account': account})
    return index(rq, logins=False)


def add_work_log(rq):
    """ 添加工作日志"""
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    if user_name and rq.method == 'GET':
        date = HSD.get_date()
        return render(rq, 'user_add_data.html',
                      {"user_name": user_name, "date": date, "add_work_log": True, "operation": "工作日志"})
    elif user_name and rq.method == 'POST':
        date = rq.POST['date'].strip()
        date = date.replace('/', '-')
        title = rq.POST['title']
        body = rq.POST['body']
        user = models.Users.objects.get(name=user_name)
        if '_save' in rq.POST:  # 保存
            models.WorkLog.objects.create(belonged=user, date=date, title=title, body=body).save()
            return redirect('user_information')
        elif '_addanother' in rq.POST:  # 保存并增加另一个
            models.WorkLog.objects.create(belonged=user, date=date, title=title, body=body).save()
        elif '_continue' in rq.POST:  # 保存并继续编辑
            id = models.WorkLog.objects.order_by('id').last()
            id = id.id + 1 if id else 1
            models.WorkLog.objects.create(id=id, belonged=user, date=date, title=title, body=body).save()
            works = [date, title, body]
            return render(rq, 'user_update_data.html',
                          {"user_name": user_name, "work": works, "id": id, "update_work_log": True,
                           "operation": "工作日志"})
        return render(rq, 'user_add_data.html', {"user_name": user_name, "add_work_log": True, "operation": "工作日志"})
    return redirect('/')


def update_work_log(rq):
    """ 修改工作日志 """
    record_from(rq)
    user_name, qx, uid = getLogin(rq.session, uid=True)
    if user_name and rq.method == 'GET':
        id = rq.GET.get('id')
        work = models.WorkLog.objects.filter(id=id, belonged=uid)
        if work:
            work = [[str(w.date), w.title, w.body] for w in work][0]
            return render(rq, 'user_update_data.html',
                          {"user_name": user_name, "work": work, "id": id, "update_work_log": True,
                           "operation": "工作日志"})
    elif user_name and rq.method == 'POST':
        msg = ""
        id = rq.POST['id']
        date = rq.POST['date'].strip()
        title = rq.POST['title']
        body = rq.POST['body']
        if date and title and body:
            date = date.replace('/', '-')
            models.WorkLog.objects.filter(id=id, belonged=uid).update(date=date, title=title, body=body)
            msg += "修改成功！"
        else:
            msg += "修改失败！"
        if '_save' in rq.POST:  # 保存
            return redirect('user_information')
        elif '_addanother' in rq.POST:  # 保存并增加另一个
            return redirect('add_work_log')
        elif '_continue' in rq.POST:  # 保存并继续编辑
            work = [date, title, body]
            return render(rq, 'user_update_data.html',
                          {"user_name": user_name, "work": work, "id": id, "update_work_log": True,
                           "operation": "工作日志", "msg": msg})

    return redirect('/')


def del_work_log(rq):
    """ 删除工作日志 """
    record_from(rq)
    user_name, qx, uid = getLogin(rq.session, uid=True)
    if user_name and rq.method == 'GET':
        id = rq.GET.get('id')
        if qx == 3:
            models.WorkLog.objects.filter(id=id).delete()
        else:
            models.WorkLog.objects.filter(id=id, belonged=uid).delete()
        return redirect('user_information')
    return redirect('/')


def add_simulation_account(rq):
    """ 添加模拟账户 """
    record_from(rq)
    user_name, qx, uid = getLogin(rq.session, uid=True)
    if user_name and rq.method == 'GET':
        return render(rq, 'user_add_data.html',
                      {"user_name": user_name, "add_simulation_account": True, "operation": "模拟账户"})
    elif user_name and rq.method == 'POST':
        host = rq.POST['host']
        enabled = rq.POST['enabled']
        sql = "UPDATE account_info SET trader_name='{}',available={} WHERE id={}".format(user_name, enabled, host)
        HSD.runSqlData('carry_investment', sql)
        try:
            if '_save' in rq.POST:  # 保存
                models.SimulationAccount.objects.create(belonged_id=uid, host=host, enabled=enabled).save()
                return redirect('user_information')
            elif '_addanother' in rq.POST:  # 保存并增加另一个
                models.SimulationAccount.objects.create(belonged_id=uid, host=host, enabled=enabled).save()

        except:
            return render(rq, 'user_add_data.html',
                          {"user_name": user_name, "add_simulation_account": True, "operation": "模拟账户",
                           "msg": "添加失败！"})
    return redirect('/')

def offon_simulation_account(rq):
    """ 停用、启用模拟账户 """
    record_from(rq)
    user_name, qx, uid = getLogin(rq.session, uid=True)
    if user_name and rq.method == 'GET':
        id = rq.GET.get('id')
        enabled = rq.GET.get('enabled')
        host = rq.GET.get('host')
        models.SimulationAccount.objects.filter(id=id, belonged=uid).update(enabled=enabled)
        sql = "UPDATE account_info SET available={} WHERE id={}".format(enabled, host)
        HSD.runSqlData('carry_investment', sql)
        return redirect('user_information')
    return redirect('/')


def del_simulation_account(rq):
    """ 删除模拟账户 """
    record_from(rq)
    user_name, qx, uid = getLogin(rq.session, uid=True)
    if user_name and rq.method == 'GET':
        id = rq.GET.get('id')
        if qx == 3:
            sac = models.SimulationAccount.objects.filter(id=id).first()
            if not sac:
                return redirect('user_information')
            sql = "UPDATE account_info SET trader_name='{}',available={} WHERE id={}".format('', 0, sac.host)
            sac.delete()
            HSD.runSqlData('carry_investment', sql)
        else:
            sac = models.SimulationAccount.objects.filter(id=id, belonged=uid).first()
            if not sac:
                return redirect('user_information')
            sql = "UPDATE account_info SET trader_name='{}',available={} WHERE id={}".format('', 0, sac.host)
            sac.delete()
            HSD.runSqlData('carry_investment', sql)

        return redirect('user_information')
    return redirect('/')


def add_real_account(rq):
    """ 添加真实账户 """
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    if user_name:
        return render(rq, 'user_add_data.html',
                      {"user_name": user_name, "add_real_account": True, "operation": "真实账户"})
    return redirect('/')


def register(rq):
    """ 用户注册 """
    if rq.method == 'GET' and rq.is_ajax():
        name = rq.GET.get('name')
        names = models.Users.objects.filter(name=name)
        if names:
            datas = 1
        else:
            datas = 0
        return HttpResponse(datas)
    elif rq.method == 'GET':
        form = forms.UsersForm()
        return render(rq, 'user_register.html', {'form': form})
    elif rq.method == 'POST':
        message = ''
        form = forms.UsersForm(rq.POST)
        if form.is_valid():
            name = rq.POST['name'].strip()
            password = rq.POST['password'].strip()
            phone = rq.POST['phone'].strip()
            email = rq.POST.get('email')
            if name and password and phone and len(phone) == 11 and phone[:2] in (
                    '13', '14', '15', '18') and not models.Users.objects.filter(name=name):
                ups = HSD.get_config('U', 'userps')
                timestamp = str(int(time.time() * 10))
                password = eval(ups)
                password = md5(password.encode()).hexdigest()

                users = models.Users.objects.create(name=name, password=password, phone=phone, email=email, enabled=1,
                                                    jurisdiction=1, creationTime=timestamp)
                users.save()
                message = "注册成功！"
            else:
                message = "注册失败！请确认输入的信息是否正确！"

        return render(rq, 'user_register.html', {'form': form, 'message': message})


def page_not_found(rq):
    """ 404页面 """
    return render(rq, '404.html')


def stockData(rq):
    """ 指定股票历史数据，以K线图显示 """
    code = rq.GET.get('code')
    data = read_from_cache(code)  # 从Redis读取数据
    if not data:
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
    """ 历史股票数据分页显示 """
    conn = 'stockDate'
    conn1 = 'stock_data'
    rq_data = rq.GET.get('code')
    dinamic = rq.GET.get('dinamic')
    code_data = read_from_cache('stock_code')
    if not code_data:
        code_data = HSD.runSqlData(conn1, 'SELECT * FROM STOCK_CODE')
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
            data = HSD.runSqlData(conn1,
                                  'select date,open,high,low,close,amout,vol,code from moment_hours WHERE amout>0 AND code in (%s) limit 0,100' % str(
                                      [i for i in res_code])[1:-1])
            data = np.array(data)
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
            count = HSD.runSqlData(conn1, 'select COUNT(1) from moment_hours WHERE amout>0')
            count = count[0][0]
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
            data = HSD.runSqlData(conn1,
                                  'select date,open,high,low,close,amout,vol,code from moment_hours WHERE amout>0 limit %s,%s' % (
                                      curPage - 1, PAGE_SIZE))
            data = np.array(data)
            data[:, 0] = [i.strftime('%Y-%m-%d') for i in data[:, 0]]
            data = data.tolist()
            write_to_cache('data_house' + str(curPage), data)

        res_code = {i[-1] + i[0]: i[1] for i in code_data}

    data = [i + [res_code.get(i[7])] for i in data] if data else None
    return render(rq, 'stockDatas.html', locals())


def showPicture(rq):
    """ 获取指定代码的K线图，显示到页面上 """
    code = rq.GET.get('code')
    if code:
        d = 'http://image.sinajs.cn/newchart/daily/n/%s.gif' % code
        return render(rq, 'stock_min.html', {'picture': d})
    else:
        return redirect('stockDatas')


def is_time(data, minutes):
    """ 判断是否在指定时间内 """
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
    """ 恒生权重股柱状图，ajax请求 """
    types = rq.GET.get('types')
    if rq.method == 'GET' and rq.is_ajax():
        dt, _ = get_zx_zt(zt=True)
        if types == '2':
            wei_sum = HSD.runSqlData('stock_data', 'SELECT TIME,SUM(number) FROM weight GROUP BY TIME')
            dt1 = [{'ZX': str(i[0])[10:], 'ZY': i[1]} for i in wei_sum]
            dt['dt1'] = dt1
        return JsonResponse(dt, safe=False)


def zhutu(rq):
    """ 柱状图 """
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    return render(rq, 'zhutu.html', {'user_name': user_name})


def zhutu_zhexian(rq):
    """ 合图 """
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    return render(rq, 'zt_zx.html', {'user_name': user_name})


def zhutu_zhexian_ajax(rq):
    """ 恒生权重股合图，ajax请求 """
    status = rq.GET.get('s')
    tm = time.localtime()
    tm = tm.tm_sec > 45  # 每分钟清除折线图缓存一次
    if status and status is not '1':
        return redirect('index')
    if rq.method == 'GET' and rq.is_ajax():
        zt, zx = get_zx_zt(zt=True, zx=True, status=status)
        return JsonResponse({'zt': zt, 'zx': zx, 'tm': tm}, safe=False)


def zhexian(rq):
    """ 折线图 """
    record_from(rq)
    status = rq.GET.get('s')
    if status and status is not '1':
        return redirect('index')
    _, result = get_zx_zt(zx=True, status=status)
    user_name, qx = getLogin(rq.session)
    result['user_name'] = user_name
    return render(rq, 'zhexian.html', result)


def zhutu2(rq):
    return render(rq, 'zhutu2.html')


def tongji_update_del(rq):
    messagess = ''
    if rq.method == 'POST':
        messagess = tongji_adus(rq)
    herys = viewUtil.tongji_first()
    ids = HSD.IDS
    return render(rq, 'tongji.html', locals())


def tongji(rq):
    """ rq_type: '1':'历史查询','2':'模拟回测','3':'实时交易数据', '4':'实盘交易记录'，'5':'实盘回测' """
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    dates = HSD.get_date()
    id_name = HSD.get_idName()

    rq_date = rq.GET.get('datetimes')
    end_date = rq.GET.get('end_date')
    page = rq.GET.get('page')
    rq_date, end_date = viewUtil.tongji_ud(page, rq_date, end_date)
    rq_id = rq.GET.get('id')
    rq_type = rq.GET.get('type')
    user = rq.GET.get('user')
    when = rq.GET.get('when')
    this_d = datetime.datetime.now()
    this_day = str(this_d)[:10]
    date_d = {
        'd': this_day,  # 当天
        'w': HSD.get_date(-this_d.weekday()),  # 当周
        'm': HSD.get_date(-this_d.day + 1),  # 当月
    }
    if when in date_d:
        rq_date, end_date = date_d[when], this_day

    rq_id = '0' if rq_id == 'None' or rq_id == '' else rq_id
    dates = rq_date if rq_date and rq_date != 'None' else dates

    end_date = str(datetime.datetime.now())[:10] if not end_date else end_date  # + datetime.timedelta(days=1)
    client = rq.META.get('REMOTE_ADDR')
    if rq_type == '5' and rq_date and end_date and rq_id != '0' and user_name:  # client in HSD.get_config('IP', 'ip')
        results2, _ = HSD.sp_order_record(rq_date, end_date)
        results2 = [i for i in results2 if i[0] == rq_id]
        res = {}
        huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
                   'avg_day': 0, 'least2': 0, 'most2': 0}
        for i in results2:
            if not i[5]:
                continue
            dt = i[2][:10]
            if dt not in res:
                res[dt] = {'duo': 0, 'kong': 0, 'mony': 0, 'shenglv': 0, 'ylds': 0, 'datetimes': []}
            if i[7] == '多':
                res[dt]['duo'] += 1
            elif i[7] == '空':
                res[dt]['kong'] += 1
            res[dt]['mony'] += i[6]
            xx = [i[2], i[4], i[7], i[6], i[3], i[5], i[8]]
            res[dt]['datetimes'].append(xx)

            huizong['least'] = [dt, i[6]] if i[6] < huizong['least'][1] else huizong[
                'least']
            huizong['most'] = [dt, i[6]] if i[6] > huizong['most'][1] else huizong[
                'most']
        init_money = 10000  # 入金
        hcd = None
        if rq_date == end_date:
            hcd = HSD.huice_day(res, init_money, real=True)

        res, huizong = viewUtil.tongji_huice(res, huizong)

        hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date)

        return render(rq, 'hc.html',
                      {'hc': hc, 'huizong': huizong, 'init_money': init_money, 'hcd': hcd, 'user_name': user_name})

    if rq_type == '4' and rq_date and end_date and user_name:  # client in HSD.get_config('IP', 'ip')
        results2, huizong = HSD.sp_order_record(rq_date, end_date)
        if user:
            results2 = [i for i in results2 if i[0] == user]
        ids = HSD.IDS
        # results2: [['01-0520186-00', 'MHIN8', '2018-07-16 09:41:13', 28548.0, '2018-07-16 09:41:57', 28518.0, -30.0, '多', 1, '已平仓']...]
        # hc = HSD.huice_day(res)
        return render(rq, 'tongjisp.html', locals())

    if rq_type == '3' and user_name:  # client in HSD.get_config('IP', 'ip')
        results2 = HSD.order_detail()
        monijy = [i for i in results2 if i[8] != 2]
        status = {-1: "取消", 0: "挂单", 1: "开仓", 2: "平仓"}
        monijy = [[id_name[i[0]] if i[0] in id_name else i[0], str(i[1]), i[2], ('空' if i[6] % 2 else '多'), i[7],
                   status.get(i[8]), i[9], i[10]] for i in monijy]
        sjjy, ssjygd = HSD.sp_order_trade()
        if rq.is_ajax():
            return JsonResponse(
                {'monijy': monijy, 'sjjy': sjjy, 'ssjygd': ssjygd, 'id_name': id_name, 'user_name': user_name})
        return render(rq, 'tongji.html',
                      {'monijy': monijy, 'sjjy': sjjy, 'ssjygd': ssjygd, 'id_name': id_name, 'user_name': user_name})

    if rq_type == '2' and rq_date and end_date and rq_id != '0' and user_name:
        results2 = HSD.order_detail(rq_date, end_date)
        # (8959114, datetime.datetime(2018, 7, 4, 9, 30, 41), 28339.62, datetime.datetime(2018, 7, 4, 9, 48, 5), 28328.35, 11.27, 1, 1.0, 2, 28496.3, 28250.88)
        res = {}
        results2 = [result for result in results2 if (rq_id == str(result[0]) and result[8] != 0)]
        # [(8959325, '2018-07-16 09:16:20', 28584.32, '2018-07-16 09:30:31', 28540.18, 44.14, 1, 1.0, 2, 28623.69, 28456.86),
        huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
                   'avg_day': 0, 'least2': 0, 'most2': 0}
        for i in results2:
            dt = str(i[1])[:10]
            if dt not in res:
                res[dt] = {'duo': 0, 'kong': 0, 'mony': 0, 'shenglv': 0, 'ylds': 0, 'datetimes': []}
            if i[8] in (1, 2):
                if i[6] == 0:
                    res[dt]['duo'] += 1
                elif i[6] == 1:
                    res[dt]['kong'] += 1
                res[dt]['mony'] += i[5]
                xx = [str(i[1]), str(i[3]), '空' if i[6] % 2 else '多', i[5], i[2], i[4], i[7], i[11]]
                res[dt]['datetimes'].append(xx)

                huizong['least'] = [dt, i[5]] if i[5] < huizong['least'][1] else huizong[
                    'least']
                huizong['most'] = [dt, i[5]] if i[5] > huizong['most'][1] else huizong[
                    'most']
        hcd = {}
        sql = "SELECT origin_asset FROM account_info WHERE id={}".format(rq_id)
        init_money = HSD.runSqlData('carry_investment', sql)
        init_money = init_money[0][0] if init_money else 10000
        if rq_date == end_date:
            hcd = HSD.huice_day(res, init_money, real=False)
            # return render(rq, 'hc_day.html', {'hcd': hcd})
        res, huizong = viewUtil.tongji_huice(res, huizong)
        hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date)

        return render(rq, 'hc.html',
                      {'hc': hc, 'hcd': hcd, 'huizong': huizong, 'init_money': init_money, 'user_name': user_name})

    if rq_type == '1' and rq_date and user_name:
        result9 = HSD.order_detail(rq_date, end_date)
        huizong = {}
        results2 = []
        if result9:
            result9 = sorted(result9, key=lambda x: x[1])
            last = {}
            jc = {}
            len_result9 = len(result9)
            for i in result9:
                c = i[0]
                if rq_id != '0' and str(c) != rq_id:
                    continue
                name = id_name[c] if c in id_name else c
                dt = i[1][:10]
                if c not in huizong:
                    huizong[c] = [dt, c, 0, 0, 0, 0, 0, 0, c, 0, 0, 0, 0, 1, {}]
                    last[c] = []
                    jc[c] = []
                if dt not in huizong[c][-1]:
                    huizong[c][-1][dt] = [dt, c, 0, 0, 0, 0, 0, 0, c, 0, 0, 0, 0, 1]

                h1 = i[5]  # 盈亏
                h2 = i[7] if i[6] % 2 == 0 else 0  # 多单数量
                h3 = i[7] if i[6] % 2 == 1 else 0  # 空单数量
                h4 = 1  # 下单总数
                h5 = i[7] if i[5] > 0 else 0  # 赢利单数
                # h6 = h5 / (h4+huizong[c][5]) * 100  # 胜率
                h7 = name  # 姓名
                huizong[c][2] += h1
                huizong[c][3] += h2
                huizong[c][4] += h3
                huizong[c][5] += h4
                huizong[c][6] += h5
                # huizong[c][7] = h6
                huizong[c][8] = h7
                huizong[c][-1][dt][2] += h1
                huizong[c][-1][dt][3] += h2
                huizong[c][-1][dt][4] += h3
                huizong[c][-1][dt][5] += h4
                huizong[c][-1][dt][6] += h5
                # huizong[c][-1][dt][7] = h6
                huizong[c][-1][dt][8] = h7
                # i: (8959325, '2018-07-03 09:24:18', 28287.84, '2018-07-03 09:30:00', 28328.05, -40.21, 1, 1.0, 2, 28328.05, 28187.84)
                # 正向加仓，反向加仓
                last[c] = [las for las in last[c] if i[1] < las[3]]
                if last[c] and i[1] <= last[c][-1][3] and (last[c][-1][6] % 2 and i[6] % 2):
                    jcyk = sum(la[2] for la in last[c]) / len(last[c]) - i[2]
                    if jcyk > 0:
                        huizong[c][9] += 1
                        huizong[c][-1][dt][9] += 1
                        jc[c].append([i[1], jcyk])
                    else:
                        huizong[c][10] += 1
                        huizong[c][-1][dt][10] += 1
                        jc[c].append([i[1], jcyk])
                elif last[c] and i[1] <= last[c][-1][3] and (not last[c][-1][6] % 2 and not i[6] % 2):
                    jcyk = i[2] - sum(la[2] for la in last[c]) / len(last[c])
                    if jcyk > 0:
                        huizong[c][9] += 1
                        huizong[c][-1][dt][9] += 1
                        jc[c].append([i[1], jcyk])
                    else:
                        huizong[c][10] += 1
                        huizong[c][-1][dt][10] += 1
                        jc[c].append([i[1], jcyk])
                results2.append(i[:9] + (name,))  # 交易明细
                last[c].append(i)
                # 最大持仓
                lzd = len(last[c])
                huizong[c][13] = lzd if lzd > huizong[c][13] else huizong[c][13]
                huizong[c][-1][dt][13] = lzd if lzd > huizong[c][-1][dt][13] else huizong[c][-1][dt][13]

            for c in jc:
                jcss = []
                for dt in huizong[c][-1]:
                    jcs = [i[1] for i in jc[c] if i[0][:10] == dt]
                    jc_z = [s for s in jcs if s > 0]
                    jc_k = [s for s in jcs if s <= 0]
                    huizong[c][-1][dt][7] = huizong[c][-1][dt][6] / huizong[c][-1][dt][5] * 100  # 胜率
                    huizong[c][-1][dt][11] = sum(jc_z) / len(jc_z) if jc_z else 0  # 一天，平均每单赚多少钱加仓
                    huizong[c][-1][dt][12] = sum(jc_k) / len(jc_k) if jc_k else 0  # 一天，平均每单亏多少钱加仓
                    jcss += jcs

                jc_z = [s for s in jcss if s > 0]
                jc_k = [s for s in jcss if s <= 0]
                huizong[c][7] = huizong[c][6] / huizong[c][5] * 100  # 胜率
                huizong[c][11] = sum(jc_z) / len(jc_z) if jc_z else 0  # 总共，平均每单赚多少钱加仓
                huizong[c][12] = sum(jc_k) / len(jc_k) if jc_k else 0  # 总共，平均每单亏多少钱加仓
        if rq_id != '0':
            results2 = [result for result in results2 if rq_id == str(result[0])]

        ids = HSD.IDS
        results2 = tuple(reversed(results2))
        # results2 = [(i[0],)+(str(i[1]),)+(i[2],)+(str(i[3]),)+i[4:9]+(id_name.get(i[0],i[0]),) for i in results2]
        if results2:
            return render(rq, 'tongji.html', locals())
    if rq_type in ('3', '4', '5'):
        users = None
    if rq_date and rq_id == '0':
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
        if rq_id != '0':
            results = [result for result in results if rq_id == str(result[2])]

        ids = HSD.IDS
        if results:
            return render(rq, 'tongji.html', locals())

    # herys = None
    # try:
    #     herys = HSD.tongji()
    # except Exception as exc:
    #     HSD.logging.error("文件：views.py 第{}行报错： {}".format(sys._getframe().f_lineno, exc))
    # if not herys:
    #     return redirect('index')
    # ids = HSD.IDS
    # return render(rq, 'tongji.html', locals())
    herys = viewUtil.tongji_first()
    ids = HSD.IDS
    if not user_name:
        logins = "请先登录再访问您需要的页面！"
    return render(rq, 'tongji.html', locals())


def tools(rq):
    cljs = models.Clj.objects.all()
    user_name, qx = getLogin(rq.session)
    return render(rq, 'tools.html', {'cljs': cljs, 'user_name': user_name})


def kline(rq):
    record_from(rq)
    date = rq.GET.get('date', HSD.get_date())
    write_to_cache('kline_date', date)
    database = rq.GET.get('database', '1')
    write_to_cache('kline_database', database)
    user_name, qx = getLogin(rq.session)
    return render(rq, 'kline.html', {'date': date, 'user_name': user_name})


def getList():
    # 时间,开盘价,最高价,最低价,收盘价,成交量
    data_dict = {'1': ['carry_investment', 'wh_same_month_min'], '2': ['carry_investment', 'wh_min']}
    dates = read_from_cache('kline_date')
    database = read_from_cache('kline_database')
    if dates and database:
        # conn = HSD.get_conn(data_dict[database][0])
        if len(dates) == 10:
            dates2 = HSD.dtf(dates) + datetime.timedelta(days=1)
        else:
            dates2 = HSD.dtf(dates)
            dates = dates2 - datetime.timedelta(minutes=20)
            dates2 = dates2 + datetime.timedelta(days=1)
            dates2 = str(dates2)[:10]

        sql = 'SELECT datetime,open,high,low,close,vol FROM %s WHERE prodcode="HSI" AND datetime>="%s" AND datetime<="%s"' % (
            data_dict[database][1], dates, dates2)
        res = list(HSD.runSqlData(data_dict[database][0], sql))
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
    data2.close()
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
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    dates = HSD.get_date()
    ZT = HSD.Limit_up()
    rq_date = rq.GET.get('date', dates)
    if t == 'today':
        zt = ZT.read_code()
        zt = sorted(zt, key=lambda x: x[2])  # 以第3个参数排序
        zt.reverse()
        return render(rq, 'zhangting.html', {'zt_today': zt, 'dates': dates, 'user_name': user_name})
    if not rq_date:
        return render(rq, 'zhangting.html', {'jyzt': False, 'dates': dates, 'user_name': user_name})
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
                      {'jyzt': True, 'zt_tomorrow': zt_tomorrow, 'dates': rq_date, 'up': date_up, 'down': date_down,
                       'user_name': user_name})

    return redirect('page_not_found')


def moni(rq):
    user_name, qx = getLogin(rq.session)
    dates = rq.GET.get('dates')
    end_date = rq.GET.get('end_date')
    fa = rq.GET.get('fa')
    database = rq.GET.get('database', '1')
    reverse = rq.GET.get('reverse')
    zsds = rq.GET.get('zsds')  # 止损
    ydzs = rq.GET.get('ydzs')  # 移动止损
    zyds = rq.GET.get('zyds')  # 止盈
    cqdc = rq.GET.get('cqdc')  # 点差
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
                           'first_time': first_time, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc,
                           'user_name': user_name})
        except Exception as exc:
            viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)
    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now())[:10]  # + datetime.timedelta(days=1)
    return render(rq, 'moni.html', {'dates': dates, 'end_date': end_date, 'fas': zbjs.xzfa, 'database': database,
                                    'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc, 'user_name': user_name})


def newMoni(rq):
    user_name, qx = getLogin(rq.session)
    dates = rq.GET.get('dates')
    end_date = rq.GET.get('end_date')
    database = rq.GET.get('database', '1')
    reverse = rq.GET.get('reverse')
    zsds = rq.GET.get('zsds')  # 止损
    ydzs = rq.GET.get('ydzs')  # 移动止损
    zyds = rq.GET.get('zyds')  # 止盈
    cqdc = rq.GET.get('cqdc')  # 点差
    zsds, ydzs, zyds, cqdc = HSD.format_int(zsds, ydzs, zyds, cqdc) if zsds and ydzs and zyds and cqdc else (
        100, 80, 200, 6)
    reverse = True if reverse else False
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
                           "pkd_chonghes": pkd_chonghes, "pkd_chonghed": pkd_chonghed, 'user_name': user_name
                           })
        except Exception as exc:
            viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)
    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    return render(rq, 'new_moni.html', {'dates': dates, 'end_date': end_date, 'fas': zbjs.xzfa, 'database': database,
                                        'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc, 'user_name': user_name})


def moni_all(rq):
    record_from(rq)
    user_name, qx = getLogin(rq.session)
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
                           'end_date': end_date, 'user_name': user_name,
                           'database': database, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc})
        except Exception as exc:
            viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)
    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    return render(rq, 'moniAll.html',
                  {'dates': dates, 'end_date': end_date, 'database': database, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds,
                   'cqdc': cqdc, 'user_name': user_name})


def gdzd(rq):
    user_name, qx = getLogin(rq.session)
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
    return render(rq, 'zdzd.html', {'gd': gd, 'zd': zd, 'user_name': user_name})


def huice(rq):
    user_name, qx = getLogin(rq.session)
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
            hc, huizong = HSD.huices(res, huizong, init_money, dates, end_date)
        except Exception as exc:
            viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)
            return redirect('index')

        return render(rq, 'hc.html', {'hc': hc, 'huizong': huizong, 'user_name': user_name})

    return render(rq, 'hc.html', {'user_name': user_name})


def account_info_update(rq):
    ''' 刷新交易统计表 '''
    HSD.runSqlData('carry_investment', "CALL account_info_update")
    return redirect('tongji')


def journalism(rq):
    if rq.is_ajax():
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



def gxjy(rq):
    """国信交易"""
    record_from(rq)
    user_name, qx = getLogin(rq.session)
    folder1 = r'\\192.168.2.226\公共文件夹\gx\历史成交'
    folder2 = r'\\192.168.2.226\公共文件夹\gx\出入金'
    client = rq.META.get('REMOTE_ADDR')
    if client in HSD.get_config('IP', 'ip') or user_name:  # 内部网络或登录用户
        types = rq.GET.get('type')
        code = rq.GET.get('code')
        group = rq.GET.get('group')
        start_date = rq.GET.get('start_date')
        end_date = rq.GET.get('end_date')
        start_date = None if start_date == 'undefined' else start_date
        h = HSD.GXJY()

        if types == 'sx':  # 刷新
            try:
                viewUtil.gxjy_refresh(h, folder1, folder2)
                init_data = h.get_gxjy_sql_all()
                response = render(rq, 'gxjy.html', {'init_data': init_data, 'user_name': user_name})
            except Exception as exc:
                viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)
                response = redirect('index')
        elif not rq.is_ajax() and types == 'js':  # 计算数据
            dd = h.get_gxjy_sql(code) if code else h.get_gxjy_sql()
            data, _ = h.ray(dd, group=group) if group == 'date' else h.ray(dd)
            length = len(data)
            if code:
                hys = [data[-1]]
            else:
                hys = [data[i] for i in range(length)
                       if i < length - 1 and (data[i + 1][0][:-4] != data[i][0][:-4])  # and data[i][4]!=0
                       # or (data[i + 1][0][:-4] != data[i][0][:-4] and i == length - 1))  # and data[i][4]!=0
                       or i == length - 1]  # and data[i][4]!=0
            if group == 'date':
                data = sorted(data, key=lambda x: x[1])
            if start_date and end_date:
                data = [i for i in data if start_date <= i[1][:10] <= end_date]

            return render(rq, 'gxjy.html', {'data': data, 'hys': hys, 'start_date': start_date, 'end_date': end_date,
                                            'user_name': user_name})
        elif rq.is_ajax() and types == 'js':  # 计算数据
            start_date = rq.GET.get('start_date', '1970-01-01')
            end_date = rq.GET.get('end_date', '2100-01-01')
            dd = h.get_gxjy_sql(code) if code else h.get_gxjy_sql()
            data, _ = h.ray(dd, group=group) if group == 'date' else h.ray(dd)
            length = len(data)
            if code:
                hys = [data[-1]]
            else:
                hys = [data[i] for i in range(length)
                       if i < length - 1 and ((data[i + 1][0][:-4] != data[i][0][:-4])  # and data[i][4]!=0
                                              or (data[i + 1][0][:-4] != data[i][0][
                                                                         :-4] and i == length - 1))  # and data[i][4]!=0
                       or i == length - 1]  # and data[i][4]!=0
            if group == 'date':
                data = sorted(data, key=lambda x: x[1])
            # h.closeConn()
            data = [i for i in data if start_date <= i[1] <= end_date]
            return JsonResponse({'data': data, 'hys': hys})
        elif types == 'tjt':  # 折线图
            start_date = rq.GET.get('start_date', '1970-01-01')
            end_date = rq.GET.get('end_date', '2100-01-01')
            dd = h.get_gxjy_sql(code) if code else h.get_gxjy_sql()
            data, pzs = h.ray(dd)
            dates = h.get_dates()
            ee = h.entry_exit()
            init_money = 0
            jz = 1  # 初始净值
            jzq = 0  # init_money / jz  # 初始净值权重
            allje = 0  # init_money  # 总金额
            eae = []  # 出入金
            zx_x, prices = [], []
            hc = {
                'allyk': [],  # 累积盈亏
                'alljz': [],  # 累积净值
                'allsxf': [],  # 累积手续费
                'pie_name': [i[0] for i in pzs],  # 成交偏好（饼图） 产品名称
                'pie_value': [{'value': i[1], 'name': i[0]} for i in pzs],  # 成交偏好 饼图的值
                'bar_name': [],  # 品种盈亏 名称
                'bar_value': [],  # 品种盈亏 净利润
                'week_name': [],  # 每周盈亏 名称
                'week_value': [],  # 每周盈亏 净利润
                'month_name': [],  # 每月盈亏 名称
                'month_value': [],  # 每月盈亏 净利润
                'eae': [],  # 出入金
                'amount': [],  # 账号总金额
            }
            name_jlr = defaultdict(float)  # 品种名称，净利润
            week_jlr = defaultdict(float)  # 每周，净利润
            month_jlr = defaultdict(float)  # 每月，净利润
            data2 = []
            code_bs = h.code_bs
            for de in dates:
                zx_x.append(de[0])
                yk, sxf = 0, 0
                f_date = datetime.datetime.strptime(de[0], '%Y-%m-%d').isocalendar()[:2]
                week = str(f_date[0]) + '-' + str(f_date[1])  # 星期
                month = de[0][:7]  # 月
                for d in data:
                    if d[1][:10] == de[0]:
                        yk += d[23]
                        sxf += d[24]
                        lr = d[8] * code_bs[d[22]]
                        name_jlr[d[22]] += lr
                        week_jlr[week] += lr
                        month_jlr[month] += lr
                    else:
                        data2.append(d)
                yk += (prices[-1] if prices else 0)
                prices.append(yk)
                sxf += (hc['allsxf'][-1] if hc['allsxf'] else 0)
                hc['allsxf'].append(round(sxf, 1))
                rj = sum(i[3] - i[2] for i in ee if i[0] <= de[0])
                if rj != init_money and rj != 0:
                    jzq = (jzq * jz + rj - init_money) / jz  # 净值权重
                    init_money = rj
                amount = init_money + yk - hc['allsxf'][-1]
                hc['amount'].append(amount)
                hc['eae'].append(init_money)
                jz = amount / jzq if jzq != 0 else 0
                hc['alljz'].append(round(jz, 4))
                data = data2
                data2 = []

            zx_x2 = [i for i in zx_x if start_date <= i <= end_date]
            ind_s = zx_x.index(zx_x2[0])
            ind_e = zx_x.index(zx_x2[-1]) + 1
            zx_x = zx_x[ind_s:ind_e]
            prices = prices[ind_s:ind_e]
            f_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').isocalendar()[:2]
            week = str(f_date[0]) + '-' + str(f_date[1])  # 开始星期
            f_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').isocalendar()[:2]
            week2 = str(f_date[0]) + '-' + str(f_date[1])  # 结束星期
            month_jlr = {k: v for k, v in month_jlr.items() if start_date <= k <= end_date}
            week_jlr = {k: v for k, v in week_jlr.items() if week <= k <= week2}
            hc['allyk'] = prices
            hc['bar_name'] = [i for i in name_jlr]
            hc['bar_value'] = [round(v, 1) for v in name_jlr.values()]
            week_jlr = {k: v for k, v in week_jlr.items() if v != 0}
            hc['week_name'] = [i for i in week_jlr]
            hc['week_value'] = [round(v, 1) for v in week_jlr.values()]
            hc['month_name'] = [i for i in month_jlr]
            hc['month_value'] = [round(v, 1) for v in month_jlr.values()]
            return render(rq, 'gxjy.html', {'zx_x': zx_x, 'hc': hc, 'start_date': start_date, 'end_date': end_date,
                                            'user_name': user_name})
        elif rq.method == "GET" and rq.is_ajax():
            start_date = rq.GET.get('start_date', '1970-01-01')
            end_date = rq.GET.get('end_date', '2100-01-01')
            init_data = h.get_gxjy_sql_all(code) if code else h.get_gxjy_sql_all()
            # h.closeConn()
            init_data = [i for i in init_data if start_date <= i[0] <= end_date]
            return JsonResponse({"init_data": init_data})
        elif types == 'hc':
            # Account_ID,DATE_ADD(OpenTime,INTERVAL 8 HOUR),OpenPrice,DATE_ADD(CloseTime,INTERVAL 8 HOUR),ClosePrice,Profit,Type,Lots,Status,StopLoss,TakeProfit
            # results2 = [[8959325, datetime.datetime(2018, 7, 3, 15, 53, 20), 28373.79, datetime.datetime(1970, 1, 1, 8, 0), 28381.82, -8.03, 1, 1.0, 1, 28435.82, 28275.82],...]
            """ dd=     bs    price                time    code   cost
                    0    -1   2162.0 2016-11-22 14:12:54   j1701  26.22
                    1    -2   2925.0 2016-11-23 13:35:13  rb1701   6.10
                    2    -2   2952.0 2016-11-23 14:54:10  rb1701   6.15"""
            res = {}
            all_price = []
            huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000, 0, 0], 'most': [0, -1000, 0, 0], 'avg': 0,
                       'avg_day': 0, 'least2': 0, 'most2': 0}
            for i in results2:
                dt = str(i[1])[:10]
                if dt not in res:
                    res[dt] = {'duo': 0, 'kong': 0, 'mony': 0, 'shenglv': 0, 'ylds': 0, 'datetimes': []}
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
            # conn = HSD.get_conn('carry_investment')
            sql = "SELECT origin_asset FROM account_info WHERE id={}".format(rq_id)
            init_money = HSD.runSqlData('carry_investment', sql)
            # conn.close()
            init_money = init_money[0][0]
            hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date)

            return render(rq, 'hc.html',
                          {'hc': hc, 'huizong': huizong, 'init_money': init_money, 'user_name': user_name})
        else:  # 原始数据
            init_data = h.get_gxjy_sql_all(code) if code else h.get_gxjy_sql_all()
            if start_date and end_date:
                init_data = [i for i in init_data if start_date <= i[0][:10] <= end_date]
            else:
                start_date, end_date = init_data[0][0][:10], init_data[-1][0][:10]
            response = render(rq, 'gxjy.html', {'init_data': init_data, 'start_date': start_date, 'end_date': end_date,
                                                'user_name': user_name})
        # h.closeConn()  # 关闭数据库
    else:
        response = index(rq, logins=False)

    return response


def systems(rq):
    user_name, qx = getLogin(rq.session)
    if not user_name:
        return index(rq, False)
    return render(rq, 'systems.html', {'user_name': user_name})


def get_system(rq):
    nc = psutil.virtual_memory().percent  # 内存使用率%
    cpu = psutil.cpu_percent(0)  # cup 使用率%

    dt = str(datetime.datetime.now())[11:19]

    zx = {'nc': nc, 'cpu': cpu, 'times': dt}
    return JsonResponse({'zx': zx}, safe=False)


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
