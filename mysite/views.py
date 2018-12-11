import pyquery
import urllib.request as request
import redis
import sys
import psutil
import json
import h5py
import numpy as np
import time
import datetime
import random
import zmq
import socket
import base64
import re
import os
import math

from django.shortcuts import render, redirect, render_to_response
from django.http import JsonResponse, StreamingHttpResponse, HttpResponse, HttpResponseNotFound
from django.conf import settings
from dwebsocket.decorators import accept_websocket, require_websocket
from django.core.cache import cache
from django.core import serializers
from django.views.decorators.csrf import csrf_exempt
from pytdx.hq import TdxHq_API
# from selenium import webdriver
# from selenium.webdriver import ActionChains
from zmq import Context
from collections import defaultdict
from hashlib import md5

from mysite import forms
from mysite import HSD
from mysite.HSD import get_ip_name, logging
from mysite import models
from mysite import viewUtil
from mysite import pypass
from mysite import Wave
# from mysite import tasks
# from mysite.tasks import tasks_record_log,down_day_data_sql
from mysite.sub_client import sub_ticker, getTickData

# * * * * * * * * * * * * * * * * * * * * * * * *  ^_^ Util function ^_^ * * * * * * * * * * * * * * * * * * * * * * * *

# 分页的一页数量
PAGE_SIZE = 28


def read_from_cache(user_name):
    """ 从缓存读数据 """
    key = 'user_id_of_' + user_name
    try:
        value = cache.get(key)
        data = json.loads(value)
    except Exception as exc:
        logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))
        data = None

    return data


def write_to_cache(user_name, data, expiry_time=settings.NEVER_REDIS_TIMEOUT):
    """ 写数据到缓存 """
    key = 'user_id_of_' + user_name
    try:
        cache.set(key, json.dumps(data), expiry_time)
    except Exception as exc:
        logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))


# 更新缓存
def redis_update(rq):
    cache.delete('user_id_of_' + 'data_house')
    cache.delete('user_id_of_' + 'stock_code')

    return render(rq, 'index.html')


def is_time(data, minutes):
    """ 判断是否在指定时间内 """
    if not data:
        return False
    sj = data[-1] if isinstance(data, list) else data['times']
    if datetime.datetime.now() - datetime.datetime.strptime(sj, '%Y-%m-%d %H:%M:%S') < datetime.timedelta(
            minutes=minutes):
        return True
    else:
        return False


def getLogin(rq, uid=False):
    """ 返回用户名与权限 """
    ses = rq.session
    if 'users' in ses:
        name, qx = ses['users']['name'], ses['users']['jurisdiction']
        ses_key = read_from_cache(name)
        ses_key = ses_key.split('_') if ses_key else (None, '未知')
        frist_key = ses.session_key
        if not uid:
            if ses_key[0] and ses_key[0] != frist_key:
                response = 0, ses_key[1]
                address = record_from(rq, True)
                write_to_cache(name, f'{rq.session.session_key}_{address}')
            else:
                response = name, qx
        else:
            id = ses['users']['id']
            if ses_key[0] and ses_key[0] != frist_key:
                response = 0, ses_key[1], 0
                address = record_from(rq, True)
                write_to_cache(name, f'{rq.session.session_key}_{address}')
            else:
                response = name, qx, id
    else:
        response = (None, 0, None) if uid else (None, 0)
    return response


def record_from(rq, login=False):
    """ 访客登记 """
    if not login and rq.is_ajax():
        return
    dt = str(datetime.datetime.now())
    ip = rq.META.get('REMOTE_ADDR')
    files = 'log\\visitor\\IP_NAME.txt'
    IP_NAME = get_ip_name(files)
    if ip not in IP_NAME:
        address = HSD.get_ip_address(ip)
        IP_NAME[ip] = address
        viewUtil.record_log(files, IP_NAME, 'w')
        # tasks_record_log.delay(files, IP_NAME, 'w')
    if login:
        return IP_NAME[ip]
    info = f"{IP_NAME[ip]}----{ip}----{rq.META.get('HTTP_HOST')}{rq.META.get('PATH_INFO')}\n"
    viewUtil.record_log('log\\visitor\\log-%s.txt' % dt[:9], info, 'a')
    # tasks_record_log.delay('log\\visitor\\log-%s.txt' % dt[:9], info, 'a')


def LogIn(rq, uid=False):
    """ 访客登记 与 返回用户名与权限 """
    record_from(rq)
    return getLogin(rq, uid)


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
    user_name, qx = getLogin(rq)
    id = rq.POST.get('id')
    name = rq.POST.get('name')
    en = rq.POST.get('en')
    passwd = rq.POST.get('pass')
    types = rq.POST.get('types')
    dates = HSD.get_date()
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


def get_cfmmc_id_host(_id='Nones', select=False):
    """ 从Redis获取id_host字典，如果为空或者select为True，则从数据库查询 """
    id_host = read_from_cache('cfmmc_id_host')
    if not id_host or select:
        id_host = viewUtil.cfmmc_id_hostName()
        write_to_cache('cfmmc_id_host', id_host) if id_host else 0
    if _id is 'Nones':
        return id_host
    return id_host.get(_id)


# ^v^ ^v^ ^v^ ^v^ ^v^ ^v^ ^v^  ^v^ ^v^ ^v^  Request and response ^v^ ^v^ ^v^ ^v^ ^v^ ^v^ ^v^  ^v^ ^v^ ^v^


def index(rq, logins='', user_name=None, qx=''):
    """ 主页面 """
    if user_name is None:
        user_name, qx = LogIn(rq)
    if user_name == 0:
        logins = "欢迎登陆！"  # f"您上次登录地点是：【{qx}】"
        user_name = rq.session['users'].get('name')
        # del rq.session['users']
    else:
        logins = "请先登录再访问您需要的页面！" if logins is False else logins

    return render(rq, 'index.html', {'user_name': user_name, 'logins': logins})


def login(rq):
    """ 用户登录 """
    if rq.method == 'POST' and rq.is_ajax():
        address = record_from(rq, True)
        username = rq.POST.get('user_name')
        password = rq.POST.get('user_password')
        message = "登录失败！请确认用户名与密码是否输入正确！"
        iphone, qq = '', ''
        with viewUtil.errors('views', 'login'):
            user = models.Users.objects.get(name=username)
            timestamp = user.creationTime
            ups = HSD.get_config('U', 'userps')
            password = eval(ups)
            password = md5(password.encode()).hexdigest()
            if user.password != password:
                pass
            elif user.password == password and user.enabled == 1:
                rq.session['users'] = {"name": user.name, "jurisdiction": user.jurisdiction, "id": user.id}
                write_to_cache(user.name, f'{rq.session.session_key}_{address}')
                return JsonResponse({"result": "yes", "users": username})
            elif user.enabled != 1:
                message = "您的账户尚未启用！请点击《联系管理员》"
                iphone = HSD.get_config('U', 'contact_phone')
                qq = HSD.get_config('U', 'contact_qq')

        return JsonResponse({"result": message, "iphone": iphone, "qq": qq})
    return redirect('/')


def logout(rq):
    """ 登出"""
    if rq.is_ajax() and 'users' in rq.session:
        del rq.session['users']
    return HttpResponse('yes')


def update_data(rq):
    """ 修改账号资料 """
    pass


def user_update_info(rq):
    """ 修改用户信息 """
    user_name, qx = LogIn(rq)
    if user_name and rq.method == 'GET':
        phone, email = '', ''
        with viewUtil.errors('views', 'user_update_password'):
            user = models.Users.objects.get(name=user_name)
            phone, email = user.phone, user.email
        return render(rq, 'user_update_data.html',
                      {"user_name": user_name, "user_update_info": True, "operation": "个人信息", "phone": phone,
                       "email": email})
    elif user_name and rq.method == 'POST':
        password0 = rq.POST.get('password0').strip()
        password1 = rq.POST.get('password1').strip()
        password2 = rq.POST.get('password2').strip()
        phone = rq.POST.get('phone').strip()
        email = rq.POST.get('email').strip()
        user = None
        with viewUtil.errors('views', 'user_update_password'):
            user = models.Users.objects.get(name=user_name)
        if (password1 == password2 != password0) and user:
            password = password0
            ups = HSD.get_config('U', 'userps')
            timestamp = user.creationTime
            password = eval(ups)
            password0 = md5(password.encode()).hexdigest()
            if user.password == password0:
                password = password1
                timestamp = str(int(time.time() * 10))
                password = eval(ups)
                password = md5(password.encode()).hexdigest()
                phone = phone or user.phone
                email = email or user.email
                models.Users.objects.filter(name=user_name).update(password=password, creationTime=timestamp,
                                                                   phone=phone, email=email)
                del rq.session['users']
                msg = "修改成功！请重新登录！"
            else:
                msg = "密码错误！"
        else:
            msg = "修改失败！"
        return render(rq, 'user_update_data.html',
                      {"user_name": user_name, "user_update_info": True, "operation": "个人信息", "msg": msg,
                       "phone": phone, "email": email})


def user_information(rq):
    """ 用户资料信息 """
    user_name, qx = LogIn(rq)
    if user_name and qx <= 2 and rq.method == 'GET':
        user = models.Users.objects.get(name=user_name)
        week = ["一", "二", "三", "四", "五", "六", "日"]
        work, allPage, curPage = viewUtil.user_work_log(rq, models.WorkLog, user)
        work = [[user_name, i.date, week[i.date.weekday()], i.title, i.body, i.id] for i in work]
        real_account = models.TradingAccount.objects.filter(belonged=user)
        account = models.SimulationAccount.objects.filter(belonged=user)
        return render(rq, 'user_information.html',
                      {"user_name": user_name, "work": work, "real_account": real_account, 'account': account,
                       'allPage': allPage, 'curPage': curPage, 'qx': qx})
    elif user_name and qx == 3 and rq.method == 'GET':
        users = models.Users.objects.all()
        users = {i.id: i.name for i in users}
        work, allPage, curPage = viewUtil.user_work_log(rq, models.WorkLog)
        week = ["一", "二", "三", "四", "五", "六", "日"]
        work = [[users[i.belonged_id], i.date, week[i.date.weekday()], i.title, i.body, i.id] for i in work]
        real_account = models.TradingAccount.objects.all()
        account = models.SimulationAccount.objects.all()
        return render(rq, 'user_information.html',
                      {"user_name": user_name, "work": work, "real_account": real_account, 'account': account,
                       'allPage': allPage, 'curPage': curPage, 'qx': qx})

    return index(rq, False, user_name, qx)


def add_work_log(rq):
    """ 添加工作日志"""
    user_name, qx = LogIn(rq)
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
        body = body.replace('\r\n', '<br>')
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
    user_name, qx, uid = LogIn(rq, uid=True)
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
        body = body.replace('\r\n', '<br>')
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
    user_name, qx, uid = LogIn(rq, uid=True)
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
    user_name, qx, uid = LogIn(rq, uid=True)
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
    user_name, qx, uid = LogIn(rq, uid=True)
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
    user_name, qx, uid = LogIn(rq, uid=True)
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
    user_name, qx = LogIn(rq)
    if user_name:
        return render(rq, 'user_add_data.html',
                      {"user_name": user_name, "add_real_account": True, "operation": "真实账户"})
    return redirect('/')


def del_real_account(rq):
    """ 删除真实账户 """
    user_name, qx, uid = LogIn(rq, uid=True)
    if user_name and rq.method == 'GET':
        tid = rq.GET.get('id')
        models.TradingAccount.objects.filter(belonged_id=uid, id=tid).delete()
    return redirect('user_information')


def user_info_public_show(rq):
    """ 显示公共信息 """
    user_name, qx = LogIn(rq)
    if user_name and qx >= 2 and rq.method == 'GET':
        users = models.Users.objects.all()
        users = {i.id: i.name for i in users}
        infopublic, allPage, curPage = viewUtil.user_work_log(rq, models.InfoPublic)
        infobody = models.InfoBody.objects.all()
        infopublic = [[i.title, str(i.startDate)[:10], i.id,
                       [[users[j.belonged_id], j.body, str(j.startDate + datetime.timedelta(hours=8))[:16], j.id] for j
                        in infobody if j.belongedTitle_id == i.id]] for i in infopublic]
        return render(rq, 'user_info_public.html',
                      {"user_name": user_name, "infopublic": infopublic,
                       'allPage': allPage, 'curPage': curPage, 'qx': qx})

    return index(rq, False, user_name, qx)


def user_info_public(rq):
    """ 公共信息 """
    user_name, qx = LogIn(rq)
    if user_name and qx >= 2 and rq.method == 'GET':
        return render(rq, 'user_add_data.html', {"user_name": user_name, "add_info_public": True,
                                                 "operation": "公共信息"})
    elif user_name and qx >= 2 and rq.method == 'POST':
        title = rq.POST['title']
        body = rq.POST['body']
        user = models.Users.objects.get(name=user_name)
        body = body.replace('\r\n', '<br>')
        if '_save' in rq.POST:  # 保存
            infopublic_save = models.InfoPublic.objects.create(belonged=user, title=title)
            infopublic_save.save()
            models.InfoBody.objects.create(belonged=user, belongedTitle_id=infopublic_save.id, body=body).save()
            return redirect('user_info_public_show')
        elif '_addanother' in rq.POST:  # 保存并增加另一个
            infopublic_save = models.InfoPublic.objects.create(belonged=user, title=title)
            infopublic_save.save()
            models.InfoBody.objects.create(belonged=user, belongedTitle_id=infopublic_save.id, body=body).save()
        elif '_continue' in rq.POST:  # 保存并继续编辑
            id = models.InfoPublic.objects.order_by('id').last()
            id = id.id + 1 if id else 1
            infopublic_save = models.InfoPublic.objects.create(id=id, belonged=user, title=title)
            infopublic_save.save()
            models.InfoBody.objects.create(belonged=user, belongedTitle_id=infopublic_save.id, body=body).save()
            infopublic = [title, body]
            return render(rq, 'user_update_data.html',
                          {"user_name": user_name, "infopublic": infopublic, "id": id, "add_info_public": True,
                           "operation": "公共信息"})
        return render(rq, 'user_add_data.html', {"user_name": user_name, "add_info_public": True,
                                                 "operation": "公共信息"})
    return redirect('/')


def user_info_public_reply(rq):
    """ 添加公共消息回复 """
    user_name, qx, uid = LogIn(rq, uid=True)
    if user_name and qx >= 2 and rq.method == 'GET':
        info_id = rq.GET.get('id')
        if info_id:
            return render(rq, 'user_add_data.html',
                          {"user_name": user_name, "add_info_body": True, "operation": "回复", 'id': info_id})
    elif user_name and qx >= 2 and rq.method == 'POST':
        id = rq.POST.get('id')
        body = rq.POST.get('body')
        if id and body:
            infobody = models.InfoBody.objects.create(belonged_id=uid, belongedTitle_id=id, body=body)
            infobody.save()

        return redirect('user_info_public_show')
    return redirect('/')


def user_info_public_replyDel(rq):
    """ 删除公共消息 回复 """
    user_name, qx, uid = LogIn(rq, uid=True)
    if user_name and rq.method == 'GET':
        id = rq.GET.get('id')
        if qx >= 2:
            models.InfoBody.objects.filter(id=id).delete()
        return redirect('user_info_public_show')
    return redirect('/')


def user_info_public_update(rq):
    """ 修改公共消息 """
    # update_info_public
    user_name, qx, uid = LogIn(rq, uid=True)
    if user_name and qx >= 2 and rq.method == 'GET':
        id = rq.GET.get('id')
        infopublic = models.InfoPublic.objects.filter(id=id, belonged=uid)
        if infopublic:
            infopublic = [w.title for w in infopublic][0]
            return render(rq, 'user_update_data.html',
                          {"user_name": user_name, "infopublic": infopublic, "id": id, "update_info_public": True,
                           "operation": "公共信息"})
    elif qx >= 2 and rq.method == 'POST':
        msg = ""
        id = rq.POST['id']
        title = rq.POST['title']
        # body = rq.POST['body']
        # body = body.replace('\r\n','<br>')
        infoupdate = models.InfoPublic.objects.filter(id=id, belonged=uid)
        if title and infoupdate:
            infoupdate.update(title=title)
            msg += "修改成功！"
        else:
            msg += "修改失败！"
        if '_save' in rq.POST:  # 保存
            return redirect('user_info_public_show')
        elif '_addanother' in rq.POST:  # 保存并增加另一个
            return redirect('user_info_public')
        elif '_continue' in rq.POST:  # 保存并继续编辑
            infopublic = title
            return render(rq, 'user_update_data.html',
                          {"user_name": user_name, "infopublic": infopublic, "id": id, "update_info_public": True,
                           "operation": "公共信息", "msg": msg})

    return redirect('user_info_public_show')


def user_info_public_delete(rq):
    """ 删除公共消息 """
    user_name, qx, uid = LogIn(rq, uid=True)
    if user_name and rq.method == 'GET':
        id = rq.GET.get('id')
        if qx == 3:
            # MonthScheme.objects.extra(where=['id IN ('+ idstring +')']).delete()
            models.InfoBody.objects.filter(belongedTitle_id=id).delete()
            models.InfoPublic.objects.filter(id=id).delete()
        else:
            models.InfoBody.objects.filter(belonged_id=uid, belongedTitle_id=id).delete()
            models.InfoPublic.objects.filter(id=id, belonged=uid).delete()
        return redirect('user_info_public_show')
    return redirect('/')


def user_cloud_public(rq):
    """ 公共云 上传 """
    user_name, qx = LogIn(rq)
    if user_name and qx >= 2:
        path_root = HSD.get_external_folder('cloud')  # 保存文件的目录
        if rq.method == 'POST':
            upload_file = rq.FILES.get('file')  # 获得文件
            if upload_file:
                r_file_name = re.findall(r'\w+\.[A-z]{1,4}', upload_file.name)
                if r_file_name:
                    r_file_name = r_file_name[-1]
                else:
                    r_file_name = str(int(time.time() * 1000))[-10:]
                file_name = user_name + "_+_" + r_file_name
                folder_size = viewUtil.get_dirsize(path_root, int(time.time() / 10)) / 1024 / 1024
                file_size = upload_file.size / 1024 / 1024
                path_file = os.path.join(path_root, file_name)
                if folder_size + file_size > 10240:  # 限制整个目录最大装10G
                    msg = f" {r_file_name} 文件太大！"
                elif os.path.isfile(path_file):
                    msg = f" {r_file_name} 已经存在！"
                else:
                    with open(path_file, 'wb+') as f:
                        for chunk in upload_file.chunks(2048):
                            f.write(chunk)
                    msg = f" {r_file_name} 上传成功!"
            else:
                msg = "请选择需要上传的文件！"
            clouds = viewUtil.get_cloud_file(path_root)
            return render(rq, 'user_cloud_public.html',
                          {'qx': qx, 'msg': msg, 'clouds': clouds, 'user_name': user_name})
        else:
            clouds = viewUtil.get_cloud_file(path_root)
            return render(rq, 'user_cloud_public.html', {'qx': qx, 'clouds': clouds, 'user_name': user_name})
    return redirect('index')


def user_cloud_public_download(rq):
    """ 公共云 下载 """
    user_name, qx = LogIn(rq)

    if user_name and qx >= 2 and rq.method == 'GET':
        path_root = HSD.get_external_folder('cloud')  # 保存文件的目录
        name = rq.GET.get('name')
        file_name = rq.GET.get('filename')
        red = HSD.RedisPool()
        red_key = f"is_download{rq.session.session_key}{file_name}"
        is_downs = red.get(red_key)
        _d = user_name + str(datetime.datetime.now())[:18]
        path_file = os.path.join(path_root, name + '_+_' + file_name)

        if os.path.isfile(path_file):
            if is_downs != _d and is_downs != 'read':
                resp = StreamingHttpResponse(viewUtil.file_iterator(path_file, red=red,
                                                                    red_key=red_key))  # viewUtil.FileWrapper(open(path_file,'rb'))
                red.set(red_key, _d, expiry=6000)
                resp['Content-Type'] = 'application/octet-stream'
                resp['Content-Disposition'] = 'attachment;filename="{}"'.format(file_name)  # 此处file_name是要下载的文件的文件名称
                return resp
        else:
            msg = "文件不存在！"
            clouds = viewUtil.get_cloud_file(path_root)
            return render(rq, 'user_cloud_public.html',
                          {'qx': qx, 'msg': msg, 'clouds': clouds, 'user_name': user_name})

    return redirect('index')


@csrf_exempt
def fileupload(rq):
    """ 公共云 上传文件分片"""
    if rq.method == 'POST':
        user_name, qx = LogIn(rq)
        upload_file = rq.FILES.get('file')
        task = rq.POST.get('task_id')  # 获取文件唯一标识符
        chunk = rq.POST.get('chunk', 0)  # 获取该分片在所有分片中的序号
        # red = HSD.RedisPool()
        # _chunk = red.get('fileupload_chunk')
        filename = '%s%s' % (task, chunk)  # 构成该分片唯一标识符
        path_root = HSD.get_external_folder('cloud')  # 保存文件的目录
        if upload_file:
            r_file_name = upload_file.name
            path_file = path_root + '/%s%s' % (user_name + "_+_", upload_file)
            folder_size = viewUtil.get_dirsize(path_root, task) / 1024 / 1024
            file_size = upload_file.size / 1024 / 1024
            if folder_size + file_size > 10240:  # 限制整个目录最大装10G
                msg = 'big'  # f" {r_file_name} 文件太大！"
            elif os.path.isfile(path_file):
                msg = 'existed'  # f" {r_file_name} 已经存在！"
            else:
                # 保存分片到本地
                with open(f'{path_root}/{filename}', 'wb') as f:
                    for chunk in upload_file.chunks():
                        f.write(chunk)
                msg = 'success'  # f" {r_file_name} 上传成功!"
        else:
            msg = 'unselected'  # "请选择需要上传的文件！"
        # print(msg)
        if msg != 'success':
            return HttpResponseNotFound('No')

    return HttpResponse()


@csrf_exempt
def fileMerge(rq):
    """ 公共云 组合分片"""
    user_name, qx = LogIn(rq)
    path_root = HSD.get_external_folder('cloud')  # 保存文件的目录
    task = rq.GET.get('task_id')
    file_name = rq.GET.get('filename', '')
    file_name = file_name.replace(' ', '').replace('\t', '').replace('\n', '')
    upload_type = rq.GET.get('type')
    if len(file_name) == 0 and upload_type:
        file_name = upload_type.split('/')[1]
    chunk = 0
    if os.path.isfile(path_root + '/%s%d' % (task, chunk)):
        with open(path_root + '/%s%s' % (user_name + "_+_", file_name), 'wb') as target_file:  # 创建新文件
            while True:
                try:
                    filename = path_root + '/%s%d' % (task, chunk)
                    with open(filename, 'rb') as source_file:  # 按序打开每个分片
                        target_file.write(source_file.read())  # 读取分片内容写入新文件
                except IOError:
                    break
                chunk += 1
                os.remove(filename)  # 删除该分片，节约空间
    return HttpResponse()


def user_cloud_public_delete(rq):
    """ 公共云 删除 """
    user_name, qx = LogIn(rq)
    if user_name and qx >= 2 and rq.method == 'POST':
        path_root = HSD.get_external_folder('cloud')  # 保存文件的目录
        name = rq.POST.get('name')
        file_name = rq.POST.get('filename')
        path_file = os.path.join(path_root, name + '_+_' + file_name)
        if name != user_name and qx <= 2:
            msg = f" {file_name} 非本人上传！"
        elif os.path.isfile(path_file):
            os.remove(path_file)
            msg = f" {file_name} 删除成功！"
        else:
            msg = f" {file_name} 文件不存在！"
        clouds = viewUtil.get_cloud_file(path_root)
        return render(rq, 'user_cloud_public.html', {'qx': qx, 'msg': msg, 'clouds': clouds, 'user_name': user_name})
    return redirect('index')


def user_cloud_public_show(rq):
    """ 公共云 文件显示 """
    user_name, qx = LogIn(rq)
    if user_name and qx >= 2 and rq.method == 'POST':
        path_root = HSD.get_external_folder('cloud')  # 保存文件的目录
        name = rq.POST.get('name')
        file_name = rq.POST.get('filename')
        path_file = os.path.join(path_root, name + '_+_' + file_name)
        import chardet
        if os.path.isfile(path_file):
            if os.path.getsize(path_file) < 1024 * 1024:  # 最大显示1MB
                with open(path_file, 'rb') as f:
                    file_body = f.read()
            else:
                with open(path_file, 'rb') as f:
                    file_body = f.read(1000)
            try:
                file_body = file_body.decode(chardet.detect(file_body)['encoding'])
            except:
                file_body = str(file_body)

            msg = f" {file_name} 内容！"
        else:
            msg = f" {file_name} 文件不存在！"
        clouds = viewUtil.get_cloud_file(path_root)
        return render(rq, 'user_cloud_public.html', {'qx': qx, 'msg': msg, 'clouds': clouds, 'user_name': user_name,
                                                     'file_body': file_body})
    return redirect('index')


def user_cloud_public_runcode(rq):
    """ 公共云 代码执行 """
    user_name, qx = LogIn(rq)
    if rq.method == 'POST' and rq.is_ajax():
        name = rq.POST.get('user_name')
        codes = rq.POST.get('codes')
        result = ''
        if user_name == name and qx >= 2:
            try:
                folds = 'mysite\\log'
                files = 'RunPy.py'
                os.chdir(folds)
                with open(files, 'w', encoding='utf-8') as f:
                    f.write(codes)
                result = str(os.popen(f'python {files}').read())
                os.chdir('../../')
            except Exception as exc:
                result = str(exc)
        result = result.replace('\n', '<br>').replace(r'D:\tools\Tools\Carry', 'My folder')
        return JsonResponse({'result': result.replace('\n', '<br>')})
    return JsonResponse({'result': 'no'})


def register(rq):
    """ 用户注册 """
    record_from(rq)
    if rq.method == 'GET' and rq.is_ajax():
        name = rq.GET.get('name')
        names = models.Users.objects.filter(name=name).exists()
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
            if name and password and phone and 1 < len(name) < 10 and len(phone) == 11 and phone[:2] in (
                    '13', '14', '15', '18') and not models.Users.objects.filter(name=name):
                ups = HSD.get_config('U', 'userps')
                timestamp = str(int(time.time() * 10))
                password = eval(ups)
                password = md5(password.encode()).hexdigest()

                users = models.Users.objects.create(name=name, password=password, phone=phone, email=email, enabled=0,
                                                    jurisdiction=1, creationTime=timestamp)
                users.save()
                message = "注册成功！"
            else:
                message = "注册失败！请确认输入的信息是否正确！"

        return render(rq, 'user_register.html', {'form': form, 'message': message})


def page_not_found(rq):
    """ 404页面 """
    record_from(rq)
    return render(rq, '404.html')


def stockData(rq):
    """ 指定股票历史数据，以K线图显示 """
    user_name, qx = LogIn(rq)
    code = rq.GET.get('code')
    data = read_from_cache(code)  # 从Redis读取数据
    if not data:
        if socket.gethostname() != 'doc':
            h5 = h5py.File(r'E:\work_File\黄海军\资料\Carry\mysite\stock_data.hdf5',
                           'r')  # r'D:\tools\Tools\stock_data.hdf5'
        else:
            h5 = h5py.File(r'D:\tools\Tools\stock_data.hdf5')
        data1 = h5['stock/' + code + '.day'][:].tolist()
        data = []
        for i in range(len(data1)):
            d = str(data1[i][0])
            data.append(
                [d[:4] + '/' + d[4:6] + '/' + d[6:8]] + [data1[i][1]] + [data1[i][4]] + [data1[i][3]] + [data1[i][2]])

        write_to_cache(code, data)  # 写入数据到Redis

    return render(rq, 'stockData.html', {'data': json.dumps(data), 'code': code, 'user_name': user_name})


def stockDatas(rq):
    """ 历史股票数据分页显示 """
    user_name, qx = LogIn(rq)
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
        with viewUtil.errors('views', 'stockDatas'):
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

    elif rq_data:
        rq_data = rq_data.upper()
        isPage = False
        res_data = [i for i in code_data if
                    rq_data in i[0] or rq_data in i[1] or rq_data in i[2] or (
                        rq_data in i[3] if i[3] else None) or rq_data in i[4]]
        try:
            res_code = {i[-1] + i[0]: i[1] for i in res_data}
            sql = (
                    'select date,open,high,low,close,amout,vol,code from '
                    'moment_hours WHERE amout>0 AND code in (%s) limit 0,100' % str([i for i in res_code])[1:-1]
            )
            data = HSD.runSqlData(conn1, sql)
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
            sql = (
                    'select date,open,high,low,close,amout,vol,code from '
                    'moment_hours WHERE amout>0 limit %s,%s' % (curPage - 1, PAGE_SIZE)
            )
            data = HSD.runSqlData(conn1, sql)
            data = np.array(data)
            data[:, 0] = [i.strftime('%Y-%m-%d') for i in data[:, 0]]
            data = data.tolist()
            write_to_cache('data_house' + str(curPage), data)

        res_code = {i[-1] + i[0]: i[1] for i in code_data}

    data = [i + [res_code.get(i[7])] for i in data] if data else None
    return render(rq, 'stockDatas.html', locals())


def showPicture(rq):
    """ 获取指定代码的K线图，显示到页面上 """
    user_name, qx = LogIn(rq)
    code = rq.GET.get('code')
    if code:
        d = 'http://image.sinajs.cn/newchart/daily/n/%s.gif' % code
        return render(rq, 'stock_min.html', {'picture': d, 'user_name': user_name})
    else:
        return redirect('stockDatas')


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
    user_name, qx = LogIn(rq)
    return render(rq, 'zhutu.html', {'user_name': user_name})


def zhutu_zhexian(rq):
    """ 合图 """
    user_name, qx = LogIn(rq)
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
    user_name, qx = LogIn(rq)
    status = rq.GET.get('s')
    if status and status is not '1':
        return redirect('index')
    _, result = get_zx_zt(zx=True, status=status)
    result['user_name'] = user_name
    return render(rq, 'zhexian.html', result)


def zhutu2(rq):
    return render(rq, 'zhutu2.html')


def tongji_update_del(rq):
    user_name, qx = LogIn(rq)
    messagess = ''
    if rq.method == 'POST':
        messagess = tongji_adus(rq)
    # herys = viewUtil.tongji_first()
    with viewUtil.errors():
        herys = HSD.tongji()
    ids = HSD.IDS
    return render(rq, 'tongji.html', locals())


def tongji(rq):
    """ rq_type: '1':'历史查询','2':'模拟回测','3':'实时交易数据', '4':'实盘交易记录'，'5':'实盘回测' """
    user_name, qx = LogIn(rq)
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

    if rq_type == '5' and rq_date and end_date and rq_id != '0' and user_name:
        results2, _ = HSD.sp_order_record(rq_date, end_date)
        results2 = [i for i in results2 if i[0] == rq_id]
        res = {}
        huizong = {'yk': 0, 'shenglv': 0, 'zl': 0, 'least': [0, 1000], 'most': [0, -1000], 'avg': 0,
                   'avg_day': 0, 'least2': [0, 1000], 'most2': [0, -1000]}
        for i in results2:
            if not i[5]:
                continue
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
            xx = [i[2], i[4], i[7], i[6], i[3], i[5], i[8]]
            res[dt]['datetimes'].append(xx)

            huizong['least'] = [dt, i[6]] if i[6] < huizong['least'][1] else huizong['least']
            huizong['least2'] = [dt, _ykds] if _ykds < huizong['least2'][1] else huizong['least2']
            huizong['most'] = [dt, i[6]] if i[6] > huizong['most'][1] else huizong['most']
            huizong['most2'] = [dt, _ykds] if _ykds > huizong['most2'][1] else huizong['most2']
        init_money = 10000  # 入金

        if rq_date == end_date:
            hcd = HSD.huice_day(res, init_money, real=True)
            max_yl = max(hcd['day_yk'])
            max_ks = min(hcd['day_yk'])
        else:
            hcd, max_yl, max_ks = None, None, None

        res, huizong = viewUtil.tongji_huice(res, huizong)

        hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date)
        hc_name = rq_id
        huizong['max_yl'], huizong['max_ks'] = max_yl, max_ks
        return render(rq, 'hc.html',
                      {'hc': hc, 'huizong': huizong, 'init_money': init_money, 'hcd': hcd, 'user_name': user_name,
                       'hc_name': hc_name})

    if rq_type == '4' and rq_date and end_date and user_name:
        result9, huizong2 = HSD.sp_order_record(rq_date, end_date)

        huizong = {}
        results2 = []
        if result9:
            result9 = sorted(result9, key=lambda x: x[2])
            last = {}
            jc = {}
            len_result9 = len(result9)
            for i in result9:
                c = i[0]
                if (rq_id != '0' and str(c) != rq_id) or i[4] is None:
                    continue
                name = ''  # id_name[c] if c in id_name else c # 名称
                dt = i[2][:10]
                if c not in huizong:
                    huizong[c] = [dt, c, 0, 0, 0, 0, 0, 0, c, 0, 0, 0, 0, 1, i[1], {}]
                    last[c] = []
                    jc[c] = []
                if dt not in huizong[c][-1]:
                    huizong[c][-1][dt] = [dt, c, 0, 0, 0, 0, 0, 0, c, 0, 0, 0, 0, 1, i[1]]
                #  ['01-0202975-00', 'HSIZ8', '2018-12-03 09:49:29', 27078.0, '2018-12-03 13:18:40', 27118.0, -40.0, '空', 1, '已平仓']
                h1 = i[6]  # 盈亏
                if i[7] == '空':
                    h2, h3 = 0, i[8]  # 多单、空单数量
                else:
                    h2, h3 = i[8], 0  # 多单、空单数量
                # i (8965391, '2018-08-17 09:58:21', 27215.18, '2018-08-17 10:15:59', 27264.75, -49.57, 1, 1.0, 2, 27264.75, 27114.75, 53680779, 'HSENG$.AUG8')
                h4 = i[8]  # 下单总数
                h5 = i[8] if i[6] > 0 else 0  # 赢利单数
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
                # 正向加仓，反向加仓
                last[c] = [las for las in last[c] if i[2] < las[4]]
                if last[c] and i[2] <= last[c][-1][4] and (last[c][-1][7] == '空' and i[7] == '空'):
                    jcyk = sum(la[3] for la in last[c]) / len(last[c]) - i[3]
                    if jcyk > 0:
                        huizong[c][9] += 1
                        huizong[c][-1][dt][9] += 1
                        jc[c].append([i[2], jcyk])
                    else:
                        huizong[c][10] += 1
                        huizong[c][-1][dt][10] += 1
                        jc[c].append([i[2], jcyk])
                elif last[c] and i[2] <= last[c][-1][4] and (last[c][-1][7] != '空' and i[7] != '空'):
                    jcyk = i[3] - sum(la[3] for la in last[c]) / len(last[c])
                    if jcyk > 0:
                        huizong[c][9] += 1
                        huizong[c][-1][dt][9] += 1
                        jc[c].append([i[2], jcyk])
                    else:
                        huizong[c][10] += 1
                        huizong[c][-1][dt][10] += 1
                        jc[c].append([i[2], jcyk])
                results2.append(i[:9] + [name,])  # 交易明细
                last[c].append(i)
                # 最大持仓
                # lzd = len(last[c])
                lzd = int(sum(i[8] for i in last[c]))
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
        huizong,huizong2 = huizong2,huizong
        # results2 = [(i[0],)+(str(i[1]),)+(i[2],)+(str(i[3]),)+i[4:9]+(id_name.get(i[0],i[0]),) for i in results2]
        is_shipan = True  # 是否实盘
        if user:
            result9 = [i for i in result9 if i[0] == user]

        ids = HSD.IDS
        if results2:
            return render(rq, 'tongjisp.html', locals())

        # results2: [['01-0520186-00', 'MHIN8', '2018-07-16 09:41:13', 28548.0, '2018-07-16 09:41:57', 28518.0, -30.0, '多', 1, '已平仓']...]
        # hc = HSD.huice_day(res)

    if rq_type == '3' and user_name:
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
                   'avg_day': 0, 'least2': [0, 1000, 0, 0], 'most2': [0, -1000, 0, 0]}
        for i in results2:
            dt = str(i[1])[:10]
            if dt not in res:
                res[dt] = {'duo': 0, 'kong': 0, 'mony': 0, 'shenglv': 0, 'ylds': 0, 'datetimes': []}
            if i[8] in (1, 2):
                if i[6] % 2:
                    res[dt]['kong'] += int(i[7])
                    _ykds = i[2] - i[4]  # 盈亏点数
                else:
                    res[dt]['duo'] += int(i[7])
                    _ykds = i[4] - i[2]  # 盈亏点数
                res[dt]['mony'] += i[5]
                xx = [str(i[1]), str(i[3]), '空' if i[6] % 2 else '多', i[5], i[2], i[4], i[7], i[11]]
                res[dt]['datetimes'].append(xx)

                huizong['least'] = [dt, i[5]] if i[5] < huizong['least'][1] else huizong['least']
                huizong['least2'] = [dt, _ykds] if _ykds < huizong['least2'][1] else huizong['least2']
                huizong['most'] = [dt, i[5]] if i[5] > huizong['most'][1] else huizong['most']
                huizong['most2'] = [dt, _ykds] if _ykds > huizong['most2'][1] else huizong['most2']
        hcd = {}
        sql = "SELECT origin_asset FROM account_info WHERE id={}".format(rq_id)
        init_money = HSD.runSqlData('carry_investment', sql)
        init_money = init_money[0][0] if init_money else 10000
        if rq_date == end_date:
            hcd = HSD.huice_day(res, init_money, real=False)
            # return render(rq, 'hc_day.html', {'hcd': hcd})

        res, huizong = viewUtil.tongji_huice(res, huizong)
        hc, huizong = HSD.huices(res, huizong, init_money, rq_date, end_date)
        hc_name = id_name.get(int(rq_id) if rq_id.isdigit() else rq_id)
        if not hc_name:
            hc_name = rq_id
        return render(rq, 'hc.html',
                      {'hc': hc, 'hcd': hcd, 'huizong': huizong, 'init_money': init_money, 'user_name': user_name,
                       'hc_name': hc_name})

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
                    hy = i[12].split('$')[0] if '$' in i[12] else i[12]
                    huizong[c] = [dt, c, 0, 0, 0, 0, 0, 0, c, 0, 0, 0, 0, 1, hy, {}]
                    last[c] = []
                    jc[c] = []
                if dt not in huizong[c][-1]:
                    hy = i[12].split('$')[0] if '$' in i[12] else i[12]
                    huizong[c][-1][dt] = [dt, c, 0, 0, 0, 0, 0, 0, c, 0, 0, 0, 0, 1, hy]

                h1 = i[5]  # 盈亏
                h2 = i[7] if i[6] % 2 == 0 else 0  # 多单数量
                h3 = i[7] if i[6] % 2 == 1 else 0  # 空单数量
                # i (8965391, '2018-08-17 09:58:21', 27215.18, '2018-08-17 10:15:59', 27264.75, -49.57, 1, 1.0, 2, 27264.75, 27114.75, 53680779, 'HSENG$.AUG8')
                h4 = int(i[7])  # 下单总数
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
                # lzd = len(last[c])
                lzd = int(sum(i[7] for i in last[c]))
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
        is_shipan = False  # 是否实盘
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
    # herys = viewUtil.tongji_first()
    with viewUtil.errors():
        herys = herys = HSD.tongji()
    ids = HSD.IDS
    if user_name == 0:
        logins = "欢迎登陆！"  # f"您上次登录地点是：【{qx}】"
        # del rq.session['users']
    elif not user_name:
        logins = "请先登录再访问您需要的页面！"
    return render(rq, 'tongji.html', locals())


def tongji_bs(rq):
    """ 买卖点 """
    user_name, qx, uid = LogIn(rq, uid=True)
    if rq.method == 'GET':
        if not user_name:
            return index(rq, False)
        # when=y&host=1001&code=J1901&ttype=1D

        code = rq.GET.get('code')
        ttype = rq.GET.get('ttype')
        host = rq.GET.get('host')
        start_date = rq.GET.get('start_date')

        if not code or not host:
            return redirect('/')
        # cache_keys = 'cfmmc_future_bs_' + start_date + code + end_date
        # cfmmc = HSD.Cfmmc(host, start_date, end_date)
        # start_date = HSD.dtf(start_date)
        # end_date = HSD.dtf(end_date)

        # data = read_from_cache(cache_keys)
        # _results = viewUtil.runThread((cfmmc.get_bs, code, ttype), (cfmmc.get_yesterday_hold, code))
        # bs = _results['get_bs']
        # hold = _results['get_yesterday_hold']
        # bs = cfmmc.get_bs(code,ttype)
        # hold = cfmmc.get_yesterday_hold(code)
        end_date = HSD.get_date()
        results2, _ = HSD.sp_order_record(start_date, end_date)
        if host:
            results2 = [i for i in results2 if i[0] == host and i[1] == code]
        bs = []
        for i in results2:
            bs.append((i[2][:17] + '00', i[8], ' 卖' if i[7] == '空' else '买', i[3], '开'))
            if i[4]:
                bs.append((i[4][:17] + '00', i[8], '买' if i[7] == '空' else ' 卖', i[5], ' 平'))
        bs.sort()
        if bs:
            start_date = HSD.dtf(bs[0][0][:10]) - datetime.timedelta(days=1)
            end_date = HSD.dtf(bs[-1][0][:10]) + datetime.timedelta(days=3)
        else:
            start_date = HSD.dtf(start_date)
            end_date = HSD.dtf(end_date)
        _days = (end_date - start_date).days
        sql = (f'SELECT datetime,OPEN,CLOSE,low,high,vol FROM wh_same_month_min WHERE prodcode="{code[:3]}"' # ADDDATE(datetime,INTERVAL 1 MINUTE)
               f' and datetime>="{start_date}" and datetime<="{end_date}"')
        data = HSD.runSqlData('carry_investment', sql)

        if not ttype:
            ttype = '1M'

        # data_len = (_days - _days // 7 * 2) * 500  # 分钟数据估计的总长度
        # dsise = 1200  # K线根数的限制
        # if not ttype and data_len > dsise:
        #     if data_len > dsise * 60:
        #         ttype = '1D'
        #     elif data_len > dsise * 30:
        #         ttype = '1H'
        #     elif data_len > dsise * 5:
        #         ttype = '30M'
        #     else:
        #         ttype = '5M'

        # rq_url = rq.META.get('QUERY_STRING')
        # rq_url = rq_url[:rq_url.index('&ttype')] if '&ttype' in rq_url else (
        #     rq_url + '_'.join(param[:2]) if '=' not in rq_url else rq_url)
        # _name = get_cfmmc_id_host(host + '_name')
        # _name = _name[0] + '*' * len(_name[1:])
        # code_name = _name + ' ' + HSD.FUTURE_NAME.get(re.sub('\d', '', code)) + ' ' + code

        code_name = host + ' ' + code

        # bs：{'2018-08-15 21:55:00': (7008.0, -2, '开'), '2018-08-17 22:08:00': (7238.0, -4, '开'),...}

        data2 = []
        if ttype == '5M':
            code_name += '（5分钟）'
            data2bs = viewUtil.future_data_cycle(data, bs, 5)
        elif ttype == '30M':
            code_name += '（30分钟）'
            data2bs = viewUtil.future_data_cycle(data, bs, 30)
        elif ttype == '1H':
            code_name += '（1小时）'
            data2bs = viewUtil.future_data_cycle(data, bs, 60)
        elif ttype == '1D':
            code_name += '（1日）'
            data2bs = viewUtil.future_data_cycle(data, bs, ttype)
        else:
            code_name += '（1分钟）'
            data2bs = viewUtil.future_data_cycle(data, bs, 1)

        open_buy = []  # 开多仓
        flat_buy = []  # 平多仓
        open_sell = []  # 开空仓
        flat_sell = []  # 平空仓
        holds = {}  # 持多仓, 持空仓
        VOL = 1000  # 手数的最大值
        rounds = lambda x: (round(x, round(math.log(VOL, 10))) if x else x)
        ttypes = defaultdict(lambda: 1)
        ttypes['5M'] = 5
        ttypes['30M'] = 30
        ttypes['60M'] = 60
        ttypes['1D'] = 1
        _days = set()
        # print(bs)
        data3 = viewUtil.future_macd(yd=True)
        data3.send(None)
        for i, bs in data2bs:  # data2:
            if not i:
                continue
            # data3.append(i)
            data2.append(data3.send(i))
            _ob, _fb, _os, _fs = '', '', '', ''
            dt = i[0][:10]
            if dt not in _days:
                _days.add(dt)
                _ccb, _ccs = 0, 0  # 持仓多，持仓空
                # yesterday_hold = hold[dt] if dt in hold else (
                #     0, 0)  # ((holds[data2[j-1][0]][0],holds[data2[j-1][0]][1]) if holds else (0, 0))
            # print(i[0])
            if i[0] in bs:
                for b in bs[i[0]]:
                    if '开' in b[4]:
                        if '买' in b[2]:
                            _ob = (int(b[3]) + b[1] / VOL) if not _ob else _ob + b[1] / VOL
                            _ccb += b[1]
                        else:
                            _os = (int(b[3]) + b[1] / VOL) if not _os else _os + b[1] / VOL
                            _ccs += b[1]
                    else:
                        if '买' in b[2]:
                            _fs = (int(b[3]) + b[1] / VOL) if not _fs else _fs + b[1] / VOL
                            _ccs -= b[1]
                        else:
                            _fb = (int(b[3]) + b[1] / VOL) if not _fb else _fb + b[1] / VOL
                            _ccb -= b[1]

            open_buy.append(rounds(_ob))
            flat_buy.append(rounds(_fb))
            open_sell.append(rounds(_os))
            flat_sell.append(rounds(_fs))
            holds[i[0]] = [
                _ccb,  #+ yesterday_hold[0],
                _ccs   #+ yesterday_hold[1]
            ]
        resp = {'user_name': user_name, 'data': data2, 'open_buy': open_buy, 'flat_buy': flat_buy,
                'open_sell': open_sell, 'flat_sell': flat_sell, 'holds': holds, 'start_date': data2[0][0][:10],
                'code': code, 'host': host, 'code_name': code_name}
        # write_to_cache()
        return render(rq, 'tongjisp_bs.html', resp)

    return redirect('/')


def tools(rq):
    user_name, qx = LogIn(rq)
    cljs = models.Clj.objects.all()
    return render(rq, 'tools.html', {'cljs': cljs, 'user_name': user_name})


def kline(rq):
    user_name, qx = LogIn(rq)
    date = rq.GET.get('date', HSD.get_date())
    ts = rq.GET.get('ts', '1')
    database = rq.GET.get('database', '1')
    if not ts.isdigit():
        ts = 1
    else:
        ts = int(ts)
    _key = f'{rq.session.session_key}_kline_date'
    write_to_cache(_key, (date, database, ts))

    return render(rq, 'kline.html', {'date': date, 'user_name': user_name, 'database': database, 'ts': ts})


def getList(rq):
    # 时间,开盘价,最高价,最低价,收盘价,成交量
    data_dict = {'1': ['carry_investment', 'wh_same_month_min'], '2': ['carry_investment', 'wh_min']}
    _key = f'{rq.session.session_key}_kline_date'
    _dates = read_from_cache(_key)
    dates, database, ts = _dates if _dates else (None, None, '1')
    ts = 5 if ts > 5 else ts
    res = ()
    if dates and database:
        # conn = HSD.get_conn(data_dict[database][0])
        if len(dates) == 10:
            dates2 = HSD.dtf(dates) + datetime.timedelta(days=ts)
        else:
            dates2 = HSD.dtf(dates)
            dates = dates2 - datetime.timedelta(minutes=20)
            dates2 = dates2 + datetime.timedelta(days=ts)
            dates2 = str(dates2)[:10]
        if database == '2':
            data = HSD.MongoDBData(db='HKFuture', table='future_1min').get_hsi(dates, dates2)
            res = [i for i in data]
        else:
            sql = (
                    'SELECT datetime,open,high,low,close,vol FROM %s WHERE prodcode="HSI" AND datetime>="%s" '
                    'AND datetime<="%s"' % (data_dict[database][1], dates, dates2)
            )
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
    dc = []
    with viewUtil.errors('views.py', 'getList'):
        data2 = HSD.Zbjs().vis(res)
        dc = data2.send(None)
        data2.close()
    # data2.send(None)
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
    # 1min 5min 15min 30min 1hour 1day 1week
    if rq.is_ajax() and size > 0:
        lists, _ch = getList(rq)
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
                'url': 'www.a667.com',  # （选填）
                'topTickers': [],  # （选填）
            }
        }
        return HttpResponse(json.dumps(data), content_type="application/json")
    elif rq.is_ajax() and size == 0:
        lists, _ch = getList(rq)  # 暂不更新  [], []  #
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
                'url': 'www.a667.com',  # （选填）
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
    user_name, qx = LogIn(rq)
    if not user_name:
        return index(rq, False, user_name, qx)
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
                zt_tomorrow[i][2] = range(int(zt_tomorrow[i][2]))  # ['★' for j in range(zt_tomorrow[i][2])]
        return render(rq, 'zhangting.html',
                      {'jyzt': True, 'zt_tomorrow': zt_tomorrow, 'dates': rq_date, 'up': date_up, 'down': date_down,
                       'user_name': user_name})

    return redirect('page_not_found')


def moni(rq):
    """ 模拟测试 """
    user_name, qx = LogIn(rq)
    if rq.method == 'GET':
        dates = rq.GET.get('dates', '')
        end_date = rq.GET.get('end_date', '')
        fa = rq.GET.get('fa', '')
        database = rq.GET.get('database', '1')
        reverse = rq.GET.get('reverse', '')
        zsds = rq.GET.get('zsds', '')  # 止损
        ydzs = rq.GET.get('ydzs', '')  # 移动止损
        zyds = rq.GET.get('zyds', '')  # 止盈
        cqdc = rq.GET.get('cqdc', '')  # 点差
        red = HSD.RedisPool()
        red_key = ''.join(('moni', dates, end_date, fa, database, reverse, zsds, ydzs, zyds, cqdc))
        if rq.is_ajax():
            resp = red.get(red_key)
            if not resp:
                data_line = red.get(red_key+'_line')
                d = data_line if data_line else ['','','','']
                return JsonResponse({'s': -1, 't': d[0], 'm': d[1], 'd': d[2], 'k': d[3]})  # (-1)  # 继续请求
            elif resp == 0:
                red.delete(red_key)
                return JsonResponse({'s': 0})  # 数据计算有误
            else:
                return JsonResponse({'s': 1})  # 数据成功写入
        else:
            zsds, ydzs, zyds, cqdc = HSD.format_int(zsds, ydzs, zyds, cqdc) if zsds and ydzs and zyds and cqdc else (
                100, 100, 200, 6)
            reverse = True if reverse else False
            if dates and end_date and fa:
                resp = red.get(red_key)
                if resp:
                    if HSD.computer_name == 'doc':
                        red.delete(red_key)
                    resp['user_name'] = user_name
                    return render(rq, 'moni.html', resp)
                else:
                    viewUtil.moni(dates, end_date, fa, database, reverse, zsds, ydzs, zyds, cqdc, red_key)
                    url_param = rq.META.get('QUERY_STRING')
                    rq_url = '?'.join([rq.META.get('PATH_INFO'), url_param]) if url_param else rq.META.get('PATH_INFO')
                    # if not rq_url:
                    #     rq_url = rq.META.get('PATH_INFO')
                    return render(rq, 'base/loading.html', {'user_name': user_name, 'rq_url': rq_url})

            else:
                zbjs = HSD.Zbjs()
                dates = datetime.datetime.now()
                day = dates.weekday() + 4
                start_date = str(dates - datetime.timedelta(days=day))[:10]
                end_date = str(dates - datetime.timedelta(days=1))[:10]
                return render(rq, 'moni.html',
                              {'dates': start_date, 'end_date': end_date, 'fas': zbjs.xzfa, 'database': database,
                               'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc, 'user_name': user_name})

    return redirect('index')


def newMoni(rq):
    user_name, qx = LogIn(rq)
    _max = rq.GET.get('MAX')
    red = HSD.RedisPool()
    red_key = 'newMoni_max_yk'
    # 最高胜率，最高总盈亏，最高每单盈亏，最高每天盈亏
    _maxs = {'1': 'shenglv', '2': 'yk', '3': 'avg', '4': 'avg_day'}
    max_yk = red.get(red_key)  # resps
    if _max in _maxs:
        if max_yk and _max in max_yk:
            return render(rq, 'new_moni.html', max_yk[_max])

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
        with viewUtil.errors('views', 'newMoni'):
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
            res_length = len(keys)
            res = [dict(res[k], **{'time': k}) for k in keys[:300]]  # 限制最多显示300天交易详细记录
            fa_doc = zbjs.fa_doc

            resp = {'res': res, 'keys': keys, 'dates': dates, 'end_date': end_date,  # 'fas': zbjs.xzfa,
                    'fa_doc': fa_doc, 'fa_one': 'fa_doc.get(fa)', 'huizong': huizong, 'database': database,
                    'first_time': first_time, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc,
                    'res_length': res_length,

                    "duo_macd": duo_macd, "duo_avg": duo_avg, "duo_yidong": duo_yidong,
                    "duo_chonghes": duo_chonghes, "duo_chonghed": duo_chonghed, "kong_macd": kong_macd,
                    "kong_avg": kong_avg, "kong_yidong": kong_yidong, "kong_chonghes": kong_chonghes,
                    "kong_chonghed": kong_chonghed, "pdd_macd": pdd_macd, "pdd_avg": pdd_avg,
                    "pdd_yidong": pdd_yidong, "pdd_chonghes": pdd_chonghes, "pdd_chonghed": pdd_chonghed,
                    "pkd_macd": pkd_macd, "pkd_avg": pkd_avg, "pkd_yidong": pkd_yidong,
                    "pkd_chonghes": pkd_chonghes, "pkd_chonghed": pkd_chonghed, 'user_name': user_name
                    }
            # print(dates,type(dates),end_date,type(end_date))
            if not max_yk:
                max_yk = {}
            for i in _maxs:
                if i not in max_yk:
                    max_yk[i] = resp
                elif max_yk[i]['huizong'][_maxs[i]] <= huizong[_maxs[i]]:
                    max_yk[i] = resp
            red.set(red_key, max_yk, 2592000)  # 保存30天
            return render(rq, 'new_moni.html', resp)

    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    return render(rq, 'new_moni.html', {'dates': dates, 'end_date': end_date, 'fas': zbjs.xzfa, 'database': database,
                                        'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds, 'cqdc': cqdc, 'user_name': user_name,
                                        })


def moni_all(rq):
    user_name, qx = LogIn(rq)
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
        with viewUtil.errors('views', 'moni_all'):
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
        # except Exception as exc:
        #     viewUtil.error_log(sys.argv[0], sys._getframe().f_lineno, exc)
    dates = datetime.datetime.now()
    day = dates.weekday() + 3
    dates = str(dates - datetime.timedelta(days=day))[:10]
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    return render(rq, 'moniAll.html',
                  {'dates': dates, 'end_date': end_date, 'database': database, 'zsds': zsds, 'ydzs': ydzs, 'zyds': zyds,
                   'cqdc': cqdc, 'user_name': user_name})


def gdzd(rq):
    """ 高开，上涨 """
    user_name, qx = LogIn(rq)
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
    user_name, qx = LogIn(rq)
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
            logging.error("文件：{} 第{}行报错： {}".format('views.py', sys._getframe().f_lineno, exc))
            return redirect('index')

        return render(rq, 'hc.html', {'hc': hc, 'huizong': huizong, 'user_name': user_name, 'hc_name': '方案 ' + fa})

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
    user_name, qx = LogIn(rq)
    folder1 = r'\\192.168.2.226\公共文件夹\gx\历史成交'
    folder2 = r'\\192.168.2.226\公共文件夹\gx\出入金'
    if user_name:  # 内部网络或登录用户
        types = rq.GET.get('type')
        code = rq.GET.get('code')
        group = rq.GET.get('group')
        start_date = rq.GET.get('start_date')
        end_date = rq.GET.get('end_date')
        start_date = None if start_date == 'undefined' else start_date
        h = HSD.GXJY()

        if types == 'sx':  # 刷新
            try:
                viewUtil.gxjy_refresh.delay(h, folder1, folder2)
                init_data = h.get_gxjy_sql_all()
                response = render(rq, 'gxjy.html', {'init_data': init_data, 'user_name': user_name})
            except Exception as exc:
                logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0], sys._getframe().f_lineno, exc))
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
                data.sort(key=lambda x: x[1])
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
        elif types == 'tjt':  # 可视化图形展示
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
            for de, *_ in dates:
                zx_x.append(de)
                yk, sxf = 0, 0
                f_date = datetime.datetime.strptime(de, '%Y-%m-%d').isocalendar()[:2]
                week = str(f_date[0]) + '-' + str(f_date[1])  # 星期
                month = de[:7]  # 月
                for d in data:
                    if d[1][:10] == de:
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
                rj = sum(i[3] - i[2] for i in ee if i[0] <= de)
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
        elif types == 'hc':  # 这个模块暂时取消
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
            for i in []:
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
            sql = "SELECT origin_asset FROM account_info WHERE id={}".format('rq_id')
            init_money = HSD.runSqlData('carry_investment', sql)
            # conn.close()
            init_money = init_money[0][0]
            hc, huizong = HSD.huices(res, huizong, init_money, 'rq_date', end_date)

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


cfmmc_login_ds = viewUtil.Dict()  # [是否登录期货监控系统, 期货监控系统类的实例化]


def cfmmc_login(rq):
    """ 期货监控系统 登录"""
    user_name, qx, uid = LogIn(rq, uid=True)
    # print(rq.META)
    # if not user_name:
    #     return index(rq, False)
    cd_ = f"cfmmc_login_d_{rq.META.get('CSRF_COOKIE')}"
    # cfmmc_login_d = red.get(cd_, _object=True)
    global cfmmc_login_ds
    cfmmc_login_d = cfmmc_login_ds[cd_]
    if not cfmmc_login_d:
        cfmmc_login_d = [False, viewUtil.Cfmmc(cd_)]
        cfmmc_login_ds[cd_] = cfmmc_login_d
    cfmmc_login_d = cfmmc_login_d[1]
    token = cfmmc_login_d.getToken(cfmmc_login_d._login_url)
    success = False
    code_name = {}  # 合约代码对应中文名
    if rq.method == 'GET' and rq.is_ajax():
        code = cfmmc_login_d.getCode()
        code = base64.b64encode(code)
        code = b"data:image/jpeg;base64," + code
        return HttpResponse(code)
    elif rq.method == 'POST':
        userID = rq.POST['userID'].strip()
        password = rq.POST['password'].strip()
        vericode = rq.POST['vericode'].strip()
        with viewUtil.errors('views', 'cfmmc_login'):
            tda = models.TradingAccount.objects.get(host=userID)
            ct = tda.creationTime
            if tda.belonged_id == uid and password[:8] == 'KR' + ct[-6:]:
                password = pypass.cfmmc_decode(password[8:], ct)

        success = cfmmc_login_d.login(userID, password, token, vericode)
        if success is True:
            createTime = str(int(time.time() * 100))
            if not models.TradingAccount.objects.filter(host=userID).exists():
                password = pypass.cfmmc_encode(password, createTime)
                rq.session['user_cfmmc'] = {'userID': userID, 'password': password, 'createTime': createTime}
                cd2_ = f"cfmmc_login_d_{userID}"
                cfmmc_login_ds[cd2_] = [True, cfmmc_login_d]  # 成功登陆
                cfmmc_login_ds.pop(cd_)
                response = {'logins': '期货监控系统登录成功！', 'user_name': user_name, 'success': 'success'}
            else:
                rq.session['user_cfmmc'] = {'userID': userID, 'password': password}
                response = {'logins': '期货监控系统登录成功！', 'user_name': user_name}
            sql = "INSERT INTO cfmmc_user(host,password,cookie,download,name,creationTime) VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE cookie=''"
            HSD.runSqlData('carry_investment', sql, (userID, password, '', 1, '', createTime))
            trade, start_date, end_date = viewUtil.cfmmc_data_page(rq)
            codes = set(i[0] for i in trade)  # 合约代码
            code_name = viewUtil.cfmmc_code_name(codes)
            if trade:  # 若已经有下载过数据，则下载3天之内的
                trade = []
                _start_date = HSD.get_date(-3)
                start_date = _start_date if _start_date < end_date else end_date
                end_date = HSD.get_date()
                cfmmc_login_d.down_day_data_sql(userID, start_date, end_date, password, createTime)
                response['logins'] = f'登录成功！正在更新{start_date}之后的数据！'
            else:  # 若没下载过数据，则下载360天之内的
                start_date = HSD.get_date(-360)
                end_date = HSD.get_date()
                cfmmc_login_d.down_day_data_sql(userID, start_date, end_date, password, createTime)
                response['logins'] = f'登录成功！正在更新{start_date}之后的数据！'
            response['host_id'] = get_cfmmc_id_host(userID, True)
            response['trade'] = trade
            response['start_date'] = start_date
            response['end_date'] = end_date
            response['code_name'] = code_name
            return render(rq, 'cfmmc_data.html', response)

    code = cfmmc_login_d.getCode()
    code = base64.b64encode(code)
    code = b"data:image/jpeg;base64," + code
    users = []
    try:
        tdas = models.TradingAccount.objects.filter(belonged_id=uid, enabled=1)
        users = [[i.host, 'KR' + i.creationTime[-6:] + i.password] for i in tdas]
        host, password = (users[0][0], users[0][1])
        users = [] if len(users) == 1 else users
    except:
        host, password = '', ''
    return render(rq, 'cfmmc_login.html',
                  {'code': code, 'user_name': user_name, 'success': success, 'users': users, 'host': host,
                   'password': password})


def cfmmc_isdownload_data(rq):
    """ ajax 请求，判断是否下载数据成功！"""
    if rq.is_ajax() and 'user_cfmmc' in rq.session:
        host = rq.session['user_cfmmc']['userID']
        res = cache.get('cfmmc_status' + host)
        if not res:
            return HttpResponse('not_login')
        if '日' in res:
            jg = res
        elif res == 'True':
            jg = '所有数据更新完毕！'
        elif res == 'False':
            jg = ''  # '部分数据更新失败！'
        elif res == 'not_run':
            jg = '数据已经更新过！'
        else:
            jg = 'no'
        cache.delete('cfmmc_status' + host) if jg != 'no' and '日' not in res else 0
        return HttpResponse(jg)
    return HttpResponse('no')


def cfmmc_data(rq):
    """ 期货监控系统 下载数据 """
    user_name, qx, uid = LogIn(rq, uid=True)
    # if not user_name:
    #     return index(rq, False)
    start_date = rq.GET.get('start_date')
    end_date = rq.GET.get('end_date')
    host = None
    cfmmc_login = None

    try:
        if 'user_cfmmc' in rq.session and start_date and end_date:
            host = rq.session['user_cfmmc'].get('userID')
            cd_ = f"cfmmc_login_d_{host}"
            cfmmc_login = cfmmc_login_ds.get(cd_)
            cfmmc_login_d = cfmmc_login[1] if cfmmc_login else None
            cfmmc_login_d.down_day_data_sql(host, start_date, end_date)
            status = True
        else:
            status = False
    except:
        status = False
    logins = '下载失败！可能由于数据已经下载！' if (cfmmc_login and cfmmc_login[0]) else '没有登录！'
    if status:
        logins = '数据正在下载！如果时间跨度过长，需等待几分钟！'
    trade, start_date, end_date = viewUtil.cfmmc_data_page(rq)
    host_id = get_cfmmc_id_host(host)
    resp = {'logins': logins, 'user_name': user_name, 'trade': trade, 'start_date': start_date, 'end_date': end_date,
            'host_id': host_id}
    return render(rq, 'cfmmc_data.html', resp)


def cfmmc_logout(rq):
    """ 期货监控系统 退出 """
    user_name, qx = getLogin(rq)
    # if not user_name:
    #     return index(rq, False)
    logins = '尚未登录--期货监控中心！'  # 账户记录退出！
    if 'user_cfmmc' in rq.session:
        with viewUtil.errors('views', 'cfmmc_logout'):
            logins = '退出失败！'
            host = rq.session['user_cfmmc'].get('userID')
            cd_ = f"cfmmc_login_d_{host}"
            cfmmc_login = cfmmc_login_ds.get(cd_)
            cfmmc_login_d = cfmmc_login[1] if cfmmc_login else None
            if cfmmc_login_d and cfmmc_login_d.logout():
                logins = '退出成功！'
                cfmmc_login_ds.delete(cd_)
        del rq.session['user_cfmmc']


    trade, start_date, end_date = viewUtil.cfmmc_data_page(rq)
    resp = {'user_name': user_name, 'logins': logins, 'trade': trade, 'start_date': start_date, 'end_date': end_date}
    return render(rq, 'cfmmc_data.html', resp)


def cfmmc_data_page(rq):
    """ 期货监控系统 展示页面 """
    user_name, qx, uid = LogIn(rq, uid=True)
    # if not user_name:
    #     return index(rq, False)
    host = rq.GET.get('host')
    if host and user_name and len(host) <= 7:
        host = get_cfmmc_id_host(host)
    rq_code_name = rq.GET.get('code_name')

    when = rq.GET.get('when')
    when, start_date, end_date = viewUtil.this_day_week_month_year(when)
    if host:
        rq.session['user_cfmmc'] = {'userID': host}

    if 'user_cfmmc' not in rq.session:
        return render(rq, 'cfmmc_data.html', {'user_name': user_name, 'is_cfmmc_login': 'no'})
    else:
        host = rq.session['user_cfmmc'].get('userID')
    trade, _start_date, _end_date = viewUtil.cfmmc_data_page(rq, start_date, end_date)
    if not start_date or not end_date:
        start_date, end_date = _start_date, _end_date
    host_id = get_cfmmc_id_host(host)
    if not trade:
        # del rq.session['user_cfmmc']
        return render(rq, 'cfmmc_data.html',
                      {'user_name': user_name, 'is_cfmmc_login': 'nos', 'logins': '暂无数据！可先登录期货监控中心',
                       'start_date': start_date, 'end_date': end_date, 'when': when, 'host_id': host_id})
    codes = set(i[0] for i in trade)  # 合约代码
    code_name = viewUtil.cfmmc_code_name(codes)
    if rq_code_name and rq_code_name not in ('null', '1'):
        trade = [i for i in trade if rq_code_name in i[0]]
    resp = {'user_name': user_name, 'trade': trade, 'start_date': start_date, 'end_date': end_date,
            'code_name': code_name, 'host_id': host_id, 'rq_code_name': rq_code_name, 'when': when}
    return render(rq, 'cfmmc_data.html', resp)


def cfmmc_data_local(rq):
    """ 本站期货交易数据 """
    # _results = viewUtil.runThread((LogIn,rq), (viewUtil.get_cfmmc_trade,), (get_cfmmc_id_host,))
    # user_name, qx = _results['LogIn']
    # trades = _results['get_cfmmc_trade']
    # id_host = _results['get_cfmmc_id_host']
    user_name, qx = LogIn(rq)
    trades = viewUtil.get_cfmmc_trade()
    id_host = get_cfmmc_id_host()
    hosts_len = len(id_host) // 3
    hosts = set(id_host)
    trade = []
    # ('RB1810', '00037695', ' 21:02:15', '买', '投机', 3638.0, 3, 109140.0, ' 平', 11.3, 120.0, '2018-05-30', '0060660900202549', '2018-05-31')
    j = 0
    for i in trades:
        name = id_host.get(i[12] + '_name')
        name = name[0] + '*' * (len(name) - 1) if name else None
        if i[12] in hosts:
            trade.append(i + (id_host.get(i[12], i[12]), name))
            hosts.remove(i[12])
            j += 1
        elif j >= hosts_len:
            break
    # trade = [i+(id_host.get(i[12],i[12]),id_host.get(i[12]+'_name')) for i in trade]
    return render(rq, 'cfmmc_data_local.html', {'user_name': user_name, 'trade': trade})


def cfmmc_save(rq):
    """ 保存期货监控系统的用户名与密码 """
    user_name, qx, uid = getLogin(rq, uid=True)
    if not user_name:
        return index(rq, False, user_name, qx)
    if rq.method == 'GET' and rq.is_ajax():
        ty = rq.GET.get('type')
        if ty == 'save' and 'user_cfmmc' in rq.session:
            # rq.session['user_cfmmc'] = {'userID': userID, 'password': password}
            userID = rq.session['user_cfmmc']['userID']
            password = rq.session['user_cfmmc']['password']
            createTime = rq.session['user_cfmmc']['createTime']
            models.TradingAccount.objects.create(belonged_id=uid, host=userID, password=password,
                                                 creationTime=createTime).save()
            return HttpResponse('yes')
    return HttpResponse('no')


def cfmmc_bs(rq, param=None):
    """ 买卖点 """
    user_name, qx, uid = LogIn(rq, uid=True)
    if rq.method == 'GET':
        # when=y&host=1001&code=J1901&ttype=1D
        if param is not None:
            param = param.split('_')
            when = param[0][0]
            host = param[0][1:]
            code = param[1]
            ttype = param[2] if len(param) > 2 else None
        else:
            code = rq.GET.get('code_name')
            when = rq.GET.get('when')
            ttype = rq.GET.get('ttype')
            host = rq.GET.get('host')
        if not code or code == '1':
            code = rq.GET.get('code')

        when, start_date, end_date = viewUtil.this_day_week_month_year(when)
        if start_date is None:
            start_date = rq.GET.get('start_date')
            end_date = rq.GET.get('end_date')

        host = get_cfmmc_id_host(host)
        if not code or not start_date or not end_date or not host:
            return redirect('/')
        cache_keys = 'cfmmc_future_bs_' + start_date + code + end_date
        cfmmc = HSD.Cfmmc(host, start_date, end_date)
        # start_date = HSD.dtf(start_date)
        # end_date = HSD.dtf(end_date)
        mongo = HSD.MongoDBData()

        # data = read_from_cache(cache_keys)
        _results = viewUtil.runThread((cfmmc.get_bs, code, ttype), (cfmmc.get_yesterday_hold, code))
        bs = _results['get_bs']
        hold = _results['get_yesterday_hold']
        # bs = cfmmc.get_bs(code,ttype)
        # hold = cfmmc.get_yesterday_hold(code)
        if bs:
            start_date = HSD.dtf(bs[0][0][:10]) - datetime.timedelta(days=1)
            end_date = HSD.dtf(bs[-1][0][:10]) + datetime.timedelta(days=3)
        else:
            start_date = HSD.dtf(start_date)
            end_date = HSD.dtf(end_date)
        _days = (end_date - start_date).days
        data = mongo.get_data(code, start_date, end_date)
        data_len = (_days - _days // 7 * 2) * 500  # 分钟数据估计的总长度
        dsise = 1200  # K线根数的限制
        if not ttype and data_len > dsise:
            if data_len > dsise * 60:
                ttype = '1D'
            elif data_len > dsise * 30:
                ttype = '1H'
            elif data_len > dsise * 5:
                ttype = '30M'
            else:
                ttype = '5M'

        rq_url = rq.META.get('QUERY_STRING')
        rq_url = rq_url[:rq_url.index('&ttype')] if '&ttype' in rq_url else (
            rq_url + '_'.join(param[:2]) if '=' not in rq_url else rq_url)
        _name = get_cfmmc_id_host(host + '_name')
        _name = _name[0] + '*' * len(_name[1:])
        code_name = _name + ' ' + HSD.FUTURE_NAME.get(re.sub('\d', '', code)) + ' ' + code

        # bs：{'2018-08-15 21:55:00': (7008.0, -2, '开'), '2018-08-17 22:08:00': (7238.0, -4, '开'),...}

        data2 = []
        if ttype == '5M':
            code_name += '（5分钟）'
            data2bs = viewUtil.future_data_cycle(data, bs, 5)
        elif ttype == '30M':
            code_name += '（30分钟）'
            data2bs = viewUtil.future_data_cycle(data, bs, 30)
        elif ttype == '1H':
            code_name += '（1小时）'
            data2bs = viewUtil.future_data_cycle(data, bs, 60)
        elif ttype == '1D':
            code_name += '（1日）'
            data2bs = viewUtil.future_data_cycle(data, bs, ttype)
        else:
            code_name += '（1分钟）'
            data2bs = viewUtil.future_data_cycle(data, bs, 1)

        open_buy = []  # 开多仓
        flat_buy = []  # 平多仓
        open_sell = []  # 开空仓
        flat_sell = []  # 平空仓
        holds = {}  # 持多仓, 持空仓
        VOL = 1000  # 手数的最大值
        rounds = lambda x: (round(x, round(math.log(VOL, 10))) if x else x)
        ttypes = defaultdict(lambda: 1)
        ttypes['5M'] = 5
        ttypes['30M'] = 30
        ttypes['60M'] = 60
        ttypes['1D'] = 1
        _days = set()
        # print(bs)
        data3 = viewUtil.future_macd()
        data3.send(None)
        for i, bs in data2bs:  # data2:
            if not i:
                continue
            # data3.append(i)
            data2.append(data3.send(i))
            _ob, _fb, _os, _fs = '', '', '', ''
            dt = i[0][:10]
            if dt not in _days:
                _days.add(dt)
                _ccb, _ccs = 0, 0  # 持仓多，持仓空
                yesterday_hold = hold[dt] if dt in hold else (
                    0, 0)  # ((holds[data2[j-1][0]][0],holds[data2[j-1][0]][1]) if holds else (0, 0))
            # print(i[0])
            if i[0] in bs:
                for b in bs[i[0]]:
                    if '开' in b[4]:
                        if '买' in b[2]:
                            _ob = (int(b[3]) + b[1] / VOL) if not _ob else _ob + b[1] / VOL
                            _ccb += b[1]
                        else:
                            _os = (int(b[3]) + b[1] / VOL) if not _os else _os + b[1] / VOL
                            _ccs += b[1]
                    else:
                        if '买' in b[2]:
                            _fs = (int(b[3]) + b[1] / VOL) if not _fs else _fs + b[1] / VOL
                            _ccs -= b[1]
                        else:
                            _fb = (int(b[3]) + b[1] / VOL) if not _fb else _fb + b[1] / VOL
                            _ccb -= b[1]

            open_buy.append(rounds(_ob))
            flat_buy.append(rounds(_fb))
            open_sell.append(rounds(_os))
            flat_sell.append(rounds(_fs))
            holds[i[0]] = [
                _ccb + yesterday_hold[0],
                _ccs + yesterday_hold[1]
            ]
        resp = {'user_name': user_name, 'data': data2, 'open_buy': open_buy, 'flat_buy': flat_buy,
                'open_sell': open_sell, 'flat_sell': flat_sell, 'holds': holds, 'rq_url': rq_url,
                'code_name': code_name}
        # write_to_cache()
        return render(rq, 'cfmmc_kline2.html', resp)

    return redirect('/')


def cfmmc_hc(rq):
    """ 期货回测，数据，图。暂时取消使用 """
    user_name, qx = LogIn(rq)
    host = rq.GET.get('host')
    host = get_cfmmc_id_host(host)
    rq_date = rq.GET.get('start_date')
    end_date = rq.GET.get('end_date')
    hc, hcd, huizong, init_money = viewUtil.cfmmc_hc_data(host, rq_date, end_date)
    hc_name = get_cfmmc_id_host(host + '_name')
    hc_name = hc_name[0] + '*' * (len(hc_name) - 1)
    return render(rq, 'cfmmc_hc.html',
                  {'hc': hc, 'huizong': huizong, 'init_money': init_money, 'hcd': hcd, 'user_name': user_name,
                   'hc_name': hc_name})


def cfmmc_huice(rq, param=None):
    """ 期货回测，绘图 """
    if param is not None:
        when = param[0]
        _host = param[1:]
    else:
        _host = rq.GET.get('host')
        when = rq.GET.get('when')
    if rq.is_ajax() and rq.method == 'GET':
        if not _host:
            return HttpResponse(0)
        host = get_cfmmc_id_host(_host)
        when, start_date, end_date = viewUtil.this_day_week_month_year(when)
        if start_date is None:
            start_date = rq.GET.get('start_date', '1970-01-01')
            end_date = rq.GET.get('end_date', '2100-01-01')

        cfmmc = HSD.Cfmmc(host, start_date, end_date)
        data = cfmmc.get_data()
        _date = data.send(None)

        cfmmc_huice_key = f'cfmmc_huice_{host}_{_date[0]}_{_date[1]}'
        red = HSD.RedisPool()
        resp = red.get(cfmmc_huice_key)

        if resp == 0:  # 错误
            red.delete(cfmmc_huice_key)
            return JsonResponse({'s': 0})
        elif not resp: # 统计中
            return JsonResponse({'s': -1, 't': ''})
        else:  # 统计完毕
            return JsonResponse({'s': 1})
    elif rq.method == 'GET':
        user_name, qx = LogIn(rq)

        if not _host:
            return redirect('/')
        host = get_cfmmc_id_host(_host)
        when, start_date, end_date = viewUtil.this_day_week_month_year(when)
        if start_date is None:
            start_date = rq.GET.get('start_date', '1970-01-01')
            end_date = rq.GET.get('end_date', '2100-01-01')

        cfmmc = HSD.Cfmmc(host, start_date, end_date)

        data = cfmmc.get_data()
        _date = data.send(None)
        cfmmc_huice_key = f'cfmmc_huice_{host}_{_date[0]}_{_date[1]}'
        red = HSD.RedisPool()
        resp = red.get(cfmmc_huice_key)
        if resp:
            resp['user_name'] = user_name
            return render(rq, 'cfmmc_tu.html', resp)
        else:
            hc_name = get_cfmmc_id_host(host + '_name')
            viewUtil.cfmmc_huice(data, host, start_date, end_date, hc_name, cfmmc_huice_key)
            url_param = rq.META.get('QUERY_STRING')
            rq_url = '?'.join([rq.META.get('PATH_INFO'), url_param]) if url_param else rq.META.get('PATH_INFO')
            # if not rq_url:
            #     rq_url = rq.META.get('PATH_INFO')
            return render(rq, 'base/loading.html',
                          {'user_name': user_name, 'host': _host, 'when': when, 'rq_url': rq_url})


def interface_huice(rq):
    """ 回测接口"""
    user_name, qx = LogIn(rq)
    # _folder = HSD.get_external_folder('huice')  # 保存文件的目录
    if HSD.computer_name == 'doc':
        _folder = r'D:\Kairui_data_file\策略文件'
    else:
        _folder = r'E:\work_File\公共文件夹\策略文件'
    if rq.method == 'GET':
        _type = rq.GET.get('type')
        hc_name = rq.GET.get('id')
        if not hc_name:

            clouds = viewUtil.get_interface_file(_folder) #[i for i in os.listdir(_folder) if i.endswith('.pkl')]
            return render(rq, 'interface_huice.html', {'user_name': user_name, 'clouds': clouds})
        if _type == 'huice':
            data_huice = viewUtil.get_interface_huice(os.path.join(_folder,hc_name))
            hc, huizong, init_money = data_huice.send(None)
            resp = {'hc': hc, 'huizong': huizong, 'init_money': init_money, 'hc_name': hc_name}
            resp['user_name'] = user_name
            return render(rq, 'interface_hc.html', resp)
        elif _type == 'huice2':  # 回测、画图
            clouds = viewUtil.get_interface_file(_folder)

            datas = viewUtil.get_interface_datas(os.path.join(_folder,hc_name))
            summary, trades, portfolio, future_account, future_positions = datas['summary'], datas['trades'], datas[
                'portfolio'], datas['future_account'], datas['future_positions']
            start_date, end_date = summary['start_date'], summary['end_date']
            # [('J1901(冶金焦炭)', 37794500), ('SR901(白糖)', 1427940), ('Y1901(豆油)', 704440),
            #  ('J1809(冶金焦炭)', 501400), ('M1901(豆粕)', 421210), ('NI1811(镍)', 208200), ('CF901(棉花)', 160050), ('MA901(甲醇)', 128540)]
            pzs = []
            # {'2018-08-07': 268215.0, '2018-08-08': 268215.0, '2018-08-09': 268215.0, '2018-08-10': 264461.0, ...}
            _qy = {str(i)[:10]: j for i, j in portfolio.total_value.items()}

            # {'2018-08-08': 1.0, '2018-08-09': 1.0, '2018-08-10': 0.9860018268926048, ...}
            jzs = {str(i)[:10]: j for i, j in portfolio.unit_net_value.items()}
            jz = 1  # 初始净值
            jzq = 0  # 初始净值权重
            allje = 0  # init_money  # 总金额
            eae = []  # 出入金
            zx_x, prices = [str(i)[:10] for i in future_account.index], []

            data_huice = viewUtil.get_interface_huice(os.path.join(_folder,hc_name), start_date, end_date)
            hc, huizong, init_money = data_huice.send(None)
            base_money = init_money  # 初始总资金

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
            for de in zx_x:
                yk, sxf = 0, 0
                f_date = datetime.datetime.strptime(de, '%Y-%m-%d').isocalendar()[:2]
                week = str(f_date[0]) + '-' + str(f_date[1])  # 星期
                month = de[:7]  # 月
                # data: ['2018-10-11', 'SR901', 270, 0.0, 'SR901(白糖)']
                # ['2018-09-21', 'CF901', -1200, 4.31, 'CF901(棉花)']
                # ['2018-09-21', 'J1901', 1550, 98.2, 'J1901(冶金焦炭)']
                # ['2018-09-19', 'J1901', -6000, 98.28999999999999, 'J1901(冶金焦炭)']

                for d in data_huice:
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
                rj = init_money
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
                data_huice = data2
                data2 = []
            if hct['zjhc']:
                huizong['max_zjhc'] = max(hct['zjhc'])  # 最大回测
            try:
                start_date, end_date = zx_x[0], zx_x[-1]
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
                huizong['deposit'] = init_money  # 存款
                huizong['draw'] = 0  # 取款
                huizong['ykratio'] = abs(round(hc['avglr'] / (hc['avgss'] if hc['avgss'] != 0 else 1), 2))
                huizong['huibaolv'] = hct['alljz'][-1]  # 回报率

                resp = {'zx_x': zx_x, 'hct': hct, 'start_date': start_date, 'end_date': end_date, 'hc': hc,
                        'huizong': huizong, 'init_money': init_money, 'hc_name': hc_name,
                        'user_name': user_name, 'clouds': clouds}
            except Exception as exc:
                resp = {'hc_name': hc_name, 'user_name': user_name, 'clouds': clouds}
                print(exc)

            return render(rq, 'interface_huice.html', resp)
        elif _type == 'huice3':
            # import requests
            # d = requests.post('http://192.168.2.237:8666', files={'file': open(os.path.join(_folder,hc_name), 'rb')})
            # d = d.text
            # d=d.replace("$.get('/bars/'","$.get('http://192.168.2.237:8666/bars/'")
            # return HttpResponse(d)
            # print('huice3......................')
            # from django.http import FileResponse
            # hc_name = r'CTP\dual_thrust\result.pkl'
            return redirect('http://192.168.2.237:8666/backtest/'+hc_name.split('\\')[-1])

        elif _type == 'down' and qx >= 2:
            path_file = os.path.join(_folder, hc_name)
            if os.path.isfile(path_file):
                resp = StreamingHttpResponse(
                    viewUtil.file_iterator(path_file))  # viewUtil.FileWrapper(open(path_file,'rb'))
                resp['Content-Type'] = 'application/octet-stream'
                resp['Content-Disposition'] = 'attachment;filename="{}"'.format(hc_name)  # 此处file_name是要下载的文件的文件名称
                return resp
        elif _type in {'code', 'explain'}:
            path_file = os.path.join(_folder, hc_name)
            pys = b''
            logins = ''
            if user_name and qx >= 2:
                if os.path.isfile(path_file):
                    with open(path_file,'rb') as f:
                        pys = f.read()
                else:
                    logins = '文件不存在！'
            else:
                logins = '权限不够！'
            pys = pys.decode()
            clouds = viewUtil.get_interface_file(_folder)
            return render(rq, 'interface_huice.html',
                          {'user_name': user_name, 'clouds': clouds, 'pys': pys, 'logins': logins})
    elif rq.method == 'POST' and qx >= 2:
        upload_file = rq.FILES.get('file')  # 获得文件
        if upload_file:
            file_name = upload_file.name
            path_file = os.path.join(_folder, file_name)
            if not os.path.isfile(path_file) and upload_file.size < 1024 * 1024 * 10:  # 限制文件最大10M
                with open(path_file, 'wb+') as f:
                    for chunk in upload_file.chunks():
                        f.write(chunk)
        clouds = [i for i in os.listdir(_folder) if i.endswith('.pkl')]
        return render(rq, 'interface_huice.html', {'user_name': user_name, 'clouds': clouds})

    return redirect('index')


def hqzj(rq):
    """ 行情总结 """
    user_name, qx = LogIn(rq)
    if rq.method == 'GET':
        sd = rq.GET.get('sd')
        ed = rq.GET.get('ed')
        db = rq.GET.get('db')
        ttype = rq.GET.get('ttype')
        hengpan = rq.GET.get('hengpan')
        if hengpan and hengpan.isdigit():
            hengpan = int(hengpan)
        else:
            hengpan = 0
        moni = rq.GET.get('moni')
        respUrl = 'hqzj.html' if moni != '1' else 'shishimoni.html'
        if sd and ed and db and ttype:
            sd = sd[:10]
            ed = ed[:10]
            if ttype == 'macd':
                zts = Wave.interval_macd(sd, ed, database=db, hengpan=hengpan)
                ec_name = '以MACD为界'
            elif ttype == 'ma60':
                zts = Wave.interval_ma60(sd, ed, database=db, hengpan=hengpan)
                ec_name = '以60均线为界'
            elif ttype == 'change':
                zts = Wave.interval_change(sd, ed, database=db, hengpan=hengpan)
                ec_name = '以异动为界'
            elif ttype == 'yinyang':
                zts = Wave.interval_yinyang(sd, ed, database=db, hengpan=hengpan)
                ec_name = '以阴阳线为界'
            else:
                zts = []
                ec_name = ''
            data = [[i[0], i[2], i[5], i[4], i[3], i[6]] for i in zts[1:]]
            data = HSD.get_macd(data)  # 计算Macd
            parhead = list(zts[0])  # 字段名称
            zts = {i[0]: list(i) for i in zts[1:]}
            resp = {'user_name': user_name, 'data': data, 'parhead': parhead, 'zts': zts,
                    'sd': sd, 'ed': ed, 'db': db,'ttype': ttype, 'ec_name': ec_name,'hengpan':hengpan}
            return render(rq, respUrl, resp)
        else:
            # 起止日期设置
            ed = datetime.datetime.now() + datetime.timedelta(days=1)
            sd = str(ed - datetime.timedelta(days=8))[:10]
            ed = str(ed)[:10]
            db = 'sql'
            ttype = 'ma60'
            hengpan = 0
            zts = Wave.interval_ma60(sd, ed, database=db)
            ec_name = '以60均线为界'
            data = [[i[0], i[2], i[5], i[4], i[3], i[6]] for i in zts[1:]]
            data = HSD.get_macd(data)  # 计算Macd
            parhead = list(zts[0])  # 字段名称
            zts = {i[0]: list(i) for i in zts[1:]}
            resp = {'user_name': user_name, 'data': data, 'parhead': parhead, 'zts': zts,
                    'sd': sd, 'ed': ed, 'db': db, 'ttype': ttype, 'ec_name': ec_name,'hengpan':hengpan}
            return render(rq, respUrl, resp)

    return redirect('index')


def hqzjzb(rq):
    """ 行情总结指标 """
    user_name, qx = LogIn(rq)
    if rq.method == 'GET':
        sd = rq.GET.get('sd')
        ed = rq.GET.get('ed')
        db = rq.GET.get('db')
        ttype = rq.GET.get('ttype')
        if sd and ed and db and ttype:
            sd = sd[:10]
            ed = ed[:10]
            data = Wave.get_data(sd,ed,database=db)

            ec_name = {
                'extreme':'macd背离','green':'绿异动','red':'红异动',
                'std':'上下引线','1min':'1分钟行情'
            }.get(ttype, '')

            if ttype == '1min':
                data = HSD.get_macd(data, yd=True)
            else:
                ddict = viewUtil.get_hqzjzb(ttype)
                data = HSD.get_macd(data,ddict=ddict)  # 计算Macd
            resp = {'user_name': user_name, 'data': data,
                    'sd': sd, 'ed': ed, 'db': db,'ttype': ttype, 'ec_name': ec_name}
            return render(rq, 'hqzjzb.html', resp)
        else:
            # 起止日期设置
            ed = datetime.datetime.now() + datetime.timedelta(days=1)
            sd = str(ed - datetime.timedelta(days=6))[:10]
            ed = str(ed)[:10]
            db = 'sql'
            ttype = '1min'
            data = Wave.get_data(sd, ed, database=db)
            # ddict = viewUtil.get_hqzjzb(ttype)

            ec_name = '1分钟行情'
            data = HSD.get_macd(data,yd=True)  # 计算Macd
            resp = {'user_name': user_name, 'data': data,
                    'sd': sd, 'ed': ed, 'db': db, 'ttype': ttype, 'ec_name': ec_name}
            return render(rq, 'hqzjzb.html', resp)

    return redirect('index')


def systems(rq):
    user_name, qx = LogIn(rq)
    if not user_name:
        return index(rq, False, user_name, qx)
    return render(rq, 'systems.html', {'user_name': user_name})


def market_news(rq):
    """ 市场快讯 """
    user_name, qx = LogIn(rq)
    return render(rq, 'market_news.html', {'user_name': user_name})


def get_system(rq):
    """ 内存 CPU 使用率 """
    if rq.is_ajax():
        nc = psutil.virtual_memory().percent  # 内存使用率%
        cpu = psutil.cpu_percent(0)  # cup 使用率%
        dt = str(datetime.datetime.now())[11:19]
        zx = {'nc': nc, 'cpu': cpu, 'times': dt}
        return JsonResponse({'zx': zx}, safe=False)
    return page_not_found(rq)


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
