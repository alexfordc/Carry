import sys
import datetime
import json

from threading import Thread

from mysite import HSD



def asyncs(func):
    """ 执行不需要返回结果的程序 """
    def wrapper(*args,**kwargs):
        t = Thread(target=func,args=args,kwargs=kwargs)
        t.start()
    return wrapper

@asyncs
def record_log(files,info,types):
    """ 访问日志 """
    if types == 'w':
        with open(files,'w') as f:
            f.write(json.dumps(info))
    elif types == 'a':
        with open(files, 'a') as f:
            f.write(info)

def error_log(files,line,exc):
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
        HSD.logging.error("文件：{} 第{}行报错： {}".format(sys.argv[0],sys._getframe().f_lineno, exc))

    return herys

def tongji_ud(page,rq_date,end_date):
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
def gxjy_refresh(h,folder1,folder2):
    """ 国信国内期货交易刷新 """
    data = h.gx_lsjl(folder1)
    data = data.fillna('')
    data = data.sort_values(['日期', '成交时间'])
    h.to_sql(data, 'gx_record')
    data = h.gx_lsjl(folder2)
    h.to_sql(data, 'gx_entry_exit')