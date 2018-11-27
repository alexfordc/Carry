import datetime

from mysite import HSD


def get_data(st, ed, database):
    if database == 'mongodb':
        mongo = HSD.MongoDBData(db='HKFuture', table='future_1min')
        data = tuple(mongo.get_hsi(st, ed))
    else:
        sql = ("SELECT DATETIME,OPEN,high,low,CLOSE,vol FROM wh_same_month_min "
               f"WHERE prodcode='HSI' AND DATETIME>='{st}' AND DATETIME<='{ed}' ORDER BY DATETIME")
        data = HSD.runSqlData('carry_investment', sql)
    return data


def interval_ma60(st=None, ed=None, database='mongodb', hengpan=0):
    """
    以60均线分波
    :param st: 开始日期
    :param ed: 终止日期
    :param database: 数据库类型
    :return: 计算结果列表
    """
    short, long, phyd = 12, 26, 9
    if st is None or ed is None:
        st = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
        ed = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    data = get_data(st, ed, database)
    cou = []
    zts = [('开始时间', '结束时间', '开盘', '最高', '最低', '收盘', '成交量', '60均线上/下方(1/0)', 'K线数量',
            '涨/跌趋势(+/-)', '此波幅度', 'macd绿区', 'macd红区', '异动小于-1.5倍', '异动大于1.5倍')]
    dc2 = [i[4] for i in data]
    dc = []
    yddy, ydxy = 0, 0  # 异动
    _vol = 0  # 成交量

    def get_cou():
        st = cou[-2][0]
        ed = cou[-1][0]
        _O = dc2[st]
        _H = max(dc2[st:ed + 1])
        _L = min(dc2[st:ed + 1])
        _C = dc2[ed]
        jc = _H - _L
        if hengpan and jc < hengpan:
            return None
        zt = '+' if dc2[st:ed + 1].index(_H) > int((ed - st) / 2) else '-'
        # if 1: #jc > 50:
        _dc = dc[st:ed + 1]
        _macdg = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _dc[_m]['macd'] < 0) or (_dc[_m]['macd'] < 0 and _dc[_m - 1]['macd'] > 0)])
        _macdr = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _dc[_m]['macd'] > 0) or (_dc[_m]['macd'] > 0 and _dc[_m - 1]['macd'] < 0)])

        return (
            str(data[st][0]), str(data[i][0]), _O, _H, _L, _C, _vol, cou[-1][1], ed - st, zt, jc, _macdg, _macdr, ydxy,
            yddy)

    lend_ = len(data) - 1
    for i, (d, o, h, l, c, v) in enumerate(data):
        dc.append({'ema_short': 0, 'ema_long': 0, 'diff': 0, 'dea': 0, 'macd': 0,
                   'var': 0,  # 方差
                   'std': 0,  # 标准差
                   'mul': 0,  # 异动
                   })
        if i == 1:
            ac = dc2[i - 1]
            this_c = dc2[i]
            dc[i]['ema_short'] = ac + (this_c - ac) * 2 / short
            dc[i]['ema_long'] = ac + (this_c - ac) * 2 / long
            # dc[i]['ema_short'] = sum([(short-j)*da[i-j][4] for j in range(short)])/(3*short)
            # dc[i]['ema_long'] = sum([(long-j)*da[i-j][4] for j in range(long)])/(3*long)
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
            # co = 1 if dc[i]['macd'] >= 0 else 0
        elif i > 1:
            n_c = dc2[i]
            dc[i]['ema_short'] = dc[i - 1]['ema_short'] * (short - 2) / short + n_c * 2 / short
            dc[i]['ema_long'] = dc[i - 1]['ema_long'] * (long - 2) / long + n_c * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i - 1]['dea'] * (phyd - 2) / phyd + dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])

        if i >= 60:
            _m60 = round(sum(j for j in dc2[i - 59:i + 1]) / 60)

            ma = 60
            std_pj = sum(dc2[i - j] - data[i - j][1] for j in range(ma)) / ma
            dc[i]['var'] = sum((dc2[i - j] - data[i - j][1] - std_pj) ** 2 for j in range(ma)) / ma  # 方差 i-ma+1,i+1
            dc[i]['std'] = float(dc[i]['var'] ** 0.5)  # 标准差
            price = c - o
            dc[i]['mul'] = _yd = round(price / dc[i]['std'], 2)
            if _yd > 1.5:
                yddy += 1
            elif _yd < -1.5:
                ydxy += 1
            _vol += v
            if c >= _m60:
                if cou and cou[-1][1] != 0:  # and not is_bs(dc[i-10:i+1],_m60,'<'):
                    cou.append((i, 0))
                    _getCou = get_cou()
                    yddy, ydxy = 0, 0
                    _vol = 0
                    if _getCou:
                        zts.append(_getCou)
                elif not cou:
                    cou.append((i, 0))
                elif i == lend_:  # data[i][0].day != data[i - 1][0].day or (data[i][0].day == data[i - 1][0].day and str(data[i][0])[11:16] == '09:15') or
                    cou.append((i, 0))
                    _getCou = get_cou()
                    yddy, ydxy = 0, 0
                    _vol = 0
                    if _getCou:
                        zts.append(_getCou)
            else:
                if cou and cou[-1][1] != 1:  # and not is_bs(dc[i-10:i+1],_m60,'>'):
                    cou.append((i, 1))
                    _getCou = get_cou()
                    yddy, ydxy = 0, 0
                    _vol = 0
                    if _getCou:
                        zts.append(_getCou)
                elif not cou:
                    cou.append((i, 1))
                elif i == lend_:  # data[i][0].day != data[i - 1][0].day or (data[i][0].day == data[i - 1][0].day and str(data[i][0])[11:16] == '09:15') or
                    cou.append((i, 1))
                    _getCou = get_cou()
                    yddy, ydxy = 0, 0
                    _vol = 0
                    if _getCou:
                        zts.append(_getCou)

    return zts


def interval_macd(st=None, ed=None, database='mongodb', hengpan=0):
    """
        以 MACD 分波
        :param st: 开始日期
        :param ed: 终止日期
        :param database: 数据库类型
        :return: 计算结果列表
        """
    short, long, phyd = 12, 26, 9
    if st is None or ed is None:
        st = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
        ed = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    data = get_data(st, ed, database)
    cou = []
    zts = [('开始时间', '结束时间', '开盘', '最高', '最低', '收盘', '成交量', 'macd红/绿区(1/0)', 'K线数量',
            '涨/跌趋势(+/-)', '此波幅度', '60均线下方', '60均线上方', '异动小于-1.5倍', '异动大于1.5倍')]
    dc2 = [i[4] for i in data]
    dc = []
    yddy, ydxy = 0, 0  # 异动
    _vol = 0  # 成交量
    _m60 = 0  # 60均线

    def get_cou():
        st = cou[-2][0]
        ed = cou[-1][0]
        _O = dc2[st]
        _H = max(dc2[st:ed + 1])
        _L = min(dc2[st:ed + 1])
        _C = dc2[ed]
        jc = _H - _L
        zt = '+' if dc2[st:ed + 1].index(_H) > int((ed - st) / 2) else '-'
        # if 1: #jc > 50:
        _dc = dc[st:ed + 1]
        # _ma60g = len([_m for _m in range(len(_dc)) if _C <= _m60])
        _ma60g = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _C <= _dc[_m]['ma60']) or (_C <= _dc[_m]['ma60'] and dc2[st] > _dc[_m - 1]['ma60'])])
        _ma60r = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _C > _dc[_m]['ma60']) or (_C > _dc[_m]['ma60'] and dc2[st] <= _dc[_m - 1]['ma60'])])

        return (
            str(data[st][0]), str(data[i][0]), _O, _H, _L, _C, _vol, cou[-1][1], ed - st, zt, jc, _ma60g, _ma60r, ydxy,
            yddy)

    macd = None
    lend_ = len(data) - 1
    for i, (d, o, h, l, c, v) in enumerate(data):
        dc.append({'ema_short': 0, 'ema_long': 0, 'diff': 0, 'dea': 0, 'macd': 0,
                   'var': 0,  # 方差
                   'std': 0,  # 标准差
                   'mul': 0,  # 异动
                   'ma60': 0,  # 60均线
                   })
        if i == 1:
            ac = dc2[i - 1]
            this_c = dc2[i]
            dc[i]['ema_short'] = ac + (this_c - ac) * 2 / short
            dc[i]['ema_long'] = ac + (this_c - ac) * 2 / long
            # dc[i]['ema_short'] = sum([(short-j)*da[i-j][4] for j in range(short)])/(3*short)
            # dc[i]['ema_long'] = sum([(long-j)*da[i-j][4] for j in range(long)])/(3*long)
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
            # co = 1 if dc[i]['macd'] >= 0 else 0
        elif i > 1:
            n_c = dc2[i]
            dc[i]['ema_short'] = dc[i - 1]['ema_short'] * (short - 2) / short + n_c * 2 / short
            dc[i]['ema_long'] = dc[i - 1]['ema_long'] * (long - 2) / long + n_c * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i - 1]['dea'] * (phyd - 2) / phyd + dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])

        dc[i]['ma60'] = _m60 = round(sum(j for j in dc2[i - 59:i + 1]) / 60) if i >= 60 else round(
            sum(j for j in dc2[0:i + 1]) / 60)

        ma = 60
        std_pj = sum(dc2[i - j] - data[i - j][1] for j in range(ma)) / ma
        dc[i]['var'] = sum((dc2[i - j] - data[i - j][1] - std_pj) ** 2 for j in range(ma)) / ma  # 方差 i-ma+1,i+1
        dc[i]['std'] = float(dc[i]['var'] ** 0.5)  # 标准差
        price = c - o
        dc[i]['mul'] = _yd = round(price / dc[i]['std'], 2)
        if _yd > 1.5:
            yddy += 1
        elif _yd < -1.5:
            ydxy += 1
        _vol += v
        _macd = 1 if dc[i]['macd'] > 0 else 0
        judge = i == lend_  # data[i][0].day != data[i - 1][0].day or (data[i][0].day == data[i - 1][0].day and str(data[i][0])[11:16] == '09:15') or
        if macd != _macd or judge:
            macd = _macd
            if macd > 0:
                if cou and cou[-1][1] != 0:  # and not is_bs(dc[i-10:i+1],_m60,'<'):
                    cou.append((i, 0))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0
                elif not cou:
                    cou.append((i, 0))
                elif judge:
                    cou.append((i, 0))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0
            else:
                if cou and cou[-1][1] != 1:  # and not is_bs(dc[i-10:i+1],_m60,'>'):
                    cou.append((i, 1))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0
                elif not cou:
                    cou.append((i, 1))
                elif judge:
                    cou.append((i, 1))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0
    return zts


def interval_change(st=None, ed=None, database='mongodb', hengpan=0):
    """
        以异动分波
        :param st: 开始日期
        :param ed: 终止日期
        :param database: 数据库类型
        :return: 计算结果列表
        """
    short, long, phyd = 12, 26, 9
    if st is None or ed is None:
        st = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
        ed = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    data = get_data(st, ed, database)
    cou = []
    zts = [('开始时间', '结束时间', '开盘', '最高', '最低', '收盘', '成交量', '异动正/反(1/0)', 'K线数量',  # 60均线上/下方(1/0)
            '涨/跌趋势(+/-)', '此波幅度', 'macd绿区', 'macd红区', '异动小于-1.5倍', '异动大于1.5倍')]
    dc2 = [i[4] for i in data]
    dc = []
    yddy, ydxy = 0, 0  # 异动
    _vol = 0  # 成交量

    def get_cou():
        st = cou[-2][0]
        ed = cou[-1][0]
        _O = dc2[st]
        _H = max(dc2[st:ed + 1])
        _L = min(dc2[st:ed + 1])
        _C = dc2[ed]
        jc = _H - _L
        zt = '+' if dc2[st:ed + 1].index(_H) > int((ed - st) / 2) else '-'
        # if 1: #jc > 50:
        _dc = dc[st:ed + 1]
        _macdg = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _dc[_m]['macd'] < 0) or (_dc[_m]['macd'] < 0 and _dc[_m - 1]['macd'] > 0)])
        _macdr = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _dc[_m]['macd'] > 0) or (_dc[_m]['macd'] > 0 and _dc[_m - 1]['macd'] < 0)])

        return (
            str(data[st][0]), str(data[i][0]), _O, _H, _L, _C, _vol, cou[-1][1], ed - st, zt, jc, _macdg, _macdr, ydxy,
            yddy)

    lend_ = len(data) - 1
    for i, (d, o, h, l, c, v) in enumerate(data):
        dc.append({'ema_short': 0, 'ema_long': 0, 'diff': 0, 'dea': 0, 'macd': 0,
                   'var': 0,  # 方差
                   'std': 0,  # 标准差
                   'mul': 0,  # 异动
                   })
        if i == 1:
            ac = dc2[i - 1]
            this_c = dc2[i]
            dc[i]['ema_short'] = ac + (this_c - ac) * 2 / short
            dc[i]['ema_long'] = ac + (this_c - ac) * 2 / long
            # dc[i]['ema_short'] = sum([(short-j)*da[i-j][4] for j in range(short)])/(3*short)
            # dc[i]['ema_long'] = sum([(long-j)*da[i-j][4] for j in range(long)])/(3*long)
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
            # co = 1 if dc[i]['macd'] >= 0 else 0
        elif i > 1:
            n_c = dc2[i]
            dc[i]['ema_short'] = dc[i - 1]['ema_short'] * (short - 2) / short + n_c * 2 / short
            dc[i]['ema_long'] = dc[i - 1]['ema_long'] * (long - 2) / long + n_c * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i - 1]['dea'] * (phyd - 2) / phyd + dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])

        if i >= 60:
            # _m60 = round(sum(j for j in dc2[i - 59:i + 1]) / 60)

            ma = 60
            std_pj = sum(dc2[i - j] - data[i - j][1] for j in range(ma)) / ma
            dc[i]['var'] = sum((dc2[i - j] - data[i - j][1] - std_pj) ** 2 for j in range(ma)) / ma  # 方差 i-ma+1,i+1
            dc[i]['std'] = dc[i]['var'] ** 0.5  # 标准差
            price = c - o
            dc[i]['mul'] = _yd = round(price / dc[i]['std'], 2)
            if _yd > 1.5:
                yddy += 1
            elif _yd < -1.5:
                ydxy += 1
            _vol += v
            if _yd > 1.5:
                if cou and cou[-1][1] != 0:  # and not is_bs(dc[i-10:i+1],_m60,'<'):
                    cou.append((i, 0))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0
                elif not cou:
                    cou.append((i, 0))
                elif i == lend_:  # data[i][0].day != data[i - 1][0].day or (data[i][0].day == data[i - 1][0].day and str(data[i][0])[11:16] == '09:15') or
                    cou.append((i, 0))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0
            elif _yd < -1.5:
                if cou and cou[-1][1] != 1:  # and not is_bs(dc[i-10:i+1],_m60,'>'):
                    cou.append((i, 1))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0
                elif not cou:
                    cou.append((i, 1))
                elif i == lend_:  # data[i][0].day != data[i - 1][0].day or (data[i][0].day == data[i - 1][0].day and str(data[i][0])[11:16] == '09:15') or
                    cou.append((i, 1))
                    zts.append(get_cou())
                    yddy, ydxy = 0, 0
                    _vol = 0

    return zts


def interval_yinyang(st=None, ed=None, database='mongodb', hengpan=0):
    """
        以阴阳线分波
        :param st: 开始日期
        :param ed: 终止日期
        :param database: 数据库类型
        :return: 计算结果列表
        """
    short, long, phyd = 12, 26, 9
    if st is None or ed is None:
        st = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
        ed = str(datetime.datetime.now() + datetime.timedelta(days=1))[:10]
    data = get_data(st, ed, database)
    cou = []
    zts = [('开始时间', '结束时间', '开盘', '最高', '最低', '收盘', '成交量', '60均线下方', '60均线上方', 'K线数量',
            '涨/跌趋势(+/-)', '此波幅度', 'macd绿区', 'macd红区', '异动小于-1.5倍', '异动大于1.5倍')]
    dc2 = [i[4] for i in data]
    dc = []
    yddy, ydxy = 0, 0  # 异动
    _vol = 0  # 成交量

    def get_cou():
        st = cou[-2][0]
        ed = cou[-1][0]
        _O = dc2[st]
        _H = max(dc2[st:ed + 1])
        _L = min(dc2[st:ed + 1])
        _C = dc2[ed]
        jc = _H - _L
        zt = '+' if dc2[st:ed + 1].index(_H) > int((ed - st) / 2) else '-'
        # if 1: #jc > 50:
        _dc = dc[st:ed + 1]
        _ma60g = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _C <= _dc[_m]['ma60']) or (_C <= _dc[_m]['ma60'] and dc2[st] > _dc[_m - 1]['ma60'])])
        _ma60r = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _C > _dc[_m]['ma60']) or (_C > _dc[_m]['ma60'] and dc2[st] <= _dc[_m - 1]['ma60'])])

        _macdg = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _dc[_m]['macd'] < 0) or (_dc[_m]['macd'] < 0 and _dc[_m - 1]['macd'] > 0)])
        _macdr = len([_m for _m in range(len(_dc)) if
                      (_m == 0 and _dc[_m]['macd'] > 0) or (_dc[_m]['macd'] > 0 and _dc[_m - 1]['macd'] < 0)])

        return (
            str(data[st][0]), str(data[i][0]), _O, _H, _L, _C, _vol, _ma60g, _ma60r, ed-st, zt, jc, _macdg, _macdr, ydxy,
            yddy)

    lend_ = len(data) - 1
    for i, (d, o, h, l, c, v) in enumerate(data):
        dc.append({'ema_short': 0, 'ema_long': 0, 'diff': 0, 'dea': 0, 'macd': 0,
                   'var': 0,  # 方差
                   'std': 0,  # 标准差
                   'mul': 0,  # 异动
                   'ma60': 0,  # 60均线
                   })
        if i == 1:
            ac = dc2[i - 1]
            this_c = dc2[i]
            dc[i]['ema_short'] = ac + (this_c - ac) * 2 / short
            dc[i]['ema_long'] = ac + (this_c - ac) * 2 / long
            # dc[i]['ema_short'] = sum([(short-j)*da[i-j][4] for j in range(short)])/(3*short)
            # dc[i]['ema_long'] = sum([(long-j)*da[i-j][4] for j in range(long)])/(3*long)
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
            # co = 1 if dc[i]['macd'] >= 0 else 0
        elif i > 1:
            n_c = dc2[i]
            dc[i]['ema_short'] = dc[i - 1]['ema_short'] * (short - 2) / short + n_c * 2 / short
            dc[i]['ema_long'] = dc[i - 1]['ema_long'] * (long - 2) / long + n_c * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i - 1]['dea'] * (phyd - 2) / phyd + dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
        dc[i]['ma60'] = _m60 = round(sum(j for j in dc2[i - 59:i + 1]) / 60) if i >= 60 else round(
            sum(j for j in dc2[0:i + 1]) / 60)
        price = c - o
        if i >= 60:
            # _m60 = round(sum(j for j in dc2[i - 59:i + 1]) / 60)
            ma = 60
            std_pj = sum(dc2[i - j] - data[i - j][1] for j in range(ma)) / ma
            dc[i]['var'] = sum((dc2[i - j] - data[i - j][1] - std_pj) ** 2 for j in range(ma)) / ma  # 方差 i-ma+1,i+1
            dc[i]['std'] = dc[i]['var'] ** 0.5  # 标准差

            dc[i]['mul'] = _yd = round(price / dc[i]['std'], 2)
            if _yd > 1.5:
                yddy += 1
            elif _yd < -1.5:
                ydxy += 1
        _vol += v
        if price > 0:
            if cou and cou[-1][1] != 0:  # and not is_bs(dc[i-10:i+1],_m60,'<'):
                cou.append((i, 0))
                zts.append(get_cou())
                yddy, ydxy = 0, 0
                _vol = 0
            elif not cou:
                cou.append((i, 0))
            elif i == lend_:  # data[i][0].day != data[i - 1][0].day or (data[i][0].day == data[i - 1][0].day and str(data[i][0])[11:16] == '09:15') or
                cou.append((i, 0))
                zts.append(get_cou())
                yddy, ydxy = 0, 0
                _vol = 0
        else:
            if cou and cou[-1][1] != 1:  # and not is_bs(dc[i-10:i+1],_m60,'>'):
                cou.append((i, 1))
                zts.append(get_cou())
                yddy, ydxy = 0, 0
                _vol = 0
            elif not cou:
                cou.append((i, 1))
            elif i == lend_:  # data[i][0].day != data[i - 1][0].day or (data[i][0].day == data[i - 1][0].day and str(data[i][0])[11:16] == '09:15') or
                cou.append((i, 1))
                zts.append(get_cou())
                yddy, ydxy = 0, 0
                _vol = 0
    return zts


def to_change(data):
    data.pop(0)
    res = []
    dc = []
    dc2 = [i[5] for i in data]
    short, long, phyd = 12, 26, 9
    for i,(d,_et,o,h,l,c,*_) in enumerate(data):
        dc.append({'ema_short': 0, 'ema_long': 0, 'diff': 0, 'dea': 0, 'macd': 0,
                   # 'var': 0,  # 方差
                   # 'std': 0,  # 标准差
                   # 'mul': 0,  # 异动
                   })
        if i == 1:
            ac = data[i - 1][5]
            dc[i]['ema_short'] = ac + (c - ac) * 2 / short
            dc[i]['ema_long'] = ac + (c - ac) * 2 / long
            # dc[i]['ema_short'] = sum([(short-j)*da[i-j][4] for j in range(short)])/(3*short)
            # dc[i]['ema_long'] = sum([(long-j)*da[i-j][4] for j in range(long)])/(3*long)
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])
            # co = 1 if dc[i]['macd'] >= 0 else 0
        elif i > 1:
            dc[i]['ema_short'] = dc[i - 1]['ema_short'] * (short - 2) / short + c * 2 / short
            dc[i]['ema_long'] = dc[i - 1]['ema_long'] * (long - 2) / long + c * 2 / long
            dc[i]['diff'] = dc[i]['ema_short'] - dc[i]['ema_long']
            dc[i]['dea'] = dc[i - 1]['dea'] * (phyd - 2) / phyd + dc[i]['diff'] * 2 / phyd
            dc[i]['macd'] = 2 * (dc[i]['diff'] - dc[i]['dea'])

        if i >= 60:
            ma = 60
            std_pj = sum(dc2[i - j] - data[i - j][2] for j in range(ma)) / ma
            # dc[i]['var'] = sum((dc2[i - j] - data[i - j][2] - std_pj) ** 2 for j in range(ma)) / ma  # 方差 i-ma+1,i+1
            # dc[i]['std'] = dc[i]['var'] ** 0.5  # 标准差
            var = sum((dc2[i - j] - data[i - j][2] - std_pj) ** 2 for j in range(ma)) / ma  # 方差 i-ma+1,i+1
            std = var ** 0.5  # 标准差
            price = c - o
            # dc[i]['mul'] = _yd = round(price / dc[i]['std'], 2)
            _yd = round(price / std, 2)
        else:
            _yd = 0
        res.append((_et, _yd, o < c))
    return res