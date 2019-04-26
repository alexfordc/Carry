import time
import json
import pickle
import re
import redis
import datetime
import requests
from pyquery import PyQuery
from threading import Thread

"""
博彩网站数据，套利统计
标准：
[俱乐部友谊赛，'Club Friendlies'，平顺，'Binh Thuan'，前江，'Tien Giang'，'2019-06-06 02:45:00'，
{'1x2':[],'1x21st':[],'dx':[]}, 'https://www.18x8bet.com/zh-cn/sports/3061823/布里斯班狮吼-vs-悉尼']
"""

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
THIS_DATE = str(datetime.datetime.now())[:10]

res1 = []
res2 = []
res3 = []
res4 = []
res5 = []


def get_landing(page=0):
    # url='https://landing-sb.prdasbb18a1.com/en-gb/Service/CentralService?GetData&ts='+str(int(time.time()*1000))
    url = 'https://landing-sb.prdasbb18a1.com/zh-cn/Service/CentralService?GetData&ts=' + str(int(time.time() * 1000))

    ss = {
        'today': '/zh-cn/sports/football/matches-by-date/today/full-time-asian-handicap-and-over-under',  # 今日赛事
        'tomorrow': '/zh-cn/sports/football/matches-by-date/tomorrow/full-time-asian-handicap-and-over-under',  # 明日赛事
        'all': '/zh-cn/sports/football/competition/full-time-asian-handicap-and-over-under',  # 所有赛事
    }

    res_frist = []
    for page in range(30):
        try:
            post_data = {
                'CompetitionID': -1,
                'IsEventMenu': False,
                'IsFirstLoad': True,
                'LiveCenterEventId': 0,
                'LiveCenterSportId': 0,
                'SportID': 1,
                'VersionF': -1,
                'VersionH': 0,
                'VersionL': -1,
                'VersionS': -1,
                'VersionT': -1,
                'VersionU': 0,
                'oIsFirstLoad': True,
                'oIsInplayAll': False,
                'oOddsType': 0,
                'oPageNo': page,
                'oSortBy': 1,
                'reqUrl': ss['all'],
            }

            d = requests.post(url, data=post_data, headers=headers).text
            d = json.loads(d)

            # 获得分类
            # res = []
            # res_dt = {}

            # for i in d['lpd']['psm']['psmd']:
            # 	name = i['sen']
            # 	yname = i['sn']
            # 	for j in i['puc']:
            # 		t_name = j['cn']
            # 		for v in j['ces']:
            # 			res.append((name,yname,t_name,v['at'],v['eid'],v['en'],v['esd'],v['est'],v['ht']))
            # 			res_dt[v['eid']] = (name,yname,t_name,v['at'],v['eid'],v['en'],v['esd'],v['est'],v['ht'])

            res_pl = []

            for i in d['mod']['d']:
                for j in i['c']:
                    t_name = j['n']
                    for v in j['e']:
                        try:
                            x2st = v['o'].get('1x21st', ())
                            if x2st:
                                x2st = (float(x2st[1]), float(x2st[5]), float(x2st[3]))
                            x1x = v['o'].get('1x2', ())
                            if x1x:
                                x1x = (float(x1x[1]), float(x1x[5]), float(x1x[3]))
                            _id = v['k']
                            _z, _k = v['i'][0], v['i'][1]  # 中文名称
                            _yz, _yk = v['i'][-3].replace('-', ' ').split(' vs ')  # 英文名称
                            address = f"https://www.18x8bet.com/zh-cn/sports/{_id}/{_z}-vs-{_k}"
                            edt = str(
                                datetime.datetime.strptime(v['edt'], '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(hours=12))
                            if x1x:
                                res_pl.append((t_name, '', _z, _yz, _k, _yk, edt, {'1x2': x1x, '1x21st': x2st,
                                                                                   'dx': [float(v['o']['ou'][5]),
                                                                                          float(v['o']['ou'][1]),
                                                                                          float(v['o']['ou'][7])],
                                                                                   "hasParlay": v['hasParlay'],
                                                                                   "hide": v['hide'],
                                                                                   "egn": v['egn'],
                                                                                   "heid": v['heid'],
                                                                                   "l": v['l'],
                                                                                   "ibs": v['ibs'],
                                                                                   "ibsc": v['ibsc'],
                                                                                   "page": page},
                                               address))
                        except:
                            pass
            if res_pl and res_frist == res_pl:
                return
            res1.extend(res_pl)
            res_frist = res_pl
        except:
            continue


# print('get_landing Yes........')


def get_marathonbet(page=0):
    res_frist = []

    for page in range(60):
        try:
            url = f'https://www.marathonbet.com/zh/betting/Football/?page={page}&pageAction=getPage&_=' + str(
                int(time.time() * 1000))
            d = requests.get(url).text
            d = json.loads(d)

            d2 = d[0]['content']
            d3 = PyQuery(d2)
            divs = d3('div')

            date_dt = {
                '一月': '01', 'Jan': '01',
                '二月': '02', 'Feb': '02',
                '三月': '03', 'Mar': '03',
                '四月': '04', 'Apr': '04',
                '五月': '05', 'May': '05',
                '六月': '06', 'Jun': '06',
                '七月': '07', 'Jul': '07',
                '八月': '08', 'Aug': '08',
                '九月': '09', 'Sep': '09',
                '十月': '10', 'Oct': '10',
                '十一月': '11', 'Nov': '11',
                '十二月': '12', 'Dec': '12',
            }

            res = []

            for i in divs:
                if i.attrib.get('data-event-page'):
                    # name = i.attrib['data-event-name']
                    _ads = i.attrib['data-event-page']
                    # _a = _ads.split('/')[-2].split('+')
                    address = 'https://www.marathonbet.com' + _ads
                    i = PyQuery(i)
                    trs = i('table tbody tr')
                    # trs=[tr for tr in trs if tr.attrib]
                    for tr in trs:
                        tr = PyQuery(tr)
                        span = tr('span')
                        _date = tr('.date').text()
                        _date = _date.strip()
                        if _date[3:5] in date_dt:
                            _date = str(time.localtime().tm_year) + '-' + date_dt[_date[3:5]] + '-' + _date[
                                                                                                      :2] + ' ' + _date[
                                                                                                                  -5:] + ':00'
                        if len(_date) == 5:
                            _date = THIS_DATE + ' ' + _date + ':00'
                        v = [i.text for i in span]
                        if len(v) > 13:
                            dxz = tr('.total-left')[0].text  # 全场大小球的界限数
                            res.append(
                                ('', '', v[0], '', v[1], '', _date, {'1x2': [float(v[4]), float(v[6]), float(v[5])],
                                                                     '1x21st': [float(v[11]), float(v[13]),
                                                                                float(v[12])],
                                                                     'dx': [float(v[9]), float(dxz.split(',')[0]),
                                                                            float(v[10])]}, address))
            if res_frist == res:
                break
            res2.extend(res)
            res_frist = res
        except Exception as exc:
            pass
        # print(exc)


# print('get_marathonbet Yes........')


def get_bwin9828():
    bsUrl = 'https://www.bwin9828.com'
    url = 'https://www.bwin9828.com/zh-cn/sport/football'
    h = requests.get(url, headers=headers).text
    h2 = PyQuery(h)
    h3 = h2('.coupon-homepage__group-link')

    res_dict = {}
    for i in h3:
        try:
            title = i.attrib['title']
            n_url = bsUrl + i.attrib['href']
            nh = requests.get(n_url, headers=headers).text
            nh2 = PyQuery(nh)
            divs = nh2('.multiple_markets')

            for div in divs:
                div = PyQuery(div)
                qcbc = div('h4')[0].attrib.get('title', '')
                if '90分钟' in qcbc:
                    qcbc = '1x2'
                elif '上半场' in qcbc:
                    qcbc = '1x21st'
                if qcbc in {'1x2', '1x21st'}:
                    nh3 = div('.body')
                    for j in nh3:
                        try:
                            if j.attrib.get('data-market-cash-out-elegible'):
                                j = PyQuery(j)
                                tds = j('td')
                                td0 = PyQuery(tds[0])
                                span = td0('span')
                                dts = str(datetime.datetime.fromtimestamp(int(span.attr('data-time')) / 1000))
                                td1 = PyQuery(tds[1])
                                a = td1('a')
                                n_title = a.attr('title')
                                href = a.attr('href')
                                td2 = PyQuery(tds[2])
                                span2 = td2('span')
                                price_1 = float(span2.attr('data-price'))
                                td3 = PyQuery(tds[3])
                                span3 = td3('span')
                                price_x = float(span3.attr('data-price'))
                                td4 = PyQuery(tds[4])
                                span4 = td4('span')
                                price_2 = float(span4.attr('data-price'))
                                name = n_title.split('v')
                                name0 = name[0].strip()
                                name1 = name[1].strip()
                                _key = f"{name0}{dts}{name1}"
                                if _key not in res_dict:
                                    res_dict[_key] = [title, '', name0, '', name1, '', dts,
                                                      {qcbc: [price_1, price_x, price_2], 'dx': []}, bsUrl + href]
                                else:
                                    res_dict[_key][7][qcbc] = [price_1, price_x, price_2]
                            # res3.append((title,name[0].strip(),name[1].strip(),dts,price_1,price_x,price_2,qcbc,bsUrl+href))
                        except Exception as exc:
                            pass
                        # print(exc)
        except:
            continue
    [res3.append(tuple(res_dict[i])) for i in res_dict]


# print('get_bwin9828 Yes........')


def get_1xbet888():
    baseUrl = 'https://1xbet888.com/line/Football/'
    url = 'https://1xbet888.com/LineFeed/Get1x2_VZip?sports=1&count=500&lng=cn&tf=2200000&tz=8&mode=4&country=90&partner=57&getEmpty=true'
    h = requests.get(url)
    h = h.text
    h = json.loads(h)

    for i in h['Value']:
        try:
            jlb = i['LE'].replace('.', '')
            ynz = i['O1E'].replace('.', '')
            ynk = i['O2E'].replace('.', '')
            ie = i['E']
            ie.sort(key=lambda x: x['T'])
            _1x2 = [ie[0]['C'], ie[1]['C'], ie[2]['C']]
            if len(ie) > 3:
                _dx = [ie[8]['C'], ie[8]['P'], ie[9]['C']]
            else:
                _dx = []
            address = f"{baseUrl}{i['LI']}-{jlb.replace(' ','-')}/{i['CI']}-{ynz.replace(' ','-')}-{ynk.replace(' ','-')}"
            res4.append((i['L'], jlb, i['O1'], ynz, i['O2'], ynk, str(datetime.datetime.fromtimestamp(i['S'])),
                         {'1x2': _1x2, '1x21st': [], 'dx': _dx},
                         address))
        except Exception as exc:
            pass
        # print(exc)


# print('get_1xbet888 Yes........')


def get_bet365():
    baseUrl = ''
    url = 'https://www.bet365.com/SportsBook.API/web?lid=10&zid=0&pd=%23AS%23B1%23&cid=198&ctid=198'
    url2 = 'https://www.bet365.com/SportsBook.API/web?lid=10&zid=0&pd=%23AC%23B1%23C1%23D14%23E406%23F16%23&cid=198&ctid=198'

    h = requests.get(url, headers=headers).text
    com = re.compile(r'PD=(.*?);')
    url_id = re.findall(com, h)
    url_id = [f"https://www.bet365.com/SportsBook.API/web?lid=10&zid=0&pd={i.replace('#','%23')}&cid=198&ctid=198" for i
              in url_id if len(i) == 23]


def hlzh(x, y, z):
    """ 盈利空间 与 投注比例计算 """
    # if isinstance(x,str):
    # 	x = float(x)
    # if isinstance(y,str):
    # 	y = float(y)
    # if isinstance(z,str):
    # 	z = float(z)

    cou = x + y + z
    x1, y1, z1 = cou / x, cou / y, cou / z
    cou1 = x1 + y1 + z1
    x2, y2, z2 = round(x1 / cou1 * 100, 3), round(y1 / cou1 * 100, 3), round(z1 / cou1 * 100, 3)

    return round(x * x2 / (x2 + y2 + z2), 3), (x2, y2, z2)


def gt_rs(rss, key, ks='1x2'):
    c1 = {i[key][7][ks][0]: (i[key][-1], i[key][2], i[key][4], i[key][0]) for i in rss if key in i and i[key][7].get(ks)}
    cx = {i[key][7][ks][1]: (i[key][-1], i[key][2], i[key][4], i[key][0]) for i in rss if key in i and i[key][7].get(ks)}
    c2 = {i[key][7][ks][2]: (i[key][-1], i[key][2], i[key][4], i[key][0]) for i in rss if key in i and i[key][7].get(ks)}
    return c1, cx, c2

def write_name(ress):
    names = {}
    for res in ress:
        for i in res:
            if i[3] not in names:
                names[i[3]] = i[2]
            if i[5] not in names:
                names[i[5]] = i[4]
    with open('mysite\\namess.json','w') as f:
        f.write(json.dumps(names))

def js_1x2(ks='1x2'):
    res = []
    r1 = {f"{i[2]}+{i[6]}": i for i in res1 if i[-2]['ibsc']}
    r2 = {f"{i[2]}+{i[6]}": i for i in res2}
    r3 = {f"{i[2]}+{i[6]}": i for i in res3}
    r4 = {f"{i[2]}+{i[6]}": i for i in res4}
    rss = set(list(r1) + list(r2) + list(r3) + list(r4))
    for i in rss:
        c1, cx, c2 = gt_rs((r1, r2, r3, r4), i, ks)
        if len(c1) > 1:
            mc1, mcx, mc2 = max(c1), max(cx), max(c2)
            gl, zh = hlzh(mc1, mcx, mc2)
            jg = (gl, (mc1, mcx, mc2), zh, (c1[mc1], cx[mcx], c2[mc2]), i.split('+')[-1])
            res.append(jg)

    r1 = {f"{i[4]}+{i[6]}": i for i in res1 if i[-2]['ibsc']}
    r2 = {f"{i[4]}+{i[6]}": i for i in res2}
    r3 = {f"{i[4]}+{i[6]}": i for i in res3}
    r4 = {f"{i[4]}+{i[6]}": i for i in res4}
    rss = set(list(r1) + list(r2) + list(r3) + list(r4))
    for i in rss:
        c1, cx, c2 = gt_rs((r1, r2, r3, r4), i, ks)
        if len(c1) > 1:
            mc1, mcx, mc2 = max(c1), max(cx), max(c2)
            gl, zh = hlzh(mc1, mcx, mc2)
            jg = (gl, (mc1, mcx, mc2), zh, (c1[mc1], cx[mcx], c2[mc2]), i.split('+')[-1])
            if jg not in res:
                res.append(jg)

    res.sort(key=lambda x: x[0])
    res.reverse()

    return res


def runs():
    global res1, res2, res3, res4, res5
    res1, res2, res3, res4, res5 = [], [], [], [], []

    red = redis.Redis()
    red.set('is_bookmaker_odds', pickle.dumps(1), ex=600)

    t1 = Thread(target=get_landing)
    t2 = Thread(target=get_marathonbet)
    t3 = Thread(target=get_bwin9828)
    t4 = Thread(target=get_1xbet888)

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()


    res_1x2 = js_1x2('1x2')[:50]
    res_1x21st = js_1x2('1x21st')[:50]

    write_name((res1, res2, res3, res4))

    res = {
        'bet_1x2': res_1x2,
        'bet_1x21st': res_1x21st,
        'time': time.time(),
    }

    red.set('is_bookmaker_odds', pickle.dumps(0), ex=600)
    red.set('bookmaker_odds', pickle.dumps(res))


def main():
    t = Thread(target=runs)
    t.start()
