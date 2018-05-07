"""Carry URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.8/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Add an import:  from blog import urls as blog_urls
    2. Add a URL to urlpatterns:  url(r'^blog/', include(blog_urls))
"""
from django.conf.urls import include,url
from django.contrib import admin
from mysite import views

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),
    url(r'^$',views.index,name='index'),
    url(r'^stockData/$',views.stockData,name='stockData'),
    url(r'^stockDatas/$',views.stockDatas,name='stockDatas'),
    url(r'^updates/$',views.redis_update),
    url(r'^min/$',views.showPicture,name='showPicture'),
    url(r'^getData/$',views.getData,name='getData'),
    url(r'^zt/$',views.zhutu,name='zhutu'),
    url(r'^zx/$',views.zhexian,name='zhexian'),
    url(r'^tj/$',views.tongji,name='tongji'),
    url(r'^ts/$',views.tools,name='tools'),

    url(r'^zhutu2/$',views.zhutu2,name='zhutu2'),

    url(r'^kline/$',views.kline,name='kline'),
    url(r'^getkline$',views.getkline),
    url(r'^getwebsocket$',views.getwebsocket),
    url(r'^zhangting/([a-z]*)/$',views.zhangting,name='zhangting'),
    url(r'^moni/$',views.moni,name='moni'),
    url(r'^bfsy/$',views.bfsy,name='bfsy'),
    url(r'^gdzd/$',views.gdzd,name='gdzd'),
    url(r'^hc/$',views.huice,name='huice'),

]

# handler404 = views.page_not_found #404页面