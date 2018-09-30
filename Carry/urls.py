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
from django.conf.urls import include, url
from django.contrib import admin
# from django.urls import path
from mysite import views

"""

from django.contrib import admin
from django.urls import path
from firstapp import views
 
urlpatterns = [
    path('admin/', admin.site.urls),
    path('hello/',views.hello)
]
"""

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),
    url(r'^$', views.index, name='index'),

    # 用户相关操作
    url(r'^user/login/$', views.login, name='login'),
    url(r'^user/logout/$', views.logout, name='logout'),
    url(r'^user/register/$', views.register, name='register'),
    url(r'^update_data/$', views.update_data, name='update_data'),
    url(r'^work/log/add/$', views.add_work_log, name='add_work_log'),
    url(r'^simulation/account/add/$', views.add_simulation_account, name='add_simulation_account'),
    url(r'^real/account/add/$', views.add_real_account, name='add_real_account'),
    url(r'^user/information/$', views.user_information, name='user_information'),
    url(r'^work/log/update/$', views.update_work_log, name='update_work_log'),
    url(r'^work/log/delete/$', views.del_work_log, name='del_work_log'),
    url(r'^simulation/account/delete/$', views.del_simulation_account, name='del_simulation_account'),
    url(r'^simulation/account/offon/$', views.offon_simulation_account, name='offon_simulation_account'),
    url(r'^user/info/public/add/$', views.user_info_public, name='user_info_public'),
    url(r'^user/info/public/show/$', views.user_info_public_show, name='user_info_public_show'),
    url(r'^user/info/public/update/$', views.user_info_public_update, name='user_info_public_update'),
    url(r'^user/info/public/delete/$', views.user_info_public_delete, name='user_info_public_delete'),
    url(r'^user/info/public/reply/$', views.user_info_public_reply, name='user_info_public_reply'),
    url(r'^user/info/public/replyDel/$', views.user_info_public_replyDel, name='user_info_public_replyDel'),
    url(r'^real/account/del/$', views.del_real_account, name='del_real_account'),
    url(r'^user/update/info/$', views.user_update_info, name='user_update_info'),
    url(r'^user/cloud/public/$', views.user_cloud_public, name='user_cloud_public'),
    url(r'^user/cloud/public/download$', views.user_cloud_public_download, name='user_cloud_public_download'),
    url(r'^user/cloud/public/delete', views.user_cloud_public_delete, name='user_cloud_public_delete'),

    url(r'^stockData/$', views.stockData, name='stockData'),
    url(r'^stockDatas/$', views.stockDatas, name='stockDatas'),
    url(r'^updates/$', views.redis_update),
    url(r'^min/$', views.showPicture, name='showPicture'),
    url(r'^getData/$', views.getData, name='getData'),
    url(r'^zt/$', views.zhutu, name='zhutu'),
    url(r'^zx/$', views.zhexian, name='zhexian'),
    url(r'^tj/$', views.tongji, name='tongji'),
    url(r'^tongji_update_del/$', views.tongji_update_del, name='tongji_update_del'),
    url(r'^ts/$', views.tools, name='tools'),
    url(r'^captcha/', include('captcha.urls')),
    url(r'^zhutu2/$', views.zhutu2, name='zhutu2'),

    url(r'^kline/$', views.kline, name='kline'),
    url(r'^getkline$', views.getkline),
    url(r'^getwebsocket$', views.getwebsocket),
    url(r'^zhangting/([a-z]*)/$', views.zhangting, name='zhangting'),
    url(r'^moni/$', views.moni, name='moni'),
    url(r'^gdzd/$', views.gdzd, name='gdzd'),
    url(r'^hc/$', views.huice, name='huice'),
    url(r'^account_update/$', views.account_info_update, name='account_update'),
    url(r'^journalism/$', views.journalism, name='journalism'),
    url(r'^liaotianshiList/$', views.liaotianshiList, name='liaotianshiList'),
    url(r'^ztzx/$', views.zhutu_zhexian, name='zhutu_zhexian'),
    url(r'^zhutu_zhexian_ajax/$', views.zhutu_zhexian_ajax, name='zhutu_zhexian_ajax'),
    url(r'^moniAll/$', views.moni_all, name='moni_all'),
    url(r'^newMoni/$', views.newMoni, name='newMoni'),
    url(r'^gxjy/$', views.gxjy, name='gxjy'),
    url(r'^systems/$', views.systems, name='systems'),
    url(r'^get_system/$', views.get_system, name='get_system'),

    # 期货监控系统
    url(r'^cfmmc/login/$', views.cfmmc_login, name='cfmmc_login'),
    url(r'^cfmmc/data/$', views.cfmmc_data, name='cfmmc_data'),
    url(r'^cfmmc/save/$', views.cfmmc_save, name='cfmmc_save'),
    url(r'^cfmmc/logout/$', views.cfmmc_logout, name='cfmmc_logout'),
    url(r'^cfmmc/dataPage/$', views.cfmmc_data_page, name='cfmmc_data_page'),
    url(r'^cfmmc/dataLocal/$', views.cfmmc_data_local, name='cfmmc_data_local'),
    url(r'^cfmmc/huice/$', views.cfmmc_huice, name='cfmmc_huice'),
    url(r'^cfmmc/huice/(?P<param>[d|w|m|y]\d{4,6}\w*)/$', views.cfmmc_huice, name='cfmmc_huice'),
    url(r'^cfmmc/bs/$', views.cfmmc_bs, name='cfmmc_bs'),
    url(r'^cfmmc/bs/(?P<param>\w+)/$', views.cfmmc_bs, name='cfmmc_bs'),
    url(r'^cfmmc/hc/$', views.cfmmc_hc, name='cfmmc_hc'),
    url(r'^cfmmc/isdownload/data/$', views.cfmmc_isdownload_data, name='cfmmc_isdownload_data'),

    url(r'^web/$', views.websocket_test, name='websocket_test'),

    # url(r'', views.page_not_found, name='page_not_found'),

]

handler404 = views.page_not_found  # 404 错误 page not found视图
handler500 = views.page_not_found  # 500 错误 server error视图
handler400 = views.page_not_found  # 500 错误 bad request视图
