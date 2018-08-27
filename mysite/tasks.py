# # from __future__ import absolute_import
# # from celery import shared_task, task
# from Carry import celery_app
# import json
# import time
# import datetime
#
# from mysite import HSD
# from django.shortcuts import render
#
# # from mysite import viewUtil
#
#
# # @periodic_task(run_every=timedelta(seconds=10), routing_key=settings.LOW_PRIORITY_QUEUE)
#
# # @shared_task(track_started=True)
#
# # @shared_task  # 方法一
# # @task(name='mysite.tasks.testss')  # 方法二
# @celery_app.task  # 方法三
# def testss(files, IP_NAME):
#     """ 测试 """
#     pass
#
#
# @celery_app.task
# def tasks_record_log(files, info, types):
#     """ 访问日志 """
#     if types == 'w':
#         with open(files, 'w') as f:
#             f.write(json.dumps(info))
#     elif types == 'a':
#         with open(files, 'a') as f:
#             f.write(info)
#
