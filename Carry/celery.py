from __future__ import absolute_import,unicode_literals #目的是拒绝隐士引入，celery.py和celery冲突。
import os
from celery import Celery
# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Carry.settings')

from django.conf import settings  # noqa

app = Celery('Carry')

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))


# from __future__ import absolute_import, unicode_literals  # 目的是拒绝隐士引入，celery.py和celery冲突。
# import os
# from celery import Celery
# from django.conf import settings
#
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Carry.settings")
#
# # 创建celery应用
# app = Celery('Carry_celery')
# # You can pass the object directly here, but using a string is better since then the worker doesn’t have to serialize the object.
# app.config_from_object('django.conf:settings')
# # 如果在工程的应用中创建了tasks.py模块，那么Celery应用就会自动去检索创建的任务。比如你添加了一个任务，在django中会实时地检索出来。
# app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)