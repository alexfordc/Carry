from __future__ import absolute_import
from celery import shared_task,task
import time
import json

# @periodic_task(run_every=timedelta(seconds=10), routing_key=settings.LOW_PRIORITY_QUEUE)

#@shared_task(track_started=True)

@task(name='mysite.tasks.record_from_w')
def record_from_w(files,IP_NAME):
    with open(files, 'w') as f:
        f.write(json.dumps(IP_NAME))

@task(name='mysite.tasks.record_from_a')
def record_from_a(files,info):
    with open(files, 'a') as f:
        f.write(info)