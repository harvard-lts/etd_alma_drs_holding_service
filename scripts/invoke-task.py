from celery import Celery
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

res = app1.send_task('etd-alma-drs-holding-service.tasks.add_holdings',
                     args=[{"hello": "world"}], kwargs={},
                     queue=os.getenv("CONSUME_QUEUE_NAME"))
