from celery import Celery
import os
import json

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

arguments = {"hello": "world", "feature_flags": {
            'dash_feature_flag': "off",
            'alma_feature_flag': "off",
            'send_to_drs_feature_flag': "off",
            'drs_holding_record_feature_flag': "off"}}
json_args = json.dumps(arguments)

res = app1.send_task('etd-alma-drs-holding-service.tasks.add_holdings',
                     args=[json_args], kwargs={},
                     queue=os.getenv("CONSUME_QUEUE_NAME"))
