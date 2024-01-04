from celery import Celery
import os

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

arguments = {"pqid": "0197490463", 
             "object_urn": "URN-3:HUL.DRS.OBJECT:101115924",
             "integration_test": True,
             "feature_flags": {
                'dash_feature_flag': "off",
                'alma_feature_flag': "off",
                'send_to_drs_feature_flag': "off",
                'drs_holding_record_feature_flag': "on"}}

res = app1.send_task('etd-alma-drs-holding-service.tasks.add_holdings',
                     args=[arguments], kwargs={},
                     queue=os.getenv("CONSUME_QUEUE_NAME"))
