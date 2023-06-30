from celery import Celery
import os
import json

app = Celery()
app.config_from_object('celeryconfig')


@app.task(serializer='json',
          name='etd-alma-drs-holding-service.tasks.add_holdings')
def add_holdings(message):
    print("message")
    print(message)
    json_message = json.loads(message)
    if "feature_flags" in json_message:
        feature_flags = json_message["feature_flags"]
        if "drs_holding_record_feature_flag" in feature_flags and \
                feature_flags["drs_holding_record_feature_flag"] == "on":
            if "send_to_drs_feature_flag" in feature_flags and \
                    feature_flags["send_to_drs_feature_flag"] == "on":
                # Create holding record
                print("FEATURE IS ON>>>>>CREATE DRS HOLDING RECORD IN ALMA")
            else:
                print("send_to_drs_feature_flag MUST BE ON FOR THE ALMA \
                HOLDING TO BE CREATED. send_to_drs_feature_flag IS SET TO OFF")
        else:
            # Feature is off so do hello world
            invoke_hello_world(json_message)


# To be removed when real logic takes its place
def invoke_hello_world(json_message):

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    new_message = {"hello": "from etd-alma-drs-holding-service"}
    if "feature_flags" in json_message:
        print("FEATURE FLAGS FOUND")
        print(json_message['feature_flags'])
        new_message['feature_flags'] = json_message['feature_flags']
    app.send_task("tasks.tasks.do_task", args=[new_message], kwargs={},
                  queue=os.getenv("PUBLISH_QUEUE_NAME"))
