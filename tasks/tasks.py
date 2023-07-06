from celery import Celery
import os
import logging
import etd
import json

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma_drs_holding')

FEATURE_FLAGS = "feature_flags"
DRS_HOLDING_FEATURE_FLAG = "drs_holding_record_feature_flag"
SEND_TO_DRS_FEATURE_FLAG = "drs_holding_record_feature_flag"


@app.task(serializer='json',
          name='etd-alma-drs-holding-service.tasks.add_holdings')
def add_holdings(message):
    logger.debug("message")
    logger.debug(message)
    json_message = json.loads(message)
    if FEATURE_FLAGS in json_message:
        feature_flags = json_message[FEATURE_FLAGS]
        if DRS_HOLDING_FEATURE_FLAG in feature_flags and \
                feature_flags[DRS_HOLDING_FEATURE_FLAG] == "on":
            if SEND_TO_DRS_FEATURE_FLAG in feature_flags and \
                    feature_flags[SEND_TO_DRS_FEATURE_FLAG] == "on":
                # Create holding record
                logger.debug("FEATURE IS ON>>>>> \
                CREATE DRS HOLDING RECORD IN ALMA")
            else:
                logger.debug("send_to_drs_feature_flag MUST BE ON \
                FOR THE ALMA HOLDING TO BE CREATED. \
                send_to_drs_feature_flag IS SET TO OFF")
        else:
            # Feature is off so do hello world
            invoke_hello_world(json_message)


# To be removed when real logic takes its place
def invoke_hello_world(json_message):

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    new_message = {"hello": "from etd-alma-drs-holding-service"}
    if FEATURE_FLAGS in json_message:
        logger.debug("FEATURE FLAGS FOUND")
        logger.debug(json_message[FEATURE_FLAGS])
        new_message[FEATURE_FLAGS] = json_message[FEATURE_FLAGS]

    # If only unit testing, return the message and
    # do not trigger the next task.
    if "unit_test" in json_message:
        return new_message

    app.send_task("tasks.tasks.do_task", args=[new_message], kwargs={},
                  queue=os.getenv("PUBLISH_QUEUE_NAME"))
