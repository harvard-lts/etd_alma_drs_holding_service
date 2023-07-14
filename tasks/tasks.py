from celery import Celery
import os
import logging
import etd
import json
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma_drs_holding')

FEATURE_FLAGS = "feature_flags"
DRS_HOLDING_FEATURE_FLAG = "drs_holding_record_feature_flag"
SEND_TO_DRS_FEATURE_FLAG = "send_to_drs_feature_flag"

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')

resource = Resource(attributes={
    SERVICE_NAME: JAEGER_SERVICE_NAME
})

trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)
otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)


@tracer.start_as_current_span("add_holdings")
@app.task(serializer='json',
          name='etd-alma-drs-holding-service.tasks.add_holdings')
def add_holdings(json_message):

    logger.debug("message")
    logger.debug(json_message)
    current_span = trace.get_current_span()
    current_span.add_event(json.dumps(json_message))
    if FEATURE_FLAGS in json_message:
        feature_flags = json_message[FEATURE_FLAGS]
        if DRS_HOLDING_FEATURE_FLAG in feature_flags and \
                feature_flags[DRS_HOLDING_FEATURE_FLAG] == "on":
            if SEND_TO_DRS_FEATURE_FLAG in feature_flags and \
                    feature_flags[SEND_TO_DRS_FEATURE_FLAG] == "on":
                # Create holding record
                logger.debug("FEATURE IS ON>>>>> \
                CREATE DRS HOLDING RECORD IN ALMA")
                current_span.add_event("FEATURE IS ON>>>>> \
                CREATE DRS HOLDING RECORD IN ALMA")
            else:
                logger.debug("send_to_drs_feature_flag MUST BE ON \
                FOR THE ALMA HOLDING TO BE CREATED. \
                send_to_drs_feature_flag IS SET TO OFF")
                current_span.add_event("send_to_drs_feature_flag MUST BE ON \
                FOR THE ALMA HOLDING TO BE CREATED. \
                send_to_drs_feature_flag IS SET TO OFF")
        else:
            # Feature is off so do hello world
            return invoke_hello_world(json_message)
    else:
        # No feature flags so do hello world for now
        return invoke_hello_world(json_message)


# To be removed when real logic takes its place
@tracer.start_as_current_span("invoke_hello_world")
def invoke_hello_world(json_message):

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    current_span = trace.get_current_span()
    new_message = {"hello": "from etd-alma-drs-holding-service"}
    if FEATURE_FLAGS in json_message:
        logger.debug("FEATURE FLAGS FOUND")
        logger.debug(json_message[FEATURE_FLAGS])
        new_message[FEATURE_FLAGS] = json_message[FEATURE_FLAGS]
        current_span.add_event("FEATURE FLAGS FOUND")
        current_span.add_event(json.dumps(json_message[FEATURE_FLAGS]))

    # If only unit testing, return the message and
    # do not trigger the next task.
    if "unit_test" in json_message:
        return new_message

    current_span.add_event("to next queue")
    app.send_task("tasks.tasks.do_task", args=[new_message], kwargs={},
                  queue=os.getenv("PUBLISH_QUEUE_NAME"))

    return {}
