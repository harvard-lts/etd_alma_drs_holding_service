from celery import Celery
from celery import bootsteps
from celery.signals import worker_ready
from celery.signals import worker_shutdown
from pathlib import Path
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
from opentelemetry.trace.propagation.tracecontext \
    import TraceContextTextMapPropagator
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from etd.drs_holding_by_dropbox import DRSHoldingByDropbox
from etd.drs_holding_by_api import DRSHoldingByAPI
from etd.mongo_util import MongoUtil
import etd.mongo_util as mongo_util

app = Celery()
app.config_from_object('celeryconfig')
etd.configure_logger()
logger = logging.getLogger('etd_alma_drs_holding')

FEATURE_FLAGS = "feature_flags"
DRS_HOLDING_FEATURE_FLAG = "drs_holding_record_feature_flag"

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(span_processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# heartbeat setup
# code is from
# https://github.com/celery/celery/issues/4079#issuecomment-1270085680
hbeat_path = os.getenv("HEARTBEAT_FILE", "/tmp/worker_heartbeat")
ready_path = os.getenv("READINESS_FILE", "/tmp/worker_ready")
update_interval = float(os.getenv("HEALTHCHECK_UPDATE_INTERVAL", 15.0))
HEARTBEAT_FILE = Path(hbeat_path)
READINESS_FILE = Path(ready_path)
UPDATE_INTERVAL = update_interval  # touch file every 15 seconds


class LivenessProbe(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Timer'}

    def __init__(self, worker, **kwargs):  # pragma: no cover
        self.requests = []
        self.tref = None

    def start(self, worker):  # pragma: no cover
        self.tref = worker.timer.call_repeatedly(
            UPDATE_INTERVAL, self.update_heartbeat_file,
            (worker,), priority=10,
        )

    def stop(self, worker):  # pragma: no cover
        HEARTBEAT_FILE.unlink(missing_ok=True)

    def update_heartbeat_file(self, worker):  # pragma: no cover
        HEARTBEAT_FILE.touch()


@worker_ready.connect
def worker_ready(**_):  # pragma: no cover
    READINESS_FILE.touch()


@worker_shutdown.connect
def worker_shutdown(**_):  # pragma: no cover
    READINESS_FILE.unlink(missing_ok=True)


app.steps["worker"].add(LivenessProbe)


@app.task(serializer='json',
          name='etd-alma-drs-holding-service.tasks.add_holdings')
def add_holdings(json_message):

    ctx = None
    if "traceparent" in json_message:  # pragma: no cover, tracing is not being tested # noqa: E501
        carrier = {"traceparent": json_message["traceparent"]}
        ctx = TraceContextTextMapPropagator().extract(carrier)
    with tracer.start_as_current_span("ALMA DRS HOLDINGS - add_holdings",
                                      context=ctx) \
            as current_span:
        logger.debug("message")
        logger.debug(json_message)
        current_span.add_event(json.dumps(json_message))
        if 'pqid' in json_message:
            proquest_identifier = json_message['pqid']
            current_span.set_attribute("identifier", proquest_identifier)
            logger.debug("processing id: " + str(proquest_identifier))

        feature_flag = os.getenv("DRS_HOLDING_RECORD_FEATURE_FLAG", "off")
        if feature_flag == "on":  # pragma: no cover, unit test should not create an Alma holding record # noqa: E501
            # Create holding record
            logger.debug("FEATURE IS ON>>>>> \
                          CREATE DRS HOLDING RECORD IN ALMA")
            current_span.add_event("FEATURE IS ON>>>>> \
                                    CREATE DRS HOLDING RECORD IN ALMA")
            if 'pqid' not in json_message:
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("Proquest ID is missing. \
                                        Cannot create DRS holding \
                                        record in Alma.")
                logger.debug("Proquest ID is missing. \
                              Cannot create DRS holding \
                              record in Alma.")
                # Can't do task if it is missing.
                return
            if 'object_urn' not in json_message:
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("Object URN is missing. \
                                        Cannot create DRS holding \
                                        record in Alma.")
                logger.debug("Object URN is missing. \
                              Cannot create DRS holding \
                              record in Alma.")
                # Can't do task if this is missing
                return

            # Create the DRS holding record in Alma
            create_drs_holding_record_in_alma(json_message)
        else:
            # No feature flags so do hello world for now
            return invoke_hello_world(json_message)


def create_drs_holding_record_in_alma(json_message):  # pragma: no cover, not sending to alma in unit tests # noqa: E501
    current_span = trace.get_current_span()
    pqid = json_message['pqid']
    object_urn = json_message['object_urn']
    mongoutil = MongoUtil()
    query = {mongo_util.FIELD_SUBMISSION_STATUS:
             mongo_util.ALMA_STATUS,
             mongo_util.FIELD_PQ_ID: pqid}
    fields = {mongo_util.FIELD_PQ_ID: 1,
              mongo_util.FIELD_IN_DASH: 1}
    try:
        matching_records = mongoutil.query_records(query, fields)
        record_list = list(matching_records)
        if len(record_list) > 1:
            logger.warn(f"Found {len(record_list)} for {pqid}")
        if (len(record_list) == 0):
            logger.error(f"Unable to find record for {pqid}")
            current_span.set_status(Status(StatusCode.ERROR))
            current_span.add_event("Unable to find record in mongo")
            return
        in_dash = record_list[0][mongo_util.FIELD_IN_DASH]
        logger.debug(record_list)
        if in_dash:
            current_span.add_event(f"{pqid} is in DASH. Creating DRS Holding \
                                    record in Alma by dropbox")
            logger.info(f"{pqid} is in DASH. Creating DRS Holding \
                                    record in Alma by dropbox")
            # Create the DRS holding record in Alma
            drs_holding = DRSHoldingByDropbox(pqid, object_urn)
            sent = drs_holding.send_to_alma(json_message)
            if not sent:
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("Unable to create DRS holding record \
                                        in Alma by dropbox")
                logger.error("Unable to create DRS holding record \
                                        in Alma by dropbox")
        else:
            current_span.add_event(f"{pqid} is NOT in DASH. Updating \
                                    DRS Holding record in Alma by API")
            logger.info(f"{pqid} is NOT in DASH. Updating \
                                    DRS Holding record in Alma by API")
            # Create the DRS holding record in Alma
            drs_holding = DRSHoldingByAPI(pqid, object_urn)
            sent = drs_holding.send_to_alma(json_message)
            if not sent:
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("Unable to create DRS holding record \
                                        in Alma by API")
                logger.error("Unable to create DRS holding record \
                                        in Alma by API")
    except Exception as e:
        logger.error(f"Error querying records: {e}")
        current_span.set_status(Status(StatusCode.ERROR))
        current_span.add_event(f"Unable to query mongo for DRS  \
                               holdings for pqid {pqid}")
        current_span.record_exception(e)


def invoke_hello_world(json_message):

    ctx = None
    if "traceparent" in json_message:  # pragma: no cover, tracing is not being tested # noqa: E501
        carrier = {"traceparent": json_message["traceparent"]}
        ctx = TraceContextTextMapPropagator().extract(carrier)
    with tracer.start_as_current_span("invoke_hello_world_drs_holding",
                                      context=ctx) as current_span:

        # For 'hello world', we are also going to place a
        # message onto the etd_ingested_into_drs queue
        # to allow the pipeline to continue
        new_message = {"hello": "from etd-alma-drs-holding-service"}
        if FEATURE_FLAGS in json_message:
            logger.debug("FEATURE FLAGS FOUND")
            logger.debug(json_message[FEATURE_FLAGS])
            new_message[FEATURE_FLAGS] = json_message[FEATURE_FLAGS]
            current_span.add_event("FEATURE FLAGS FOUND")
            current_span.add_event(json.dumps(json_message[FEATURE_FLAGS]))

        if 'pqid' in json_message:
            proquest_identifier = json_message['pqid']
            new_message["pqid"] = proquest_identifier
            current_span.set_attribute("identifier", proquest_identifier)
            logger.debug("processing id: " + str(proquest_identifier))

        # If only unit testing, return the message and
        # do not trigger the next task.
        if "unit_test" in json_message:
            return new_message

        carrier = {}  # pragma: no cover, tracing is not being tested # noqa: E501
        TraceContextTextMapPropagator().inject(carrier)  # pragma: no cover, tracing is not being tested # noqa: E501
        traceparent = carrier["traceparent"]  # pragma: no cover, tracing is not being tested # noqa: E501
        new_message["traceparent"] = traceparent  # pragma: no cover, tracing is not being tested # noqa: E501
        current_span.add_event("to next queue")  # pragma: no cover, tracing is not being tested # noqa: E501
        app.send_task("tasks.tasks.do_task", args=[new_message], kwargs={},
                      queue=os.getenv("PUBLISH_QUEUE_NAME"))  # pragma: no cover, does not reach this for unit testing # noqa: E501

        return {}  # pragma: no cover, does not reach this for unit testing # noqa: E501
