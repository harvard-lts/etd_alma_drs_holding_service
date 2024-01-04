# flake8: noqa
import os
from opentelemetry import trace
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import SERVICE_NAME

import sys
import re
from lxml import etree
import logging
from etd.mongo_util import MongoUtil
import etd.mongo_util as mongo_util
from . import configure_logger

# To help find other directories that might hold modules or config files
binDir = os.path.dirname(os.path.realpath(__file__))

# Find and load any of our modules that we need
confDir = binDir.replace('/bin', '/conf')
libDir  = binDir.replace('/bin', '/lib')
sys.path.append(confDir)
sys.path.append(libDir)
from .etds2alma_tables import schools
from lib.ltstools import get_date_time_stamp
from .xfer_files import xfer_files
from lib.notify import notify

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')
notify.logDir = os.getenv("LOGFILE_PATH", "/home/etdadm/logs/etd_alma_drs_holding")

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(span_processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

almaMarcxmlTemplate = os.getenv('ALMA_MARCXML_DRSHOLDING_TEMPLATE',
								"/home/etdadm/templates/" \
								"alma_marcxml_drsholding_template.xml")
dropboxUser = os.getenv('DROPBOX_USER')
dropboxServer = os.getenv('DROPBOX_SERVER')
privateKey = os.getenv('PRIVATE_KEY_PATH')
dataDir = os.getenv('DATA_DIR')
filesDir = os.getenv('FILES_DIR')
notifyJM = False
jobCode = 'drsholding2alma'
instance = os.getenv('INSTANCE', '')
if (instance == 'prod'): # pragma: no cover
    instance = ''

metsDmdSecNamespace = '{http://www.loc.gov/METS/}'
metsDimNamespace = '{http://www.dspace.org/xmlns/dspace/dim}'

yyyymmdd = get_date_time_stamp('day')
yymmdd = yyyymmdd[2:]

reTheTitle = re.compile('"?(the) .*', re.IGNORECASE)
reAnTitle = re.compile('"?(an) .*', re.IGNORECASE)
reATitle = re.compile('"?(a) .*', re.IGNORECASE)

# To wrap the xml records in a collection
xmlStartCollection = """
<collection xmlns="http://www.loc.gov/MARC21/slim"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
"""

xmlEndCollection = "</collection>"

FEATURE_FLAGS = "feature_flags"
ALMA_FEATURE_FORCE_UPDATE_FLAG = "alma_feature_force_update_flag"
ALMA_FEATURE_VERBOSE_FLAG = "alma_feature_verbose_flag"
INTEGRATION_TEST = os.getenv('MONGO_DB_COLLECTION_ITEST', 'integration_test')
ALMA_TEST_BATCH_NAME = os.getenv('ALMA_TEST_BATCH_NAME','proquest2023071720-993578-gsd')

"""
This the worker class for the etd alma service.
"""


class DRSHoldingByDropbox():
    logger = logging.getLogger('etd_alma_drs_holding')

    def __init__(self, pqid, test_collection=None):
        configure_logger()
        self.mongoutil = MongoUtil()
        self.pqid = pqid
        if test_collection is not None:  # pragma: no cover, only changes collection # noqa
            self.mongoutil.set_collection(self.mongoutil.db[test_collection])
	
    @tracer.start_as_current_span("send_holding_to_alma_worker")
    def send_to_alma(self, message):  # pragma: no cover
        """
		Sends a message to the Alma DRS holding worker main.

		Args:
			message (dict): The message to be sent.

		Returns:
			bool: True if the message was sent successfully, False otherwise.
		"""
        force = False
        verbose = False
        integration_test = False
        current_span = trace.get_current_span()
        if FEATURE_FLAGS in message:
            feature_flags = message[FEATURE_FLAGS]
            if (ALMA_FEATURE_FORCE_UPDATE_FLAG in feature_flags and
                feature_flags[ALMA_FEATURE_FORCE_UPDATE_FLAG] == "on"):
                force = True
            if (ALMA_FEATURE_VERBOSE_FLAG in feature_flags and
                feature_flags[ALMA_FEATURE_VERBOSE_FLAG] == "on"):
                verbose = True
        if (INTEGRATION_TEST in message and
            message[INTEGRATION_TEST] == True):
            integration_test = True
            self.logger.info('running integration test for alma drs holding service')    
            current_span.add_event("running integration test for alma drs holding service")
        current_span.add_event("sending to alma drs holding worker main")
        self.logger.info('sending to alma drs holding worker main')
        self.send_to_alma_worker(force, verbose, integration_test)
        self.logger.info('complete')
        return True
		
    @tracer.start_as_current_span("send_holding_to_alma_worker")
    def send_to_alma_worker(self, force=False, verbose=False, integration_test=False):  # pragma: no cover
        """
	    Sends the DRS holding to Alma dropbox.

	    Args:
		    force (bool): Flag to force re-running a processed batch. Default is False.
		    verbose (bool): Flag to enable verbose logging. Default is False.
		    integration_test (bool): Flag to indicate integration testing. Default is False.

	    Returns:
		    bool: True if the DRS holding was sent, False otherwise.
	    """
        current_span = trace.get_current_span()
        current_span.add_event("sending drs holding to alma dropbox")
        global notifyJM
        wroteXmlRecords = False

	    # Create a notify object, this will also set-up logging and
        # logFile  = f'{logDir}/{jobCode}.{yymmdd}.log'
        notifyJM = notify('monitor', jobCode, None)
		
        # Let the Job Monitor know that the job has started
        notifyJM.log('pass', 'Create ETD Alma DRS Holding Record', verbose)
        notifyJM.report('start')

        mongo_record_for_pqid = self.___get_record_from_mongo()

        # Start xml record collection output file
        yyyymmddhhmm    = get_date_time_stamp('minute')
        xmlCollectionFileName = f'AlmaDRSDark{instance.capitalize()}_{yyyymmddhhmm}.xml'
        xmlCollectionFile = xmlCollectionFileName
        if integration_test:
            xmlCollectionFile = f'AlmaDRSDarkTest{instance.capitalize()}_{yyyymmddhhmm}.xml'
            schoolMatch = re.match(r'proquest\d+-\d+-(\w+)', ALMA_TEST_BATCH_NAME)
            if schoolMatch:
                school = schoolMatch.group(1)
        else:
            school = mongo_record_for_pqid[mongo_util.FIELD_SCHOOL_ALMA_DROPBOX]
        xmlCollectionOut = open(xmlCollectionFile, 'w')
        xmlCollectionOut.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        xmlCollectionOut.write(f'{xmlStartCollection}\n')

        # Check to see if this was already processed by looking in Mongo
        # Do not re-run a processed batch unless forced #- test
        if (not integration_test):
            if ((not force) and self.__record_already_processed()):
                notifyJM.log('fail', f'Holding record {self.pqid} has already been created. Use force flag to re-run.', True)
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event(f'Holding record {self.pqid} has already been created. Use force flag to re-run.')
                return
		# Let the Job Monitor know that the job has started
        notifyJM.log('pass', f'Holding record creation for {self.pqid} for school {school} is beginning', verbose)

        batch = mongo_record_for_pqid[mongo_util.FIELD_DIRECTORY_ID]
		
        # Check for mets file and mapfile
        metsFile = f'{dataDir}/in/{batch}/mets.xml'
        if not os.path.exists(metsFile):
            notifyJM.log('fail', f"{metsFile} not found for {batch}", True)
            current_span.set_status(Status(StatusCode.ERROR))
            current_span.add_event(f'{metsFile} not found')
            current_span.add_event(f'skippping batch {batch} for school {school}')
            self.logger.error(f'{metsFile} not found for {batch}')
            return

		# Get needed data from mets file
        marcXmlValues = self.getFromMets(metsFile, school)
	
        if marcXmlValues:
	
            # Write marc xml record in batch directory
            marcXmlRecord = False
            try:
                batchOutDir = f'{dataDir}/out/{batch}'
                marcXmlRecord = self.writeMarcXml(batch, batchOutDir, marcXmlValues, verbose)
                self.logger.debug(f'Wrote DRS Holding MARCXML record for {batch} for {school}')
                current_span.add_event(f'Wrote DRS Holding MARCXML record for {batch} for {school}')
            except Exception as err:
                self.logger.error(f"Writing DRS Holding MARCXML record for {batch} for {school} failed, skipping", exc_info=True)
                notifyJM.log('fail', f"Writing DRS Holding MARCXML record for {batch} for {school} failed, skipping", True)
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event(f'Writing DRS Holding MARCXML record for {batch} for {school} failed, skipping')
                return

            # And then write xml record to collection file and update mongo
            if marcXmlRecord:
                xmlCollectionOut.write(marcXmlRecord)
                self.logger.debug(f'MARCXML record for {batch} for {school} added to collection file')
                current_span.add_event(f'MARCXML record for {batch} for {school} added to collection file')
				# Update mongo
                self.logger.debug(f'Updating mongo...')
                try:
                    query = {mongo_util.FIELD_PQ_ID: self.pqid,
                             mongo_util.FIELD_DIRECTORY_ID: batch}
                    self.mongoutil.update_status(
                        query, mongo_util.DRS_HOLDING_DROPBOX_STATUS)
                    current_span.add_event(f'Status for Proquest id {self.pqid} in {batch} for school {school} updated in mongo')
                except Exception as e:
                    self.logger.error(f"Error updating status for {self.pqid}: {e}")
                    current_span.set_status(Status(StatusCode.ERROR))
                    current_span.add_event(f'Could not update proquest id {self.pqid} in {batch} for school {school} in mongo')
                    current_span.record_exception(e)
                    self.mongoutil.close_connection()
                    raise e
		
        drsHoldingSent = False			
        # If marcxml file was written successfully, finish xml records 
	    # collection file and then send it to dropbox for Alma to load
        if wroteXmlRecords:

            xmlCollectionOut.write(f'{xmlEndCollection}\n')
            xmlCollectionOut.close()

            xfer = xfer_files(dropboxServer, dropboxUser, privateKey)
	
            if xfer.error:
                notifyJM.log('fail', xfer.error, True)
                self.logger.error(xfer.error)
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event(xfer.error)				
            else:
                targetFile = '/incoming/' + os.path.basename(xmlCollectionFile)
                xfer.put_file(xmlCollectionFile, targetFile)

                if xfer.error:
                    notifyJM.log('fail', xfer.error, True)
                    current_span.set_status(Status(StatusCode.ERROR))
                    current_span.add_event(xfer.error)
                    self.logger.error(xfer.error)
                else:
                    notifyJM.log('pass', f'{xmlCollectionFile} was sent to {dropboxUser}@{dropboxServer}:{targetFile}', verbose)
                    current_span.set_attribute("uploaded_identifier", marcXmlValues['proquestId'])
                    current_span.set_attribute("uploaded_file", targetFile)
                    current_span.add_event(f'{xmlCollectionFile} was sent to {dropboxUser}@{dropboxServer}:{targetFile}')
                    self.logger.debug("uploaded proquest id: " + str(marcXmlValues['proquestId']))
                    self.logger.debug("uploaded file: " + str(targetFile))

            xfer.close()

            drsHoldingSent = True
            # Otherwise, remove file
            os.remove(xmlCollectionFile)
        else:
            xmlCollectionOut.close()
            os.remove(xmlCollectionFile)
            notifyJM.log('pass', 'No DRS Holding to send to Alma', verbose)
            current_span.add_event("No DRS Holding to send to Alma")
            self.logger.debug("No DRS Holding to send to Alma")

        current_span.add_event(f'{self.pqid} DRS holding was sent to Alma')
        self.logger.debug(f'{self.pqid} DRS holding was sent to Alma')
        notifyJM.log('pass', f'{self.pqid} DRS holding was sent to Alma', verbose)
        notifyJM.report('complete')
        current_span.add_event("completed")	
	
        # Returns True if the DRS Holding was sent, False otherwise
        return drsHoldingSent
	
    @tracer.start_as_current_span("send_holding_to_alma_worker")
    def __record_already_processed(self): # pragma: no cover, not using for unit tests
        current_span = trace.get_current_span()
        current_span.add_event("verifying if DRS holding exists")
        query = {mongo_util.FIELD_SUBMISSION_STATUS:
                 mongo_util.DRS_HOLDING_DROPBOX_STATUS,
				 mongo_util.FIELD_PQ_ID: self.pqid}
        fields = {mongo_util.FIELD_PQ_ID: 1,
                  mongo_util.FIELD_SCHOOL_ALMA_DROPBOX: 1,
                  mongo_util.FIELD_SUBMISSION_STATUS: 1,
                  mongo_util.FIELD_DIRECTORY_ID: 1}
        self.logger.info("Starting poll for alma DRS holding")
        matching_records = []

        try:
            matching_records = self.mongoutil.query_records(query, fields)
            self.logger.info(f"Found {len(matching_records)} \
                             matching records")
            self.logger.debug(f"Matching records: {matching_records}")
        except Exception as e:
            self.logger.error(f"Error querying records: {e}")
            current_span.set_status(Status(StatusCode.ERROR))
            current_span.add_event("Unable to query mongo for DRS holdings")
            current_span.record_exception(e)
            raise e
        record_list = list(matching_records)
        return len(record_list) > 0
	
    @tracer.start_as_current_span("send_holding_to_alma_worker")
    def ___get_record_from_mongo(self): # pragma: no cover, not using for unit tests
        current_span = trace.get_current_span()
        current_span.add_event("getting data from mongo exists")
        query = {mongo_util.FIELD_SUBMISSION_STATUS:
                 mongo_util.ALMA_STATUS,
				 mongo_util.FIELD_PQ_ID: self.pqid}
        fields = {mongo_util.FIELD_PQ_ID: 1,
                  mongo_util.FIELD_SCHOOL_ALMA_DROPBOX: 1,
                  mongo_util.FIELD_SUBMISSION_STATUS: 1,
                  mongo_util.FIELD_DIRECTORY_ID: 1}
        self.logger.info("getting data from mongo exists")
        matching_records = []

        try:
            matching_records = self.mongoutil.query_records(query, fields)
            self.logger.info(f"Found {len(matching_records)} \
                             matching records")
            self.logger.debug(f"Matching records: {matching_records}")
        except Exception as e:
            self.logger.error(f"Error querying records: {e}")
            current_span.set_status(Status(StatusCode.ERROR))
            current_span.add_event("Unable to query mongo for DRS holdings")
            current_span.record_exception(e)
            raise e
        record_list = list(matching_records)
        if len(record_list) > 1:
            self.logger.warn(f"Found {len(record_list)} for {self.pqid}")
        if (len(record_list) == 0):
            self.logger.error(f"Unable to find record for {self.pqid}")
            current_span.set_status(Status(StatusCode.ERROR))
            current_span.add_event("Unable to find record in mongo")
            current_span.record_exception(e)
            raise Exception(f"Unable to find record for {self.pqid}")
        return record_list[0]
    

	# Get data from mets file that's needed to write marc xml.
	# The marcXmlValues dictionary is populated and returned.
    def getFromMets(self, metsFile, school):  # pragma: no cover
        global notifyJM, jobCode
        if notifyJM == False:
            notifyJM = notify('monitor', jobCode, None)
        foundAll = True
        marcXmlValues = {}
		
        if self.pqid:
            marcXmlValues['proquestId'] = self.pqid
        marcXmlValues['school'] = school

		# Load mets file, get the root node and then parse
        self.__cleanMetsFile(metsFile)
        metsTree   = etree.parse(metsFile)
        rootMets   = metsTree.getroot()
        rootDmdSec = rootMets.find(f'{metsDmdSecNamespace}dmdSec')
        for dimField in rootDmdSec.iter(f'{metsDimNamespace}field'):
		
			# Dc mdschema
            if dimField.attrib['mdschema'] == 'dc':

				# Date created
                if dimField.attrib['element'] == 'date':
                    if dimField.attrib['qualifier'] == 'created':
                        marcXmlValues['dateCreated'] = dimField.text
		
				# Title and title indicator 2
                elif dimField.attrib['element'] == 'title':
                    marcXmlValues['title'] = dimField.text
					
                    if reTheTitle.match(marcXmlValues['title']):
                        marcXmlValues['titleIndicator2'] = '4'
                    elif reAnTitle.match(marcXmlValues['title']):
                        marcXmlValues['titleIndicator2'] = '3'
                    elif reATitle.match(marcXmlValues['title']):
                        marcXmlValues['titleIndicator2'] = '2'
                    else:
                        marcXmlValues['titleIndicator2'] = '0'

		# Check that we found our needed values
        for var in ('dateCreated', 'proquestId', 'title', 'school'):
            if var not in marcXmlValues:
                notifyJM.log('fail', f'Failed to find {var} in {metsFile}', True)
                foundAll = False
				
        if foundAll:
            return marcXmlValues
        else:
            return False

	# Write marcxml using data passed in the marcXmlValues dictionary
    def writeMarcXml(self, batch, batchOutDir, marcXmlValues, verbose):  # pragma: no cover
        global notifyJM, jobCode
        if notifyJM == False:
            notifyJM = notify('monitor', jobCode, None)
        xmlRecordFile = f'{batchOutDir}/' + batch.replace('proquest', 'almadrsholding') + '.xml'

		# Load template file and swapped in variables
        marcXmlTree = etree.parse(almaMarcxmlTemplate)
        rootRecord = marcXmlTree.getroot()
        for child in rootRecord.iter('controlfield', 'subfield'):
            # 008 controlfield
            if child.tag == 'controlfield':
                if child.attrib['tag'] == '008':
                    childText = child.text.replace('YYMMDD', yymmdd)
                    childText = childText.replace('DATE_CREATED_VALUE', marcXmlValues['dateCreated'])
                    child.text = childText
                    print(child.text)

			# datafield/subfields
            elif child.tag == 'subfield':
                parent = child.getparent()
                if parent.tag == 'datafield':
					
					# Datafield 035, Proquest ID
                    if parent.attrib['tag'] == '035':
                        if child.attrib['code'] == 'a':
                            childText  = child.text.replace('PROQUEST_IDENTIFIER_VALUE', marcXmlValues['proquestId'])
                            child.text = childText

					# Datafield 245, title and title indicator 2
                    elif parent.attrib['tag'] == '245':
                        if child.attrib['code'] == 'a':
                            childText  = child.text.replace('TITLE_VALUE', marcXmlValues['title'])
                            child.text = childText
                            parentInd2 = parent.attrib['ind2'].replace('TITLE_INDICATOR_2_VALUE', marcXmlValues['titleIndicator2'])
                            parent.attrib['ind2'] = parentInd2

					# Datafield 852
                    elif parent.attrib['tag'] == '852':
                        if 'object_urn' in marcXmlValues: # print NET/ETD if there is a dash id
                            if child.attrib['code'] == 'z':
                                childText  = child.text.replace('[DRS OBJECT URN]', marcXmlValues['object_urn'])
                                child.text = childText

					# Datafield 909, proquest id
                    elif parent.attrib['tag'] == '909':
                        if child.attrib['code'] == 'k':
                            childText  = child.text.replace('LIB_CODE_3_CHAR', schools[marcXmlValues['school']]['lib_code_3_char'])
                            child.text = childText
		
		# Write xml record out in batch directory
        with open(xmlRecordFile, 'w') as xmlRecordOut:
            xmlRecordOut.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            xmlRecordOut.write(f'{xmlStartCollection}\n')
            marcXmlStr = etree.tostring(rootRecord, encoding='unicode')
            xmlRecordOut.write(marcXmlStr)
            xmlRecordOut.write(f'{xmlEndCollection}\n')
			
        notifyJM.log('pass', f'Wrote {xmlRecordFile}', verbose)
		
		# And then return it to be collected with other processed records
        return marcXmlStr

    def __cleanMetsFile(self, metsFile):
        with open(metsFile, 'r') as metsFileIn:
            metsFileContents = metsFileIn.readlines()

		# Remove control characters
        r = re.compile('[\u0000-\u0008\u000c\u000e-\u001f]')
        with open(metsFile, 'w') as metsFileOut:
            for line in metsFileContents:
                line = r.sub('', line)			
                metsFileOut.write(line)
