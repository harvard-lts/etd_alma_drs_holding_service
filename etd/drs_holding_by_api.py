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
from lxml import etree
import logging
from . import configure_logger
import requests
from datetime import datetime
from time import time
from lib.notify import notify
import lxml.etree as ET
import shutil

# To help find other directories that might hold modules or config files
binDir = os.path.dirname(os.path.realpath(__file__))

# Find and load any of our modules that we need
confDir = binDir.replace('/bin', '/conf')
libDir = binDir.replace('/bin', '/lib')
sys.path.append(confDir)
sys.path.append(libDir)


# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')
notify.logDir = os.getenv("LOGFILE_PATH", "/home/etdadm/logs/etd")

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(span_processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# Alma API keys
ALMA_API_KEY = os.getenv('ALMA_API_KEY')
ALMA_API_KEY_2 = os.getenv('ALMA_API_KEY_2')
ALMA_API_BASE = os.getenv('ALMA_API_BASE')
ALMA_SRU_BASE = os.getenv('ALMA_SRU_BASE')
ALMA_SRU_MARCXML_BASE = os.getenv('ALMA_SRU_MARCXML_BASE')
ALMA_GET_BIB_BASE = "/almaws/v1/bibs/"
ALMA_GET_HOLDINGS_PATH = "/holdings"
SUBFIELD_Z_BASE = "Preservation object, "
almaMarcxmlTemplate = os.getenv('ALMA_MARCXML_DRSHOLDING_API_TEMPLATE',
                                "./templates/"
                                "alma_marcxml_drsholding_api_template.xml")

FEATURE_FLAGS = "feature_flags"
ALMA_FEATURE_FORCE_UPDATE_FLAG = "alma_feature_force_update_flag"
ALMA_FEATURE_VERBOSE_FLAG = "alma_feature_verbose_flag"
INTEGRATION_TEST = os.getenv('MONGO_DB_COLLECTION_ITEST', 'integration_test')
data_dir = os.getenv('DATA_DIR', './')

notifyJM = False
jobCode = 'drsholding2alma'
instance = os.getenv('INSTANCE', '')

"""
This the worker class for the etd alma service.
"""


class DRSHoldingByAPI():
    logger = logging.getLogger('etd_alma_drs_holding')

    
    @tracer.start_as_current_span("init_drs_holding_by_api")
    def __init__(self, pqid, object_urn, unittesting=False, integration_test=False, alt_output_dir=None):
        """
         This method initializes the class and creates a working directory 
         for xml files.
        """
        configure_logger()
        self.pqid = pqid
        self.mmsid = None
        self.holding_id = None
        self.object_urn = object_urn
        self.unittesting = unittesting
        self.integration_test = integration_test
        self.marc_xml_values = {}
        self.marc_xml_values['pqid'] = pqid
        self.marc_xml_values['object_urn'] = object_urn
        self.namespace_mapping = {'srw':
                                  'http://www.loc.gov/zing/srw/',
                                  'marc': 'http://www.loc.gov/MARC21/slim',
                                  'mods': 'http://www.loc.gov/mods/v3'}
        self.output_dir = f'{data_dir}/out/proquest{self.pqid}-holdings'
        if alt_output_dir is not None:
            self.output_dir = alt_output_dir 
        os.makedirs(self.output_dir, exist_ok=True)
        if not os.path.exists(self.output_dir):
            if (not self.unittesting):
                current_span = trace.get_current_span()
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event(f'can\'t create {self.output_dir}')
            self.logger.critical(f'can\'t create {self.output_dir}')


    
    @tracer.start_as_current_span("get_mms_id")
    def get_mms_id(self, pqid):
        """
        This method gets the mms id by proquest id.

        Args: proquest id
        Returns: True if the mms id is retrieved, False otherwise.
        """
        current_span = trace.get_current_span()
        if (not self.unittesting):
            current_span.add_event("getting mms id via proquest id")
            current_span.set_attribute("identifier", pqid)
        self.logger.debug("getting mms id via proquest id")

        r = requests.get(ALMA_SRU_MARCXML_BASE + pqid)
        self.logger.debug(ALMA_SRU_MARCXML_BASE + pqid)
        sru_file = f'{self.output_dir}/sru.xml'
        if r.status_code == 200:
            with open(sru_file, 'wb') as f:
                f.write(r.content)
        else:
            self.logger.error(f"Error getting DRS holding for pqid: {pqid}")
            return False

        mmsid_xpath = "//srw:searchRetrieveResponse/srw:records/srw:record/" \
                      "srw:recordIdentifier"
        xpath_245_ind2_xpath = "string(//marc:record/marc:datafield[@tag='245']/@id2)"
        xpath_245_subfield_a_xpath = "//marc:record/marc:datafield[@tag='245']" \
            "/marc:subfield[@code='a']"

        doc = ET.parse(sru_file)
        mms_id = doc.xpath(mmsid_xpath,
                           namespaces=self.namespace_mapping)[0].text
        self.marc_xml_values['mms_id'] = mms_id
        self.mmsid = mms_id
        xpath_245_ind2 = doc.xpath(xpath_245_ind2_xpath,
                                   namespaces=self.namespace_mapping)
        self.marc_xml_values['titleIndicator2'] = xpath_245_ind2
        xpath_245_subfield_a = doc.xpath(xpath_245_subfield_a_xpath,
                                         namespaces=self.namespace_mapping)[0]
        self.marc_xml_values['title'] = xpath_245_subfield_a

        return mms_id

    
    @tracer.start_as_current_span("get_drs_holding_id_by_mms_id")
    def get_drs_holding_id_by_mms_id(self, mms_id):
        """"
        This method gets the drs holdings id by mms id, 
        assuming the library code is a 3-letter code and location code = "NET".
        It will return false if cant find a 3-letter library code and 
        location code with "NET".

        Args: mms id
        
        Returns: True if the drs holdings id is retrieved, False otherwise.
        """

        current_span = trace.get_current_span()
        if (not self.unittesting):
            current_span.add_event("getting drs holdings by mms id")
            current_span.set_attribute("mms_id", mms_id)
        self.logger.debug("getting drs holdings by mms id")
        self.logger.debug(ALMA_API_BASE + ALMA_GET_BIB_BASE + mms_id
                         + ALMA_GET_HOLDINGS_PATH + "?apikey=" + ALMA_API_KEY_2)

        r = requests.get(ALMA_API_BASE + ALMA_GET_BIB_BASE + mms_id
                         + ALMA_GET_HOLDINGS_PATH + "?apikey=" + ALMA_API_KEY_2)
        holdings_file = f'{self.output_dir}/holdings.xml'
        if r.status_code == 200:
            with open(holdings_file, 'wb') as f:
                f.write(r.content)
        else:
            self.logger.error("Error getting DRS holdings list "
                              "for pqid: " + "self.pqid")
            return False

        # in case there are multiple holdings, loop through the holdings.xml file
        # and find the one where library code is a 3-letter code and location code = "NET"
        doc = ET.parse(holdings_file)
        library_xpath = "//library"
        loc_xpath = "//location"
        holding_id_xpath = "//holding_id"
        holdings = doc.xpath("//holding")
        for holding in holdings:
            library_code = holding.xpath(library_xpath)[0].text
            location_code = holding.xpath(loc_xpath)[0].text
            holding_id = holding.xpath(holding_id_xpath)[0].text
            if (len(library_code) == 3 and location_code == "NET"):
                self.marc_xml_values['library_code'] = library_code
                self.marc_xml_values['location_code'] = location_code
                self.marc_xml_values['holding_id'] = holding_id
                self.holding_id = holding_id
                break
        if self.holding_id is None:
            self.logger.error("Error getting DRS holdings id for pqid: " +
                              self.pqid)
            return False
        if self.marc_xml_values['library_code'] is None:
            self.logger.error("Error getting DRS holdings lib code " +
                              "for pqid: " + self.pqid)
            return False
        if self.marc_xml_values['location_code'] is None:
            self.logger.error("Error getting DRS holdings location code " +
                              "for pqid: " + self.pqid)
            return False
        return self.holding_id


    
    @tracer.start_as_current_span("get_drs_holding")
    def get_drs_holding(self, mms_id, holding_id):
        """
        This method gets the drs holding, given a mms id and holding id.
        Args: mms id, holding id
        Returns: True if the drs holding is retrieved, False otherwise.
        """
        current_span = trace.get_current_span()
        if (not self.unittesting):
            current_span.add_event("get drs holding")
            current_span.set_attribute("holding_id", holding_id)
            current_span.set_attribute("mms_id", mms_id)

        self.logger.debug("get drs holding")
        self.logger.debug(ALMA_API_BASE + ALMA_GET_BIB_BASE + mms_id +
                         ALMA_GET_HOLDINGS_PATH + "/" + holding_id +
                         "?apikey=" + ALMA_API_KEY_2)

        r = requests.get(ALMA_API_BASE + ALMA_GET_BIB_BASE + mms_id +
                         ALMA_GET_HOLDINGS_PATH + "/" + holding_id +
                         "?apikey=" + ALMA_API_KEY_2)
        holding_file = f'{self.output_dir}/holding.xml'
        if r.status_code == 200:
            with open(holding_file, 'wb') as f:
                f.write(r.content)
        else:
            self.logger.error("Error getting DRS holding file for pqid: " +
                              self.pqid)
            return False

        doc = ET.parse(holding_file)
        leader_xpath = "//record/leader"
        xpath_001_xpath = "//record/controlfield[@tag='001']"
        xpath_005_xpath = "//record/controlfield[@tag='005']"
        xpath_008_xpath = "//record/controlfield[@tag='008']"
        created_by_xpath = "//holding/created_by"
        created_date_xpath = "//holding/created_date"
        self.marc_xml_values['last_modified_by'] = "ETD"
        dateTimeStamp = datetime.fromtimestamp(int(time())).strftime("%Y-%m-%dZ")
        self.marc_xml_values['last_modified_date'] = dateTimeStamp
        self.marc_xml_values['last_modified_by'] = "ETDDEV"

        self.marc_xml_values['leader'] = doc.xpath(leader_xpath,
                                namespaces=self.namespace_mapping)[0].text
        self.marc_xml_values['001'] = doc.xpath(xpath_001_xpath,
                                namespaces=self.namespace_mapping)[0].text
        self.marc_xml_values['005'] = doc.xpath(xpath_005_xpath,
                                namespaces=self.namespace_mapping)[0].text
        self.marc_xml_values['008'] = doc.xpath(xpath_008_xpath,
                                namespaces=self.namespace_mapping)[0].text
        self.marc_xml_values['created_by'] = doc.xpath(created_by_xpath,
                                namespaces=self.namespace_mapping)[0].text
        self.marc_xml_values['created_date'] = doc.xpath(created_date_xpath,
                                namespaces=self.namespace_mapping)[0].text

        if self.marc_xml_values['leader'] is None:
            self.logger.error("Error getting marc leader value " +
                              "for pqid: " + self.pqid)
            return False
        if self.marc_xml_values['001'] is None:
            self.logger.error("Error getting marc 001 value " +
                              "for pqid: " + self.pqid)
            return False
        if self.marc_xml_values['005'] is None:
            self.logger.error("Error getting marc 005 value " +
                              "for pqid: " + self.pqid)
            return False
        if self.marc_xml_values['008'] is None:
            self.logger.error("Error getting marc 008 value " +
                              "for pqid: " + self.pqid)
            return False

        # return the xml holding
        return r.content

 
    @tracer.start_as_current_span("transform_drs_holding")
    def transform_drs_holding(self, batchOutDir, marcXmlValues, verbose=False):
        """
        Transforms the marcxml for the DRS holding.

        Args:
            marc_xml_values (dict): The marcxml values to be transformed.

        Returns:
            bool: True if the marcxml was transformed, False otherwise.
        """
        current_span = trace.get_current_span()
        if (not self.unittesting):
            current_span.add_event("transforming drs holding")
        self.logger.debug("transforming drs holding")

        updated_holding = f'{batchOutDir}/updated_holding.xml'
        # Load template file and swapped in variables
        marcXmlTree = etree.parse(almaMarcxmlTemplate)
        rootRecord = marcXmlTree.getroot()

        try:
            for child in rootRecord.iter('holding_id', 'created_by',
                                         'created_date','last_modified_by',
                                         'last_modified_date', 
                                         'leader', 'controlfield', 'subfield'):
                # header tags
                if child.tag == 'holding_id':
                    childText = child.text.replace('HOLDING_ID', marcXmlValues['holding_id'])
                    child.text = childText
                elif child.tag == 'created_by':
                    childText = child.text.replace('CREATED_BY', marcXmlValues['created_by'])
                    child.text = childText
                elif child.tag == 'created_date':
                    childText = child.text.replace('CREATED_DATE', marcXmlValues['created_date'])
                    child.text = childText
                elif child.tag == 'last_modified_by':
                    childText = child.text.replace('LAST_MODIFIED_BY', marcXmlValues['last_modified_by'])
                    child.text = childText
                elif child.tag == 'last_modified_date':
                    childText = child.text.replace('LAST_MODIFIED_DATE', marcXmlValues['last_modified_date'])
                    child.text = childText

                #leader tag
                elif child.tag == 'leader':
                    childText = child.text.replace('LEADER', marcXmlValues['leader'])
                    child.text = childText

                # 008 controlfield
                elif child.tag == 'controlfield':
                    if child.attrib['tag'] == '008':
                        childText = child.text.replace('FIELD_008', marcXmlValues['008'])
                        child.text = childText

                # datafield/subfields
                elif child.tag == 'subfield':
                    parent = child.getparent()
                    if parent.tag == 'datafield':

                        # Datafield 035, Proquest ID
                        if parent.attrib['tag'] == '035':
                            if child.attrib['code'] == 'a':
                                childText = child.text.replace('PROQUEST_IDENTIFIER_VALUE', marcXmlValues['pqid'])
                                child.text = childText

                        # Datafield 245, title and title indicator 2
                        elif parent.attrib['tag'] == '245':
                            if child.attrib['code'] == 'a':
                                childText = child.text.replace('TITLE_VALUE', marcXmlValues['title'])
                                child.text = childText
                                parentInd2 = parent.attrib['ind2'].replace('TITLE_INDICATOR_2_VALUE', marcXmlValues['titleIndicator2'])
                                parent.attrib['ind2'] = parentInd2

                        # Datafield 852
                        elif parent.attrib['tag'] == '852':
                            if child.attrib['code'] == 'z':
                                childText = child.text.replace('[DRS OBJECT URN]', self.object_urn)
                                child.text = childText
                            elif child.attrib['code'] == 'b':
                                childText = child.text.replace('LIB_CODE_3_CHAR', marcXmlValues['library_code'].upper())
                                child.text = childText

                        # Datafield 909, proquest id
                        elif parent.attrib['tag'] == '909':
                            if child.attrib['code'] == 'k':
                                childText = child.text.replace('LIB_CODE_3_CHAR', marcXmlValues['library_code'].upper())
                                child.text = childText
        except Exception as e:
            self.logger.error("Error transforming DRS holding for pqid: " +
                              self.pqid, exc_info=True)
            if (not self.unittesting):
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error transforming drs holding for pqid: " + self.pqid)
            return False

        try:
            # Write xml record out in batch directory
            with open(updated_holding, 'w') as UpdatedRecordOut:
                UpdatedRecordOut.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                updated_holding_xml = etree.tostring(rootRecord, encoding='unicode')
                UpdatedRecordOut.write(updated_holding_xml)
        except Exception as e:
            self.logger.error("Error writing DRS holding for pqid: " + self.pqid, exc_info=True)
            if (not self.unittesting):
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error writing drs holding for pqid: " + self.pqid)
                return False

        self.logger.debug(f'Wrote {updated_holding}')

        # And then return it to be collected with other processed records
        return True
    

    @tracer.start_as_current_span("upload_new_drs_holding")
    def upload_new_drs_holding(self, pqid, mms_id, holding_id, filename):  # pragma: no cover
        """
        This method uploads the new drs holding to alma.

        Args: proquest id, mms id, holding id, filename
        Returns: True if the new drs holding is uploaded to alma,
            False otherwise.
        """
        current_span = trace.get_current_span()
        if (not self.unittesting):
            current_span.add_event("submitting drs holding")
        self.logger.debug("submitting drs holding")
        self.logger.debug(ALMA_API_BASE + ALMA_GET_BIB_BASE +
                          mms_id + "/holdings/" + holding_id +
                          "?apikey=" + ALMA_API_KEY_2)

        headers = {'Content-Type': 'application/xml'}
        r = requests.put(ALMA_API_BASE + ALMA_GET_BIB_BASE +
                          mms_id + "/holdings/" + holding_id +
                          "?apikey=" + ALMA_API_KEY_2,
                          data=filename, headers=headers)
        if r.status_code == 200:
            self.logger.info("Successfully submitted new DRS holding for pqid: " +
                              pqid + " status code: " + str(r.status_code))
            return True
        else:
            self.logger.error("Error submitting new DRS holding for pqid: " +
                              pqid + " status code: " + str(r.status_code))
            return False


    
    @tracer.start_as_current_span("confirm_new_drs_holding")
    def confirm_new_drs_holding(self, pqid):
        """
        This method confirms the new drs holding is in alma with an urn attached.

        Args: proquest id
        Returns: True if the new drs holding is in alma with an urn attached,
            False otherwise.
        """
        current_span = trace.get_current_span()
        if (not self.unittesting):
            current_span.add_event("confirming drs holding")
            current_span.set_attribute("identifier", pqid)
        self.logger.debug("confirming new drs holding")
        r = requests.get(ALMA_SRU_MARCXML_BASE + pqid)
        sru_file = f'{self.output_dir}/src_marc.xml'
        if r.status_code == 200:
            with open(sru_file, 'wb') as f:
                f.write(r.content)
        else:
            self.logger.error("Error getting updated DRS holding for pqid: " +
                              self.pqid)
            return False

        urn_xpath = "//marc:record/marc:datafield[@tag='852']" \
                      "/marc:subfield[@code='z']"
        doc = ET.parse(sru_file)
        urn_statement = doc.xpath(urn_xpath,
                           namespaces=self.namespace_mapping)[0].text
        return urn_statement == SUBFIELD_Z_BASE + self.object_urn


    @tracer.start_as_current_span("send_to_alma")
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
        retval = False
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
            if (not self.unittesting):
                current_span.add_event("running integration test for alma drs holding service")
        if (not self.unittesting):
            current_span.add_event("sending to alma drs holding worker main")
        self.logger.info('sending to alma drs holding worker main')
        retval = self.send_to_alma_worker(force, verbose, integration_test)
        self.logger.info('complete')
        return retval
		
    @tracer.start_as_current_span("send_to_alma_worker")
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
        global notifyJM
        drsHoldingSent = False

	    # Create a notify object, this will also set-up logging and
        # logFile  = f'{logDir}/{jobCode}.{yymmdd}.log'
        notifyJM = notify('monitor', jobCode, None)

        # Let the Job Monitor know that the job has started
        notifyJM.log('pass', 'Update ETD Alma DRS Holding Record', verbose)
        notifyJM.report('start')
        if (not self.unittesting):
            current_span.add_event("sending drs holding to alma dropbox")
        self.logger.debug(f'{self.pqid} DRS holding was sent to Alma')

        # START PROCESSING
        # Get the mms id
        mms_id = self.get_mms_id(self, self.pqid)
        if not mms_id:
            self.logger.error("Error getting mms id for pqid: " +
                              self.pqid)
            return False
        holding_id = self.get_drs_holding_id_by_mms_id(mms_id)
        if not holding_id:
            self.logger.error("Error getting mms id for pqid: " +
                              self.pqid)
            if (not self.unittesting):
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error getting mms id for pqid: " + self.pqid)
            return False
        holding_record = self.get_drs_holding(mms_id, holding_id)
        if not holding_record:
            self.logger.error("Error getting drs holding for pqid: " +
                              self.pqid)
            if (not self.unittesting):
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error getting drs holding for pqid: " + self.pqid)
            return False
        transformed = self.transform_drs_holding(self.output_dir, self.marc_xml_values)
        if not transformed:
            notifyJM.log('fail', f'Error transforming drs holding record for pqid: {self.pqid}', verbose)
            self.logger.error("Error transforming drs holding record for pqid: " +
                              self.pqid)
            if (not self.unittesting):
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error transforming drs holding for pqid: " + self.pqid)
            return False
        notifyJM.log('pass', f'Wrote updated_holding for pqid: {self.pqid}', verbose)
        uploaded = self.upload_new_drs_holding(self.pqid, mms_id, holding_id,
                                               f'{self.output_dir}/updated_holding.xml')
        if not uploaded:
            self.logger.error("Error uploading drs holding for pqid: " +
                              self.pqid)
            if (not self.unittesting):
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error uploading drs holding for pqid: " + self.pqid)
            return False
        drsHoldingSent = self.confirm_new_drs_holding(self.pqid)
        if not drsHoldingSent:
            self.logger.error("Error confirming drs holding update for pqid: " +
                              self.pqid)
            if (not self.unittesting):
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event("error confirming drs holding update for pqid: " + self.pqid)
            return False

        # delete the output directory
        shutil.rmtree(f'{data_dir}/out/proquest{self.pqid}-holdings', ignore_errors=True)

        if (not self.unittesting):
            current_span.add_event(f'{self.pqid} DRS holding was updated & sent to Alma')
        self.logger.debug(f'{self.pqid} DRS holding was updated & sent to Alma')
        notifyJM.log('pass', f'{self.pqid} DRS holding was updated & sent to Alma', verbose)
        notifyJM.report('complete')
        if (not self.unittesting):
            current_span.add_event("completed")
        return drsHoldingSent
