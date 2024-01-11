from etd.drs_holding_by_api import DRSHoldingByAPI
from datetime import datetime
from time import time


class TestDRSHoldingByAPI():

    def test_transform_drs_holding(self):
        """"
        Test case for transform_drs_holding method of DRSHoldingByAPI class.
        """
        pqid = "28542882"
        holding_id = "222608684560003941"
        object_urn = "URN-3:HUL.DRS.OBJECT:12345678"
        drs_holding = DRSHoldingByAPI(pqid, object_urn, True)
        output_dir = "./tests/data/out"
        date_time_stamp = \
            datetime.fromtimestamp(int(time())).strftime("%Y-%m-%dZ")
        marc_xml_values = {}
        marc_xml_values["mms_id"] = "99156845176203941"
        marc_xml_values['leader'] = "00537nx a22001571n 4500"
        marc_xml_values['title'] = "test title"
        marc_xml_values['titleIndicator2'] = "test title indicator"
        marc_xml_values['001'] = "1234"
        marc_xml_values['005'] = "20240110140636.0"
        marc_xml_values['library_code'] = "HUL"
        marc_xml_values['location_code'] = "NET"
        marc_xml_values['holding_id'] = holding_id
        marc_xml_values['pqid'] = pqid
        marc_xml_values['008'] = "2401102u 8 4001uu 0000000"
        marc_xml_values['created_by'] = "SYSTEM"
        marc_xml_values['created_date'] = "2024-01-10Z"
        marc_xml_values['holding_id'] = holding_id
        marc_xml_values['last_modified_date'] = date_time_stamp
        marc_xml_values['last_modified_by'] = "ETDDEV"
        assert drs_holding.transform_drs_holding(output_dir, marc_xml_values)
