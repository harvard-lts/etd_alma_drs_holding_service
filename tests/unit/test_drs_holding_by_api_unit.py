from etd.drs_holding_by_api import DRSHoldingByAPI
import lxml.etree as ET


class TestDRSHoldingByAPI():

    def test_transform_drs_holding(self):
        """"
        Test case for transform_drs_holding method of DRSHoldingByAPI class.
        """
        pqid = "28542882"
        object_urn = "URN-3:HUL.DRS.OBJECT:12345678"
        drs_holding = DRSHoldingByAPI(pqid, object_urn, True)
        output_dir = "./tests/data/unit"
        updated_holding = f'{output_dir}/updated_holding.xml'
        expected_statement = f'Preservation master, {object_urn}'

        assert drs_holding.transform_drs_holding(output_dir, object_urn)
        urn_xpath = "//record/datafield[@tag='852']" \
                    "/subfield[@code='z']"
        doc = ET.parse(updated_holding)
        urn_statement = doc.xpath(urn_xpath,
                                  namespaces=drs_holding.namespace_mapping)
        [0].text
        assert urn_statement == expected_statement
