from etd.drs_holding_by_api import DRSHoldingByAPI


class TestDRSHoldingByAPI():

    def test_get_mms_id(self):
        """"
        Test case for get_mms_id method of DRSHoldingByAPI class.
        """
        pqid = "28542882"
        object_urn = "URN-3:HUL.DRS.OBJECT:12345678"
        drs_holding = DRSHoldingByAPI(pqid, object_urn, True)
        mms_id = drs_holding.get_mms_id(pqid)
        assert mms_id == "99156845176203941"

    def test_get_drs_holding_id_by_mms_id(self):
        """
        Test case for get_drs_holdings_by_mms_id method of
        DRSHoldingByAPI class.
        """
        pqid = "28542882"
        object_urn = "URN-3:HUL.DRS.OBJECT:12345678"
        drs_holding = DRSHoldingByAPI(pqid, object_urn, True)
        mms_id = "99156845176203941"
        holding_id = drs_holding.get_drs_holding_id_by_mms_id(mms_id)
        assert drs_holding.marc_xml_values is not None
        assert drs_holding.marc_xml_values['holding_id'] == holding_id
        assert holding_id == "222607866070003941"

    def test_get_drs_holding(self):
        """
        Test case for get_drs_holding method of DRSHoldingByAPI class.
        """
        pqid = "7453039999"
        object_urn = "URN-3:HUL.DRS.OBJECT:12345678"
        mms_id = "99156845176203941"
        holding_id = "222608684560003941"
        drs_holding = DRSHoldingByAPI(pqid, object_urn, True)
        holding_xml = drs_holding.get_drs_holding(mms_id, holding_id)
        assert holding_xml is not None
        assert drs_holding.marc_xml_values is not None
        assert drs_holding.marc_xml_values['001'] == \
            "222608684560003941"

    def test_upload_new_drs_holding(self):
        """
        Test case for upload_new_drs_holding method of DRSHoldingByAPI class.
        """
        pqid = "7453039999"
        object_urn = "URN-3:HUL.DRS.OBJECT:12345678"
        mms_id = "99156848360203941"
        holding_id = "222608684560003941"
        holding_file = "./tests/data/test_upload_nodash_with_urn.xml"
        drs_holding = DRSHoldingByAPI(pqid, object_urn, integration_test=True)
        assert drs_holding.upload_new_drs_holding(pqid, mms_id,
                                                  holding_id, holding_file)

    def test_confirm_new_drs_holding(self):
        """
        Test case for confirm_new_drs_holding method of DRSHoldingByAPI class.
        """
        pqid = "7453039999" #"0434559314"
        mms_id = "99156848360203941"
        holding_id = "222608684560003941"
        object_urn = "URN-3:HUL.DRS.OBJECT:12345678"
        drs_holding = DRSHoldingByAPI(pqid, object_urn, True)
        assert drs_holding.confirm_new_drs_holding(pqid, mms_id, holding_id,
                                                   object_urn)

