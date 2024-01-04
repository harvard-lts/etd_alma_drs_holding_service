from etd.drs_holding_by_dropbox import DRSHoldingByDropbox
import lxml.etree as ET
import os.path

class TestDRSHoldingByDropbox():

    def test_getFromMets(self):
        """
        Test case for the getFromMets method of DRSHoldingByDropbox class.
        """
        metsFile = "/home/etdadm/tests/data/samplemets.xml"
        school = "gsd"

        drs_holding = DRSHoldingByDropbox('1234567890')
        result = drs_holding.getFromMets(metsFile, school)

        assert result is not None
        assert result['proquestId'] == '1234567890'
        assert result['school'] == school
        assert 'dateCreated' in result
        assert "title" in result

    def test_getFromMets_missing_values(self):
        """
        Test case to verify the behavior of getFromMets method when there are missing values in the mets file.
        """
        metsFile = "/home/etdadm/tests/data/samplemets.xml"
        school = "gsd"

        # Simulate missing values in the mets file
        drs_holding = DRSHoldingByDropbox(None)
        
        result = drs_holding.getFromMets(metsFile, school)

        assert not result
    
    def test_writeMarcXml(self):
        """
        Test case for the writeMarcXml method of DRSHoldingByDropbox class.
        """
        batch = "almadrsholding2023071720-993578-gsd"
        drs_holding = DRSHoldingByDropbox('1234567890')
        batchOutputDir = "/home/etdadm/tests/data/in/proquest2023071720-993578-gsd"
        
        school = "gsd"
        # Generate the data
        generatedMarcXmlValues = {}
        generatedMarcXmlValues['titleIndicator2'] = '0'
        generatedMarcXmlValues['title'] = "Naming Expeditor: Reimagining Institutional Naming System at Harvard"
        generatedMarcXmlValues['dateCreated'] = "2023-05"
        generatedMarcXmlValues['school'] = school
        generatedMarcXmlValues['proquestId'] = "1234567890"
        generatedMarcXmlValues['object_urn'] = "URN-3:HUL.DRS.OBJECT:101115942"
        verbose = False

        # Write the marcxml
        drs_holding.writeMarcXml(batch, batchOutputDir, generatedMarcXmlValues, verbose)
        marcFile = batchOutputDir + "/" + batch + ".xml"
        assert os.path.exists(marcFile)
        namespace_mapping = {'marc': 'http://www.loc.gov/MARC21/slim'}
        doc = ET.parse(marcFile)
        pqidXPath = "//marc:record/marc:datafield[@tag='035']" \
                     "/marc:subfield[@code='a']"
        titleXPath = "//marc:record/marc:datafield[@tag='245']" \
                     "/marc:subfield[@code='a']"
        objecturnXPath = "//marc:record/marc:datafield[@tag='852']" \
                     "/marc:subfield[@code='z']"
        libcodeXPath = "//marc:record/marc:datafield[@tag='909']" \
                     "/marc:subfield[@code='k']"
        assert doc.xpath(pqidXPath, namespaces=namespace_mapping)[0].text == \
            "(ProQuestETD)1234567890"
        assert doc.xpath(titleXPath, namespaces=namespace_mapping)[0].text == \
            "Naming Expeditor: " \
            "Reimagining Institutional Naming System at Harvard"
        assert doc.xpath(objecturnXPath, namespaces=namespace_mapping)[0].text == \
            "Preservation object," \
            "URN-3:HUL.DRS.OBJECT:101115942"
        assert doc.xpath(libcodeXPath, namespaces=namespace_mapping)[0].text == \
            "netDES"
        os.remove(marcFile)
